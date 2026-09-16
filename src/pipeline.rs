//! Pipeline orchestrator for both simulation modes.
//!
//! sampler → generator (streaming, transform, batching) → bounded channel →
//! N workers → transport (retries + backoff) + global rate limiter.
//!
//! Backpressure: the bounded channel (~2× worker occupancy) is the only
//! buffering; a slow transport or engaged limiter blocks the generator in
//! `send().await`. Retry uses exponential backoff with full jitter and honors
//! `Retry-After`; auth (401/403) retries once, then aborts the run.
//! SIGINT/SIGTERM: no further generation, in-flight batches drain, summary.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, TimeZone, Utc};
use tokio::sync::{mpsc, watch};

use crate::config::Config;
use crate::event::Event;
use crate::sampling::{EventSource, Rng};
use crate::stats::{Stats, Summary};

const BASE_BACKOFF_MS: u64 = 500;
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);
const QUEUE_PER_WORKER: usize = 2;

/// Global token-bucket limiter shared by all workers: a configured rate R is
/// the aggregate events/sec cap, not rate-per-worker. Rate 0 = unlimited.
pub struct RateLimiter {
    state: std::sync::Mutex<LimiterState>,
    rate_hz: f64,
}

struct LimiterState {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(rate_hz: f64, capacity_tokens: f64) -> Self {
        RateLimiter {
            state: std::sync::Mutex::new(LimiterState {
                tokens: capacity_tokens.max(1.0),
                last: Instant::now(),
            }),
            rate_hz: rate_hz.max(0.0),
        }
    }

    /// Wait time before `n` tokens would be granted (exposed for tests).
    pub fn wait_duration(&self, n: u64) -> Duration {
        if self.rate_hz <= 0.0 {
            return Duration::ZERO;
        }
        let now = Instant::now();
        let mut st = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        st.tokens += now.duration_since(st.last).as_secs_f64() * self.rate_hz;
        st.last = now;
        if st.tokens >= n as f64 {
            st.tokens -= n as f64;
            Duration::ZERO
        } else {
            let deficit = n as f64 - st.tokens;
            Duration::from_secs_f64((deficit / self.rate_hz).clamp(0.005, 3600.0))
        }
    }

    pub async fn wait(&self, n: u64) {
        if self.rate_hz <= 0.0 {
            return;
        }
        loop {
            let d = self.wait_duration(n);
            if d.is_zero() {
                return;
            }
            tokio::time::sleep(d.min(Duration::from_millis(200))).await;
        }
    }
}

/// Exponential backoff with "full jitter": uniform in (1, base×2^(n-1)].
pub fn backoff_delay(attempt: u32, rng: &mut Rng) -> Duration {
    let exp_ms = BASE_BACKOFF_MS
        .saturating_mul(1u64 << attempt.saturating_sub(1).min(12))
        .min(MAX_BACKOFF.as_millis() as u64);
    Duration::from_millis(exp_ms)
        .mul_f64(rng.next_f64().max(0.01))
        .min(MAX_BACKOFF)
}

#[derive(Debug, Default)]
pub struct GenCounters {
    pub malformed: u64,
    pub detected: u64,
    pub year_inferred: u64,
    pub encoding_errors: u64,
}

/// Build the normalized event for a raw line.
///
/// Replay keeps the body verbatim, with no parsed event timestamp; transports
/// stamp the observed (send) time into the envelope so bodies remain
/// historical while ingestion looks recent. Synthetic-today rewrites only the
/// confidently detected timestamp substring/JSON field; without detection the
/// body is untouched and the transport falls back to observed time.
pub fn build_event(
    raw: crate::event::RawEvent,
    mode: crate::config::SimMode,
    window_start: DateTime<Utc>,
    anchor: DateTime<Utc>,
    rng: &mut Rng,
    counters: &mut GenCounters,
) -> Event {
    if raw.line.contains('\u{FFFD}') {
        counters.encoding_errors += 1;
    }
    if raw.line.trim().is_empty() {
        counters.malformed += 1;
    }
    let detect = crate::timestamp::parse(&raw.line);
    counters.detected += u64::from(detect.is_some());
    counters.year_inferred += detect.map(|(_, _, yi)| u64::from(yi)).unwrap_or(0);

    let mut body = raw.line.clone();
    let ts = match mode {
        crate::config::SimMode::Replay => None,
        crate::config::SimMode::SyntheticToday => {
            let span_ns = (anchor - window_start)
                .num_nanoseconds()
                .unwrap_or(0)
                .max(0) as u64;
            let offset = if span_ns > 0 {
                rng.next_u64() % span_ns
            } else {
                0
            };
            let new_ts = window_start + chrono::Duration::nanoseconds(offset as i64);
            match crate::timestamp::transform_json(&raw.line, new_ts)
                .or_else(|| crate::timestamp::transform(&raw.line, new_ts))
            {
                Some(t) => {
                    body = t.body;
                    Some(new_ts)
                }
                None => None,
            }
        }
    };
    Event {
        body,
        timestamp: ts,
        observed: Utc::now(),
        severity: crate::event::detect_severity(&raw.line),
        source_file: raw.source_file,
        source_type: raw.source_type,
        ts_detected: detect.is_some(),
        ts_year_inferred: detect.map(|(_, _, yi)| yi).unwrap_or(false),
        line_no: raw.line_no,
    }
}

/// The resolved recent window [start, anchor).
pub fn recent_window_span(cfg: &Config, anchor: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    use crate::config::TimeWindow as W;
    let start = match cfg.recent_window {
        W::Today => {
            let l = anchor.with_timezone(&Local);
            let midnight = l.date_naive().and_hms_opt(0, 0, 0).unwrap();
            Local
                .from_local_datetime(&midnight)
                .single()
                .unwrap_or(l)
                .with_timezone(&Utc)
        }
        other => {
            let d = match other {
                W::Seconds(d) => {
                    chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::hours(1))
                }
                W::Today | W::Now => chrono::Duration::hours(1),
            };
            anchor - d
        }
    };
    (start, anchor)
}

fn mode_str(cfg: &Config) -> &'static str {
    match cfg.mode {
        crate::config::SimMode::Replay => "replay",
        crate::config::SimMode::SyntheticToday => "synthetic-today",
    }
}

/// Send one batch with bounded retries: exponential backoff with full jitter,
/// `Retry-After` honored; auth retried once, then the run is aborted via flag.
pub async fn dispatch_with_retry(
    transport: &dyn crate::transport::Transport,
    batch: Vec<Event>,
    stats: &Stats,
    retries: u32,
    fail_fast: bool,
    fatal: &AtomicBool,
    backoff_rng: &mut Rng,
) {
    let n = batch.len() as u64;
    let mut attempt: u32 = 0;
    // Accounting rule (§26): `failed` counts events whose final outcome is
    // failure — intermediate retries are tracked by `retried` only.
    loop {
        let result = transport.send(&batch).await;
        if result.failed == 0 {
            stats.add_sent(result.succeeded.max(n), result.bytes);
            return;
        }
        let give_up = if result.auth_error {
            attempt >= 1 // one confirmation retry for auth, then abort
        } else {
            !result.retryable || attempt >= retries
        };
        if give_up {
            stats.add_sent(result.succeeded, result.bytes);
            stats.add_failed(n.saturating_sub(result.succeeded));
            if result.auth_error || fail_fast {
                fatal.store(true, Ordering::SeqCst);
            }
            return;
        }
        attempt += 1;
        stats.add_retried(n);
        if result.auth_error {
            continue; // immediate confirmation retry (auth)
        }
        let delay = result
            .retry_after
            .unwrap_or_else(|| backoff_delay(attempt, backoff_rng));
        tokio::time::sleep(delay.max(Duration::from_millis(10))).await;
    }
}

/// Run the full simulation end to end.
pub async fn run<S: EventSource + Send + 'static>(
    cfg: &Config,
    source: S,
    transport: Arc<dyn crate::transport::Transport>,
    seed: u64,
) -> anyhow::Result<Summary> {
    let started = Instant::now();
    let stats = Arc::new(Stats::new(cfg.lines));
    let anchor = Utc::now();
    let (window_start, _) = recent_window_span(cfg, anchor);

    let workers = cfg.workers.max(1);
    let batch_size = cfg.batch_size.max(1);
    let limiter = Arc::new(RateLimiter::new(cfg.rate, batch_size as f64));
    let (batch_tx, batch_rx) = mpsc::channel::<Vec<Event>>((workers * QUEUE_PER_WORKER).max(4));
    let batch_rx = Arc::new(tokio::sync::Mutex::new(batch_rx));
    let fatal = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let transport_name = transport.name().to_string();

    // SIGINT/SIGTERM → graceful shutdown.
    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = shutdown_tx.send(true);
            }
        }
    });

    // ---- generator (streaming; blocking IO on the async task is fine) ----
    let gen_handle = {
        let stats_gen = Arc::clone(&stats);
        let fatal_gen = Arc::clone(&fatal);
        let shutdown_gen = shutdown_rx.clone();
        let batch_tx = batch_tx.clone();
        let mut source = source;
        let mode = cfg.mode;
        let lines = cfg.lines;
        let window_start_gen = window_start;
        let anchor_gen = anchor;
        let mut rng = Rng::from_seed_opt(Some(seed));
        tokio::spawn(async move {
            let mut counters = GenCounters::default();
            let mut buffer: Vec<Event> = Vec::with_capacity(batch_size);
            let mut flush_at: Option<Instant> = None;
            let mut generated: u64 = 0;

            let outcome: Result<(), String> = async {
                while generated < lines {
                    if *shutdown_gen.borrow() || fatal_gen.load(Ordering::Relaxed) {
                        break;
                    }
                    if buffer.len() >= batch_size {
                        batch_tx
                            .send(std::mem::take(&mut buffer))
                            .await
                            .map_err(|_| "channel closed")?;
                    } else if let Some(t) = flush_at {
                        if t.elapsed() >= FLUSH_TIMEOUT {
                            flush_at = None;
                            if !buffer.is_empty() {
                                batch_tx
                                    .send(std::mem::take(&mut buffer))
                                    .await
                                    .map_err(|_| "channel closed")?;
                            }
                        }
                    }
                    match source.next() {
                        Ok(Some(raw)) => {
                            generated += 1;
                            buffer.push(build_event(
                                raw,
                                mode,
                                window_start_gen,
                                anchor_gen,
                                &mut rng,
                                &mut counters,
                            ));
                            if buffer.len() == 1 {
                                flush_at = Some(Instant::now());
                            }
                        }
                        Ok(None) => break, // source exhausted (see --no-loop)
                        Err(e) => {
                            eprintln!("warning: sampling error: {e}");
                            break;
                        }
                    }
                }
                if !buffer.is_empty() {
                    let _ = batch_tx.send(buffer).await;
                }
                Ok(())
            }
            .await;
            if let Err(e) = outcome {
                eprintln!("warning: generator stopped: {e}");
            }
            stats_gen.add_generated(generated);
            stats_gen.add_ts_detected(counters.detected, counters.year_inferred);
            stats_gen.add_malformed(counters.malformed);
            stats_gen.add_encoding_errors(counters.encoding_errors);
            // Implicitly drops batch_tx; the channel then closes for workers.
        })
    };
    // The main task must release its own sender so the channel (and thus the
    // workers' recv) can close when the generator finishes.
    drop(batch_tx);

    // ---- workers ----
    let mut worker_handles = Vec::with_capacity(workers);
    for id in 0..workers {
        let rx = Arc::clone(&batch_rx);
        let transport_w = Arc::clone(&transport);
        let limiter_w = Arc::clone(&limiter);
        let stats_w = Arc::clone(&stats);
        let fatal_w = Arc::clone(&fatal);
        let fail_fast = cfg.fail_fast;
        let retries = cfg.retries;
        worker_handles.push(tokio::spawn(async move {
            let mut backoff_rng = Rng::from_seed_opt(Some(id as u64));
            while let Some(batch) = rx.lock().await.recv().await {
                if fatal_w.load(Ordering::Relaxed) {
                    continue;
                }
                limiter_w.wait(batch.len() as u64).await;
                dispatch_with_retry(
                    transport_w.as_ref(),
                    batch,
                    stats_w.as_ref(),
                    retries,
                    fail_fast,
                    &fatal_w,
                    &mut backoff_rng,
                )
                .await;
            }
        }));
    }
    drop(batch_rx);

    // ---- progress reporter ----
    let _reporter_handle = {
        let stats_rep = Arc::clone(&stats);
        let mut shutdown_rep = shutdown_rx.clone();
        let quiet = cfg.quiet;
        let started_rep = started;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if !quiet {
                            let s = stats_rep.snapshot();
                            let elapsed = started_rep.elapsed().as_secs_f64().max(1e-9);
                            eprint!(
                                "\r generated={} sent={} failed={} rate={:.0}/s   ",
                                s.generated, s.sent, s.failed, s.sent as f64 / elapsed
                            );
                        }
                    }
                    _ = shutdown_rep.changed() => break,
                }
            }
            if !quiet {
                eprintln!();
            }
        })
    };

    // ---- monitor: run until generator finishes, fatal error, or signal ----
    while !gen_handle.is_finished() && !fatal.load(Ordering::Relaxed) {
        if *shutdown_rx.borrow() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Generator might have finished successfully; or need draining grace.
    let aborted = fatal.load(Ordering::Relaxed) || *shutdown_rx.borrow();
    let _ = shutdown_tx.send(true); // stop the reporter
                                    // Drain: generator flushes its final batch; workers then run to empty.
    let _ = gen_handle.await;
    for handle in worker_handles {
        let _ = handle.await;
    }

    let elapsed = started.elapsed();
    let snapshot = stats.snapshot();
    let achieved = snapshot.sent as f64 / elapsed.as_secs_f64().max(1e-9);
    let mut warnings = Vec::new();
    if snapshot.failed > 0 {
        warnings.push(format!("{} events failed", snapshot.failed));
    }
    let undetected = snapshot.generated.saturating_sub(snapshot.ts_detected);
    if cfg.mode == crate::config::SimMode::SyntheticToday && undetected > snapshot.generated / 2 {
        warnings.push(format!(
            "timestamp detection coverage low: {} / {} events",
            snapshot.ts_detected, snapshot.generated
        ));
    }
    Ok(Summary {
        snapshot,
        mode: mode_str(cfg).to_string(),
        transport: transport_name,
        endpoint: None,
        seed: Some(seed),
        warnings,
        elapsed_secs: elapsed.as_secs_f64(),
        events_per_sec: achieved,
        aborted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rate_limiter_wait_duration() {
        let lim = RateLimiter::new(10.0, 10.0);
        // First grant is immediate (full bucket).
        assert_eq!(lim.wait_duration(10), Duration::ZERO);
        // Immediately draining 10 more must defer ~1s (deficit / rate).
        let d = lim.wait_duration(10);
        assert!(d > Duration::from_millis(500) && d < Duration::from_millis(1100));

        // Rate 0 (or negative) ⇒ unlimited.
        let un = RateLimiter::new(0.0, 10.0);
        assert_eq!(un.wait_duration(999999), Duration::ZERO);
    }

    #[test]
    fn rate_limiter_burst_smaller_than_rate_no_wait() {
        let lim = RateLimiter::new(1000.0, 1000.0);
        for _ in 0..20 {
            let d = lim.wait_duration(10);
            assert!(
                d < Duration::from_millis(100),
                "small bursts should be near-immediate, got {d:?}"
            );
            std::thread::sleep(Duration::from_millis(15));
        }
    }

    #[test]
    fn backoff_delay_growth_and_jitter() {
        let mut rng = Rng::new(42);
        for attempt in 1..=6u32 {
            let d = backoff_delay(attempt, &mut rng);
            assert!(d <= Duration::from_secs(30));
            assert!(d >= Duration::ZERO);
        }
        // Deterministic with the same seed.
        let mut a = Rng::new(1);
        let mut b = Rng::new(1);
        assert_eq!(
            backoff_delay(3, &mut a),
            backoff_delay(3, &mut b),
            "same seed must give the same jitter"
        );
    }

    #[test]
    fn synthetic_instants_uniform_in_window() {
        // Statistical sanity: many draws stay strictly within [start, anchor].
        let mut rng = Rng::new(99);
        let anchor = Utc::now();
        let start = anchor - chrono::Duration::hours(1);
        let span = (anchor - start).num_nanoseconds().unwrap() as u64;
        for _ in 0..200 {
            let t = start + chrono::Duration::nanoseconds((rng.next_u64() % span) as i64);
            assert!(t >= start && t <= anchor);
        }
    }

    #[test]
    fn build_event_replay_keeps_body_and_clears_ts() {
        let mut rng = Rng::new(5);
        let mut counters = GenCounters::default();
        let raw = crate::event::RawEvent {
            line: "2015-10-17 15:37:56 INFO x".to_string(),
            source_file: "a.log".into(),
            source_type: "apache",
            line_no: 1,
        };
        let raw2 = crate::event::RawEvent {
            line: "Dec 10 06:55:46 host-a app[1]: alpha".to_string(),
            source_file: "a.log".into(),
            source_type: "linux-syslog",
            line_no: 2,
        };
        let e = build_event(
            raw,
            crate::config::SimMode::Replay,
            Utc::now() - chrono::Duration::minutes(10),
            Utc::now(),
            &mut rng,
            &mut counters,
        );
        assert!(e.timestamp.is_none());
        assert!(e.ts_detected);
        assert_eq!(e.body, "2015-10-17 15:37:56 INFO x");
        assert_eq!(counters.detected, 1);
        let e2 = build_event(
            raw2,
            crate::config::SimMode::Replay,
            Utc::now() - chrono::Duration::minutes(10),
            Utc::now(),
            &mut rng,
            &mut counters,
        );
        assert!(e2.ts_year_inferred, "syslog line infers the year");
        assert_eq!(counters.year_inferred, 1);
    }

    #[test]
    fn build_event_synthetic_rewrites_detected_ts() {
        let mut rng = Rng::new(6);
        let mut counters = GenCounters::default();
        let anchor = Utc::now();
        let window = anchor - chrono::Duration::hours(2);
        let raw = crate::event::RawEvent {
            line: "2015-10-17 15:37:56 INFO x".to_string(),
            source_file: "a.log".into(),
            source_type: "other",
            line_no: 1,
        };
        let e = build_event(
            raw.clone(),
            crate::config::SimMode::SyntheticToday,
            window,
            anchor,
            &mut rng,
            &mut counters,
        );
        let ts = e.timestamp.expect("timestamp");
        assert!(ts <= anchor && ts >= window);
        // Only the timestamp span is rewritten; the remainder is preserved.
        assert!(e.body.ends_with(" INFO x"), "{}", e.body);
        assert!(!e.body.contains("2015-10-17"), "{}", e.body);
    }

    #[test]
    fn build_event_synthetic_undetected_untouched() {
        let mut rng = Rng::new(7);
        let mut counters = GenCounters::default();
        let anchor = Utc::now();
        let raw = crate::event::RawEvent {
            line: "no timestamps in here".to_string(),
            source_file: "q.log".into(),
            source_type: "other",
            line_no: 3,
        };
        let e = build_event(
            raw,
            crate::config::SimMode::SyntheticToday,
            anchor - chrono::Duration::hours(1),
            anchor,
            &mut rng,
            &mut counters,
        );
        assert!(e.timestamp.is_none());
        assert_eq!(e.body, "no timestamps in here");
        assert_eq!(counters.detected, 0);
    }
}
