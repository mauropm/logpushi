//! Thread-safe statistics counters and run summaries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct Stats {
    requested: AtomicU64,
    generated: AtomicU64,
    sent: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    ts_detected: AtomicU64,
    ts_year_inferred: AtomicU64,
    malformed_lines: AtomicU64,
    encoding_errors: AtomicU64,
    bytes_sent: AtomicU64,
}

impl Stats {
    pub fn new(requested: u64) -> Self {
        Stats {
            requested: AtomicU64::new(requested),
            ..Default::default()
        }
    }

    pub fn requested(&self) -> u64 {
        self.requested.load(Ordering::Relaxed)
    }

    pub fn add_requested(&self, n: u64) {
        self.requested.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_generated(&self, n: u64) {
        self.generated.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_sent(&self, n: u64, bytes: u64) {
        self.sent.fetch_add(n, Ordering::Relaxed);
        if bytes > 0 {
            self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn add_failed(&self, n: u64) {
        self.failed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_retried(&self, n: u64) {
        self.retried.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_ts_detected(&self, n: u64, year_inferred: u64) {
        self.ts_detected.fetch_add(n, Ordering::Relaxed);
        if year_inferred > 0 {
            self.ts_year_inferred
                .fetch_add(year_inferred, Ordering::Relaxed);
        }
    }

    pub fn add_malformed(&self, n: u64) {
        self.malformed_lines.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_encoding_errors(&self, n: u64) {
        self.encoding_errors.fetch_add(n, Ordering::Relaxed);
    }

    pub fn generated(&self) -> u64 {
        self.generated.load(Ordering::Relaxed)
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            requested: self.requested(),
            generated: self.generated(),
            sent: self.sent(),
            failed: self.failed(),
            retried: self.retried.load(Ordering::Relaxed),
            ts_detected: self.ts_detected.load(Ordering::Relaxed),
            ts_year_inferred: self.ts_year_inferred.load(Ordering::Relaxed),
            malformed_lines: self.malformed_lines.load(Ordering::Relaxed),
            encoding_errors: self.encoding_errors.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct Snapshot {
    pub requested: u64,
    pub generated: u64,
    pub sent: u64,
    pub failed: u64,
    pub retried: u64,
    pub ts_detected: u64,
    pub ts_year_inferred: u64,
    pub malformed_lines: u64,
    pub encoding_errors: u64,
    pub bytes_sent: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Summary {
    pub snapshot: Snapshot,
    pub mode: String,
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    pub warnings: Vec<String>,
    pub elapsed_secs: f64,
    pub events_per_sec: f64,
    pub aborted: bool,
}

pub fn fmt_elapsed(d: Duration) -> String {
    format!("{:.2}s", d.as_secs_f64())
}

pub fn print_summary(sum: &Summary) {
    if let Err(e) = print_summary_json_if_needed(sum) {
        // stdout invariants should never fail; surface on stderr.
        eprintln!("warning: summary print failed: {e}");
    }
}

fn print_summary_json_if_needed(sum: &Summary) -> std::io::Result<()> {
    let cfg_echo = "";
    let seed_str = sum
        .seed
        .map(|s| s.to_string())
        .unwrap_or_else(|| "auto".to_string());
    println!(
        "──────────────────────────────────────────\nLogpushi\n──────────────────────────────────────────\nMode:              {}\nTransport:         {}\nEndpoint:          {}\nSeed:              {}\nEvents requested:  {}\nEvents generated:  {}\nEvents sent:       {}\nEvents failed:     {}\nEvents retried:    {}\nTimestamps detected: {}\nBytes sent:        {}\nElapsed:           {}\nRate:              {:.0} events/s\n──────────────────────────────────────────",
        sum.mode,
        sum.transport,
        sum.endpoint.as_deref().unwrap_or("-"),
        seed_str,
        sum.snapshot.requested,
        sum.snapshot.generated,
        sum.snapshot.sent,
        sum.snapshot.failed,
        sum.snapshot.retried,
        sum.snapshot.ts_detected,
        sum.snapshot.bytes_sent,
        fmt_elapsed(Duration::from_secs_f64(sum.elapsed_secs)),
        sum.events_per_sec,
    );
    for w in &sum.warnings {
        println!("warning: {w}");
    }
    let _ = cfg_echo;
    Ok(())
}

pub struct RunTimer {
    start: Instant,
}

impl RunTimer {
    pub fn start() -> Self {
        RunTimer {
            start: Instant::now(),
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}
