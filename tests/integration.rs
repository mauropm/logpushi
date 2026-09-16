//! End-to-end pipeline tests with an in-process mock transport, and a
//! loopback HTTP mock HEC server (no external network access).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use logpushi::config::{Config, SimMode, TimeWindow, TransportKind};
use logpushi::event::Event;
use logpushi::pipeline;
use logpushi::sampling::{ReplaySampler, Rng, SyntheticSampler};
use logpushi::transport::{BatchResult, Transport};

/// Deterministic mock capture transport.
struct Mock {
    calls: AtomicU64,
    events: Mutex<Vec<String>>,
}

#[async_trait]
impl Transport for Mock {
    async fn send(&self, batch: &[Event]) -> BatchResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut seen = self.events.lock().unwrap();
        for e in batch {
            seen.push(format!("{}|{}", e.line_no, e.body));
        }
        BatchResult::delivered(batch.len() as u64, batch.len() as u64 * 10)
    }
    fn name(&self) -> &'static str {
        "mock"
    }
}

fn mock() -> Arc<Mock> {
    Arc::new(Mock {
        calls: AtomicU64::new(0),
        events: Mutex::new(Vec::new()),
    })
}

fn source_for(cfg: &Config, rng: Rng, synthetic: bool) -> Box<dyn logpushi::sampling::EventSource> {
    if synthetic {
        Box::new(
            SyntheticSampler::new(cfg.log_dir.as_path(), rng, None).expect("synthetic sampler"),
        )
    } else {
        let mut rng = rng;
        Box::new(
            ReplaySampler::new(cfg.log_dir.as_path(), None, &mut rng, true, true)
                .expect("replay sampler"),
        )
    }
}

fn temp_logs() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(d.path().join("sub")).unwrap();
    std::fs::write(
        d.path().join("app.log"),
        "Dec 10 06:55:46 host-a app[1]: alpha\n2015-10-17 15:37:56 INFO beta\nno ts here\n",
    )
    .unwrap();
    std::fs::write(d.path().join("sub/b.log"), "gamma one\ngamma two\n").unwrap();
    // Spec: README.md and *_label(s).txt files are excluded from discovery.
    std::fs::write(d.path().join("README.md"), "ignore me\n").unwrap();
    std::fs::write(d.path().join("_labels.txt"), "ignore me\n").unwrap();
    d
}

fn replay_cfg(dir: &std::path::Path) -> Config {
    Config {
        log_dir: dir.to_path_buf(),
        lines: 25,
        seed: Some(7),
        batch_size: 10,
        workers: 2,
        ..Default::default()
    }
}

fn corpus_digest(dir: &std::path::Path) -> String {
    let mut content = String::new();
    content += &std::fs::read_to_string(dir.join("app.log")).unwrap();
    content += &std::fs::read_to_string(dir.join("sub/b.log")).unwrap();
    content
}

#[tokio::test]
async fn replay_counts_all_lines_and_leaves_logs_untouched() {
    let tmp = temp_logs();
    let before = corpus_digest(tmp.path());
    let cfg = replay_cfg(tmp.path());
    let source = source_for(&cfg, Rng::from_seed_opt(cfg.seed), false);
    let m = mock();
    let summary = pipeline::run(&cfg, source, m.clone(), 7).await.unwrap();
    assert_eq!(summary.snapshot.generated, 25);
    assert_eq!(summary.snapshot.sent, 25);
    assert_eq!(summary.snapshot.failed, 0);
    assert_eq!(summary.snapshot.encoding_errors, 0);
    assert!(summary.snapshot.bytes_sent > 0);
    assert_eq!(corpus_digest(tmp.path()), before, "source logs untouched");
}

#[tokio::test]
async fn synthetic_today_rewrites_timestamps() {
    // Directory with only syslog lines: every detected ts infers the year.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("s.log"),
        "Dec 10 06:55:46 host-a app[1]: alpha\nNov  3 01:02:03 host-b app[2]: beta\n",
    )
    .unwrap();
    let cfg = Config {
        mode: SimMode::SyntheticToday,
        recent_window: TimeWindow::Now,
        ..replay_cfg(tmp.path())
    };
    let source = source_for(&cfg, Rng::from_seed_opt(cfg.seed), true);
    let m = mock();
    let summary = pipeline::run(&cfg, source, m.clone(), 7).await.unwrap();
    assert_eq!(summary.snapshot.generated, 25);
    assert_eq!(summary.snapshot.sent, 25);
    // At least syslog + ISO lines in each cycle get detected timestamps.
    assert!(summary.snapshot.ts_detected > 0, "coverage");
    assert!(
        summary.snapshot.ts_year_inferred > 0,
        "syslog lines need the year inferred"
    );
}

#[tokio::test]
async fn synthetic_json_body_field_transform() {
    // Isolated dir with a single JSON log so every sampled event is JSON.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("j.log"),
        r#"{"@timestamp": "2015-10-17T15:37:56Z", "level": "info", "msg": "value"}"#,
    )
    .unwrap();
    let cfg = Config {
        log_dir: tmp.path().to_path_buf(),
        mode: SimMode::SyntheticToday,
        lines: 10,
        batch_size: 5,
        seed: Some(3),
        workers: 1,
        ..Default::default()
    };
    let source = source_for(&cfg, Rng::from_seed_opt(cfg.seed), true);
    let m = mock();
    let s = pipeline::run(&cfg, source, m.clone(), 3).await.unwrap();
    assert_eq!(s.snapshot.sent, 10);
    let seen = m.events.lock().unwrap();
    assert!(!seen.is_empty());
    let body = seen[0].split_once('|').unwrap().1.to_string();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let ts = v["@timestamp"].as_str().unwrap().to_string();
    assert!(!ts.contains("2015"), "{ts}");
    assert_eq!(v["level"], "info");
    assert_eq!(v["msg"], "value");
}

#[tokio::test]
async fn line_count_is_respected_across_batches() {
    let tmp = temp_logs();
    let cfg = Config {
        lines: 3,
        batch_size: 2,
        workers: 1,
        ..replay_cfg(tmp.path())
    };
    let source = source_for(&cfg, Rng::from_seed_opt(cfg.seed), false);
    let m = mock();
    let s = pipeline::run(&cfg, source, m.clone(), 2).await.unwrap();
    assert_eq!(s.snapshot.generated, 3);
    assert_eq!(
        m.calls.load(Ordering::SeqCst),
        2,
        "batches of 2 and 1: flush on 1s timer"
    );
    let seen = m.events.lock().unwrap();
    assert_eq!(seen.len(), 3);
}

// ---------------------------------------------------------------------------
// Loopback HTTP mocks for the real HEC transport.
// ---------------------------------------------------------------------------

use std::io::Write as _;

fn read_head_and_body(stream: &mut std::net::TcpStream) -> Option<(String, Vec<u8>)> {
    use std::io::Read;
    let mut buf = [0u8; 65536];
    let n = stream.read(&mut buf).ok()?;
    let s = String::from_utf8_lossy(&buf[..n]).to_string();
    let (head, rest) = s.split_once("\r\n\r\n")?;
    let cl = head
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| {
            l.split(':')
                .nth(1)
                .and_then(|v| v.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let mut body = rest.as_bytes().to_vec();
    body.truncate(cl);
    let path = head.split(' ').nth(1).unwrap_or("/").to_string();
    Some((path, body))
}

fn spawn_mock(
    addr_holder: std::sync::mpsc::Sender<std::net::SocketAddr>,
    counter: Arc<AtomicU64>,
    f: Arc<dyn Fn(usize) -> String + Send + Sync>,
) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    addr_holder.send(l.local_addr().unwrap()).unwrap();
    for conn in l.incoming() {
        let mut s = match conn {
            Ok(s) => s,
            Err(_) => break,
        };
        let f = f.clone();
        let counter = counter.clone();
        std::thread::spawn(move || {
            if let Some((path, body)) = read_head_and_body(&mut s) {
                if path.contains("/services/collector/event") && !body.is_empty() {
                    let n = (counter.fetch_add(1, Ordering::SeqCst) + 1) as usize;
                    let resp = f(n);
                    s.write_all(resp.as_bytes()).unwrap();
                } else {
                    s.write_all(b"HTTP/1.1 400 Bad\r\nContent-Length: 2\r\n\r\nno")
                        .unwrap();
                }
            }
        });
    }
}

/// HEC mock: N 429s with Retry-After: 0 first, then always success.
fn spawn_flaky_mock(tx: std::sync::mpsc::Sender<std::net::SocketAddr>, flaky_replies: usize) {
    let counter = Arc::new(AtomicU64::new(0));
    let f = move |n: usize| -> String {
        if n <= flaky_replies {
            "HTTP/1.1 429 Too Many\r\nRetry-After: 0\r\nContent-Length: 2\r\n\r\n{}".to_string()
        } else {
            "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n{\"code\":0}".to_string()
        }
    };
    spawn_mock(tx, counter, Arc::new(f));
}

/// HEC mock: always 401.
fn spawn_auth_fail_mock(tx: std::sync::mpsc::Sender<std::net::SocketAddr>) {
    let counter = Arc::new(AtomicU64::new(0));
    spawn_mock(
        tx,
        counter,
        Arc::new(move |_n: usize| -> String {
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: 2\r\n\r\n{}".to_string()
        }),
    );
}

fn spawn_success_mock(tx: std::sync::mpsc::Sender<std::net::SocketAddr>) {
    let counter = Arc::new(AtomicU64::new(0));
    spawn_mock(
        tx,
        counter,
        Arc::new(move |_n: usize| -> String {
            "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n{\"code\":0}".to_string()
        }),
    );
}

async fn run_hec_lines(
    addr: &str,
    lines: u64,
    batch: usize,
    retries: u32,
    token: &str,
) -> logpushi::stats::Summary {
    let tmp = temp_logs();
    let cfg = Config {
        log_dir: tmp.path().to_path_buf(),
        lines,
        batch_size: batch,
        workers: 1,
        retries,
        transport: TransportKind::Hec,
        endpoint: Some(format!("http://{addr}")),
        hec_token: Some(token.into()),
        ..Default::default()
    };
    let source = source_for(&cfg, Rng::from_seed_opt(cfg.seed), false);
    let tx = logpushi::transport::build(&cfg).unwrap();
    pipeline::run(&cfg, source, tx, 1).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hec_local_success() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || spawn_success_mock(tx));
    let addr = rx.recv().unwrap();
    let s = run_hec_lines(&addr.to_string(), 6, 5, 2, "t").await;
    assert_eq!(s.snapshot.sent, 6);
    assert_eq!(s.snapshot.failed, 0);
    assert!(!s.aborted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hec_retries_on_429_then_succeeds() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || spawn_flaky_mock(tx, 2));
    let addr = rx.recv().unwrap();
    let s = run_hec_lines(&addr.to_string(), 5, 5, 5, "t").await;
    assert_eq!(s.snapshot.sent, 5, "must succeed after backoff retries");
    assert!(s.snapshot.retried > 0, "retried counter must tick");
    assert_eq!(s.snapshot.failed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hec_auth_failure_aborts() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || spawn_auth_fail_mock(tx));
    let addr = rx.recv().unwrap();
    let s = run_hec_lines(&addr.to_string(), 5, 5, 2, "t").await;
    assert_eq!(s.snapshot.sent, 0);
    assert_eq!(s.snapshot.failed, 5);
    assert!(s.aborted, "auth failure must abort");
}
