//! Runtime configuration: CLI flags -> environment -> config file -> defaults,
//! resolved per key. Command-line values arrive via `cli.rs` `Option`s (None =
//! "not provided"), so precedence is explicit and testable.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::cli::{Cli, Commands, RunArgs};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Stdout,
    Otel,
    Hec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SimMode {
    #[default]
    Replay,
    SyntheticToday,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtelProtocol {
    Http,
    Grpc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeWindow {
    Now,
    Today,
    Seconds(Duration),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub log_dir: PathBuf,
    pub mode: SimMode,
    pub lines: u64,
    pub seed: Option<u64>,
    pub file: Option<PathBuf>,
    pub batch_size: usize,
    pub workers: usize,
    pub rate: f64,
    pub timeout: Duration,
    pub retries: u32,
    pub insecure: bool,
    pub fail_fast: bool,
    pub quiet: bool,
    pub verbose: bool,
    pub json: bool,
    pub transport: TransportKind,
    pub endpoint: Option<String>,
    pub hec_token: Option<String>,
    pub hec_index: Option<String>,
    pub hec_source: Option<String>,
    pub hec_sourcetype: Option<String>,
    pub host: String,
    pub otel_protocol: OtelProtocol,
    pub service_name: String,
    pub recent_window: TimeWindow,
    pub ts_distribution: String,
    pub start_offset_random: bool,
    pub no_loop: bool,
    pub count_lines: bool,
    pub validate_sample: u64,
    pub validate_file: Option<PathBuf>,
}

/// Resolution context: optional CLI value, environment variable name,
/// config-file key, and default.
fn resolve<T: std::str::FromStr>(
    cli: Option<T>,
    env_key: Option<&str>,
    file_key: &str,
    file_map: &HashMap<String, String>,
    default: T,
) -> anyhow::Result<T>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    if let Some(v) = cli {
        return Ok(v);
    }
    if let Some(ek) = env_key {
        if let Ok(s) = std::env::var(ek) {
            if !s.is_empty() {
                return s
                    .parse::<T>()
                    .map_err(|e| anyhow::anyhow!("env {ek}={s:?}: {e}"));
            }
        }
    }
    if let Some(s) = file_map.get(file_key) {
        return s
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("config file key {file_key:?}: {e}"));
    }
    Ok(default)
}

fn resolve_opt<T: std::str::FromStr>(
    cli: Option<T>,
    env_key: Option<&str>,
    file_key: &str,
    file_map: &HashMap<String, String>,
) -> anyhow::Result<Option<T>>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    if cli.is_some() {
        return Ok(cli);
    }
    if let Some(ek) = env_key {
        if let Ok(s) = std::env::var(ek) {
            if !s.is_empty() {
                return s
                    .parse::<T>()
                    .map(Some)
                    .map_err(|e| anyhow::anyhow!("env {ek}={s:?}: {e}"));
            }
        }
    }
    if let Some(s) = file_map.get(file_key) {
        return s
            .parse::<T>()
            .map(Some)
            .map_err(|e| anyhow::anyhow!("config file key {file_key:?}: {e}"));
    }
    Ok(None)
}

fn resolve_string(
    cli: Option<String>,
    env_key: Option<&str>,
    file_key: &str,
    file_map: &HashMap<String, String>,
    default: &str,
) -> String {
    if let Some(v) = cli {
        return v;
    }
    if let Some(ek) = env_key {
        if let Ok(s) = std::env::var(ek) {
            if !s.is_empty() {
                return s;
            }
        }
    }
    if let Some(s) = file_map.get(file_key) {
        return s.clone();
    }
    default.to_string()
}

fn load_config_file(path: &Path) -> anyhow::Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read config file {}: {e}", path.display()))?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", path.display()))?;
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("config file must be a JSON object"))?;
    let mut map = HashMap::new();
    for (k, v) in obj {
        let s = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => anyhow::bail!("config key {k:?}: unsupported value type"),
        };
        map.insert(k.replace('-', "_"), s);
    }
    Ok(map)
}

pub fn parse_window(s: &str) -> anyhow::Result<TimeWindow> {
    let s = s.trim().to_ascii_lowercase();
    match s.as_str() {
        "now" => return Ok(TimeWindow::Now),
        "today" => return Ok(TimeWindow::Today),
        _ => {}
    }
    if s.len() < 2 {
        anyhow::bail!("unknown recent-window {s:?}");
    }
    let (num, unit) = (&s[..s.len() - 1], &s[s.len() - 1..]);
    let n: u64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("unknown recent-window {s:?}"))?;
    match unit {
        "s" => Ok(TimeWindow::Seconds(Duration::from_secs(n))),
        "m" => Ok(TimeWindow::Seconds(Duration::from_secs(n * 60))),
        "h" => Ok(TimeWindow::Seconds(Duration::from_secs(n * 3600))),
        "d" => Ok(TimeWindow::Seconds(Duration::from_secs(n * 86400))),
        _ => Err(anyhow::anyhow!(
            "unknown recent-window {s:?} (use now|today|Ns|Nm|Nh|Nd)"
        )),
    }
}

fn parse_transport(s: &str) -> anyhow::Result<TransportKind> {
    match s {
        "stdout" => Ok(TransportKind::Stdout),
        "otel" => Ok(TransportKind::Otel),
        "hec" => Ok(TransportKind::Hec),
        other => Err(anyhow::anyhow!(
            "unknown transport {other:?} (expected otel|hec|stdout)"
        )),
    }
}

impl Config {
    fn from_run(
        run: &RunArgs,
        mode: SimMode,
        file_map: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let transport_s = resolve_string(
            run.transport.clone(),
            Some("LOGPUSHI_TRANSPORT"),
            "transport",
            file_map,
            "stdout",
        );
        let transport = parse_transport(&transport_s)?;

        // Token resolution is special: env is preferred, CLI is a no-secret-in-docs convenience.
        let hec_token = run
            .hec_token
            .clone()
            .or_else(|| {
                std::env::var("LOGPUSHI_HEC_TOKEN")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .or_else(|| file_map.get("hec_token").cloned());

        let endpoint = run
            .endpoint
            .clone()
            .or_else(|| match transport {
                TransportKind::Otel => std::env::var("LOGPUSHI_OTEL_ENDPOINT")
                    .ok()
                    .filter(|s| !s.is_empty()),
                TransportKind::Hec => std::env::var("LOGPUSHI_HEC_ENDPOINT")
                    .ok()
                    .filter(|s| !s.is_empty()),
                TransportKind::Stdout => None,
            })
            .or_else(|| file_map.get("endpoint").cloned());

        let recent_window_str = resolve_string(
            run.recent_window.clone(),
            None,
            "recent_window",
            file_map,
            "1h",
        );
        let recent_window = parse_window(&recent_window_str)?;

        let start_offset = resolve_string(
            run.start_offset.clone(),
            None,
            "start_offset",
            file_map,
            "random",
        );
        let start_offset_random = match start_offset.as_str() {
            "random" => true,
            "start" => false,
            other => anyhow::bail!("--start-offset must be random|start, got {other:?}"),
        };

        let otel_protocol = match resolve_string(
            run.otel_protocol.clone(),
            None,
            "otel_protocol",
            file_map,
            "http",
        )
        .as_str()
        {
            "http" => OtelProtocol::Http,
            "grpc" => OtelProtocol::Grpc,
            other => anyhow::bail!("unknown --otel-protocol {other:?} (use http|grpc)"),
        };

        let cfg = Config {
            log_dir: resolve(
                run.log_dir.clone(),
                Some("LOGPUSHI_LOG_DIR"),
                "log_dir",
                file_map,
                PathBuf::from("./logs"),
            )?,
            mode,
            lines: resolve(run.lines, None, "lines", file_map, 100)?,
            seed: resolve_opt(run.seed, Some("LOGPUSHI_SEED"), "seed", file_map)?,
            file: run
                .file
                .clone()
                .or_else(|| file_map.get("file").map(PathBuf::from)),
            batch_size: resolve(
                run.batch_size,
                Some("LOGPUSHI_BATCH_SIZE"),
                "batch_size",
                file_map,
                100,
            )?,
            workers: resolve(
                run.workers,
                Some("LOGPUSHI_WORKERS"),
                "workers",
                file_map,
                4,
            )?,
            rate: resolve(run.rate, Some("LOGPUSHI_RATE"), "rate", file_map, 0.0)?,
            timeout: Duration::from_secs(resolve(
                run.timeout,
                Some("LOGPUSHI_TIMEOUT"),
                "timeout",
                file_map,
                30,
            )?),
            retries: resolve(
                run.retries,
                Some("LOGPUSHI_RETRIES"),
                "retries",
                file_map,
                3,
            )?,
            insecure: run.insecure || file_map.contains_key("insecure"),
            fail_fast: run.fail_fast || file_map.contains_key("fail_fast"),
            quiet: run.quiet || file_map.contains_key("quiet"),
            verbose: run.verbose || file_map.contains_key("verbose"),
            json: run.json || file_map.contains_key("json"),
            transport,
            endpoint,
            hec_token,
            hec_index: run.index.clone().or_else(|| file_map.get("index").cloned()),
            hec_source: run
                .source
                .clone()
                .or_else(|| file_map.get("source").cloned()),
            hec_sourcetype: run
                .sourcetype
                .clone()
                .or_else(|| file_map.get("sourcetype").cloned()),
            host: resolve_string(run.host.clone(), None, "host", file_map, ""),
            otel_protocol,
            service_name: resolve_string(
                run.service_name.clone(),
                Some("LOGPUSHI_SERVICE_NAME"),
                "service_name",
                file_map,
                "logpushi",
            ),
            recent_window,
            ts_distribution: resolve_string(
                run.ts_distribution.clone(),
                None,
                "ts_distribution",
                file_map,
                "uniform",
            ),
            start_offset_random,
            no_loop: run.no_loop || file_map.contains_key("no_loop"),
            count_lines: false,
            validate_sample: 200,
            validate_file: None,
        };
        Ok(cfg)
    }
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.workers == 0 {
            anyhow::bail!("--workers must be >= 1");
        }
        if self.batch_size == 0 {
            anyhow::bail!("--batch-size must be >= 1");
        }
        if self.lines == 0 {
            anyhow::bail!("--lines must be >= 1");
        }
        if self.rate.is_sign_negative() || self.rate.is_nan() {
            anyhow::bail!("--rate must be >= 0");
        }
        match self.transport {
            TransportKind::Otel | TransportKind::Hec => {
                if self.endpoint.is_none() {
                    anyhow::bail!(
                        "--transport {} requires --endpoint (or LOGPUSHI_OTEL_ENDPOINT / LOGPUSHI_HEC_ENDPOINT); \
                         explicit endpoints prevent accidental injection into production",
                        if self.transport == TransportKind::Otel { "otel" } else { "hec" }
                    );
                }
            }
            TransportKind::Stdout => {}
        }
        if self.transport == TransportKind::Hec && self.hec_token.is_none() {
            anyhow::bail!(
                "--transport hec requires a token: set the LOGPUSHI_HEC_TOKEN environment variable"
            );
        }
        if self.transport != TransportKind::Otel && self.otel_protocol == OtelProtocol::Grpc {
            anyhow::bail!("--otel-protocol applies only to --transport otel");
        }
        if self.transport == TransportKind::Otel && self.otel_protocol == OtelProtocol::Grpc {
            anyhow::bail!("OTLP/gRPC is not implemented; use --otel-protocol http");
        }
        if self.ts_distribution != "uniform" {
            anyhow::bail!(
                "--ts-distribution {} is not implemented; only 'uniform'",
                self.ts_distribution
            );
        }
        if self.insecure {
            eprintln!("warning: TLS verification disabled by --insecure; development use only");
        }
        Ok(())
    }

    /// A redacted, printable echo of the config for summaries (`--json`).
    pub fn describe(&self) -> String {
        let endpoint = match self.transport {
            TransportKind::Stdout => "-".to_string(),
            _ => redact_url(self.endpoint.as_deref().unwrap_or("")),
        };
        format!(
            "mode={} transport={} endpoint={} lines={} batch_size={} workers={} rate={} retries={} seed={}",
            match self.mode {
                SimMode::Replay => "replay",
                SimMode::SyntheticToday => "synthetic-today",
            },
            transport_name(self.transport),
            endpoint,
            self.lines,
            self.batch_size,
            self.workers,
            self.rate,
            self.retries,
            self.seed
                .map(|s| s.to_string())
                .unwrap_or_else(|| "auto".to_string()),
        )
    }
}

pub fn transport_name(t: TransportKind) -> &'static str {
    match t {
        TransportKind::Stdout => "stdout",
        TransportKind::Otel => "otel",
        TransportKind::Hec => "hec",
    }
}

/// Redact query parameters that could carry credentials.
fn redact_url(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _)) => format!("{base}?<redacted>"),
        None => url.to_string(),
    }
}

pub fn resolve_config(cli: Cli) -> anyhow::Result<Config> {
    let map = match cli.config.as_deref() {
        Some(p) => load_config_file(p)?,
        None => load_config_file(Path::new("logpushi.json"))?,
    };
    match cli.command {
        Commands::Replay(run) => {
            let cfg = Config::from_run(&run, SimMode::Replay, &map)?;
            cfg.validate()?;
            Ok(cfg)
        }
        Commands::SyntheticToday(run) => {
            let cfg = Config::from_run(&run, SimMode::SyntheticToday, &map)?;
            // Mode-specific validation now that mode is set.
            if !(cfg.mode == SimMode::SyntheticToday || cfg.mode == SimMode::Replay) {
                unreachable!()
            }
            cfg.validate()?;
            Ok(cfg)
        }
        Commands::Discover(d) => {
            let dir = d
                .log_dir
                .clone()
                .or_else(|| std::env::var("LOGPUSHI_LOG_DIR").ok().map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from("./logs"));
            Ok(Config {
                log_dir: dir,
                mode: SimMode::Replay,
                lines: 0,
                seed: None,
                file: None,
                batch_size: 100,
                workers: 4,
                rate: 0.0,
                timeout: Duration::from_secs(30),
                retries: 3,
                insecure: false,
                fail_fast: false,
                quiet: false,
                verbose: false,
                json: d.json,
                transport: TransportKind::Stdout,
                endpoint: None,
                hec_token: None,
                hec_index: None,
                hec_source: None,
                hec_sourcetype: None,
                host: String::new(),
                otel_protocol: OtelProtocol::Http,
                service_name: "logpushi".to_string(),
                recent_window: TimeWindow::Seconds(Duration::from_secs(3600)),
                ts_distribution: "uniform".into(),
                start_offset_random: true,
                no_loop: false,
                count_lines: d.lines,
                validate_sample: 0,
                validate_file: None,
            })
        }
        Commands::Validate(v) => {
            let dir = v
                .log_dir
                .clone()
                .or_else(|| std::env::var("LOGPUSHI_LOG_DIR").ok().map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from("./logs"));
            Ok(Config {
                log_dir: dir,
                mode: SimMode::Replay,
                lines: 0,
                seed: None,
                file: v.file,
                batch_size: 100,
                workers: 4,
                rate: 0.0,
                timeout: Duration::from_secs(30),
                retries: 3,
                insecure: false,
                fail_fast: false,
                quiet: false,
                verbose: false,
                json: v.json,
                transport: TransportKind::Stdout,
                endpoint: None,
                hec_token: None,
                hec_index: None,
                hec_source: None,
                hec_sourcetype: None,
                host: String::new(),
                otel_protocol: OtelProtocol::Http,
                service_name: "logpushi".to_string(),
                recent_window: TimeWindow::Seconds(Duration::from_secs(3600)),
                ts_distribution: "uniform".into(),
                start_offset_random: true,
                no_loop: false,
                count_lines: false,
                validate_sample: 200,
                validate_file: None,
            })
        }
    }
}

/// Baseline defaults (also the base for CLI/env/file overrides).
impl std::default::Default for Config {
    fn default() -> Self {
        Config {
            log_dir: "logs".into(),
            mode: SimMode::Replay,
            lines: 1000,
            seed: None,
            file: None,
            batch_size: 100,
            workers: 4,
            rate: 0.0,
            timeout: std::time::Duration::from_secs(10),
            retries: 3,
            insecure: false,
            fail_fast: false,
            quiet: false,
            verbose: false,
            json: false,
            transport: TransportKind::Stdout,
            endpoint: None,
            hec_token: None,
            hec_index: None,
            hec_source: None,
            hec_sourcetype: None,
            host: String::new(),
            otel_protocol: OtelProtocol::Http,
            service_name: String::new(),
            recent_window: TimeWindow::Now,
            ts_distribution: "uniform".into(),
            start_offset_random: true,
            no_loop: false,
            count_lines: false,
            validate_sample: 100,
            validate_file: None,
        }
    }
}
