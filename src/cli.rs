//! CLI argument definitions.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "logpushi",
    version,
    about = "Generate realistic log activity and push it into Quetzalog",
    long_about = "Logpushi replays historical log files and synthesizes recent activity from them, \
pushing generated events to a SIEM-compatible endpoint (OpenTelemetry OTLP or Splunk HEC). \
Source logs are only ever read, never modified."
)]
pub struct Cli {
    /// Path to a JSON config file with flat keys mirroring long flag names
    /// (e.g. `{"transport": "otel", "batch_size": 500}`). Keys given on the
    /// command line or in the environment take precedence.
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

/// Options shared by `replay` and `synthetic-today`. Optional-valued fields
/// distinguish "not provided" (fall through env/config/default) from explicit
/// CLI values.
#[derive(clap::Args, Debug, Clone)]
pub struct RunArgs {
    /// Directory to discover log files in
    #[arg(long, value_name = "DIR")]
    pub log_dir: Option<PathBuf>,

    /// Number of events to generate
    #[arg(long, short = 'n', value_name = "N")]
    pub lines: Option<u64>,

    /// Deterministic PRNG seed (default: derived from time)
    #[arg(long, value_name = "N")]
    pub seed: Option<u64>,

    /// Restrict sampling to a single file (skips random selection)
    #[arg(long, value_name = "FILE")]
    pub file: Option<PathBuf>,

    /// Transport: otel | hec | stdout
    #[arg(long, value_name = "KIND")]
    pub transport: Option<String>,

    /// Transport endpoint URL (OTLP logs or HEC base URL)
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<String>,

    /// Events per batch
    #[arg(long, value_name = "N")]
    pub batch_size: Option<usize>,

    /// Concurrent send workers
    #[arg(long, value_name = "N")]
    pub workers: Option<usize>,

    /// Global events/sec limit; 0 = unlimited
    #[arg(long, value_name = "RATE", allow_negative_numbers = false)]
    pub rate: Option<f64>,

    /// Per-request timeout in seconds
    #[arg(long, value_name = "SECS")]
    pub timeout: Option<u64>,

    /// Max retries per batch for transient failures
    #[arg(long, value_name = "N")]
    pub retries: Option<u32>,

    /// Disable TLS certificate verification (development only)
    #[arg(long)]
    pub insecure: bool,

    /// Abort the run on the first batch failure
    #[arg(long)]
    pub fail_fast: bool,

    /// Suppress progress output
    #[arg(long)]
    pub quiet: bool,

    /// Detailed per-batch logging
    #[arg(long)]
    pub verbose: bool,

    /// Machine-readable (JSON) summary output
    #[arg(long)]
    pub json: bool,

    // ---- replay-only ----
    /// Start position in the replayed file: random | start
    #[arg(long, value_name = "POS")]
    pub start_offset: Option<String>,

    /// Stop at EOF instead of wrapping around
    #[arg(long)]
    pub no_loop: bool,

    // ---- synthetic-today-only ----
    /// Window for synthetic timestamps: now | Ns | Nm | Nh | Nd | today
    #[arg(long, value_name = "SPAN")]
    pub recent_window: Option<String>,

    /// Distribution of timestamps inside the window (only `uniform` is implemented)
    #[arg(long, value_name = "STRATEGY")]
    pub ts_distribution: Option<String>,

    // ---- HEC options ----
    /// HEC token (prefer the LOGPUSHI_HEC_TOKEN environment variable)
    #[arg(long, value_name = "TOKEN")]
    pub hec_token: Option<String>,

    /// HEC index
    #[arg(long, value_name = "NAME")]
    pub index: Option<String>,

    /// HEC source override
    #[arg(long, value_name = "NAME")]
    pub source: Option<String>,

    /// HEC sourcetype override
    #[arg(long, value_name = "NAME")]
    pub sourcetype: Option<String>,

    /// Override the host field on all events
    #[arg(long, value_name = "NAME")]
    pub host: Option<String>,

    // ---- OTLP options ----
    /// OTLP wire protocol: http (default) | grpc
    #[arg(long, value_name = "PROTO")]
    pub otel_protocol: Option<String>,

    /// Resource service.name
    #[arg(long, value_name = "NAME")]
    pub service_name: Option<String>,
}

#[derive(clap::Args, Debug, Clone)]
pub struct DiscoverArgs {
    /// Directory to discover log files in
    #[arg(long, value_name = "DIR")]
    pub log_dir: Option<PathBuf>,

    /// Also count lines per file (full-corpus scan; slow)
    #[arg(long)]
    pub lines: bool,

    /// Machine-readable output
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ValidateArgs {
    /// Directory to validate
    #[arg(long, value_name = "DIR")]
    pub log_dir: Option<PathBuf>,

    /// Restrict validation to a single file
    #[arg(long, value_name = "FILE")]
    pub file: Option<PathBuf>,

    /// Lines sampled per file for detection coverage
    #[arg(long, value_name = "N")]
    pub sample: Option<u64>,

    /// Machine-readable output
    #[arg(long)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Replay events from a randomly chosen log file, bodies untouched
    Replay(RunArgs),
    /// Rewrite detected timestamps into a recent window, bodies otherwise preserved
    SyntheticToday(RunArgs),
    /// Print the discovered corpus inventory
    Discover(DiscoverArgs),
    /// Check corpus health: readability, timestamp-detection coverage
    Validate(ValidateArgs),
}
