//! CLI entrypoint: wires config resolution, sampling, pipeline, and reporting.

use clap::Parser;

use logpushi::cli::{Cli, Commands};
use logpushi::config::{resolve_config, SimMode};
use logpushi::discovery::{count_lines, discover};
use logpushi::pipeline;
use logpushi::sampling::{EventSource, ReplaySampler, Rng, SyntheticSampler};
use logpushi::stats::Summary;
use logpushi::timestamp;
use logpushi::transport;

fn main() {
    let cli = Cli::parse();
    let code = run(cli);
    std::process::exit(code);
}

fn run(cli: Cli) -> i32 {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: cannot create async runtime: {e}");
            return 1;
        }
    };
    match rt.block_on(run_async(cli)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            1
        }
    }
}

async fn run_async(cli: Cli) -> anyhow::Result<i32> {
    match cli.command {
        Commands::Discover(d) => run_discover(d.log_dir, d.lines, d.json),
        Commands::Validate(v) => run_validate(v.log_dir, v.file, v.sample.unwrap_or(200), v.json),
        Commands::Replay(_) | Commands::SyntheticToday(_) => run_simulation(cli).await,
    }
}

fn resolve_log_dir(dir: Option<std::path::PathBuf>) -> anyhow::Result<std::path::PathBuf> {
    Ok(dir.unwrap_or_else(|| std::path::PathBuf::from("./logs")))
}

fn run_discover(dir: Option<std::path::PathBuf>, count: bool, json: bool) -> anyhow::Result<i32> {
    let dir = resolve_log_dir(dir)?;
    let files = discover(&dir).map_err(anyhow::Error::new)?;
    if json {
        #[derive(serde::Serialize)]
        struct Entry {
            path: String,
            size: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            lines: Option<u64>,
        }
        let entries: Vec<Entry> = files
            .iter()
            .map(|f| {
                let lines = if count {
                    count_lines(&f.path).ok()
                } else {
                    None
                };
                Entry {
                    path: f.rel_str(),
                    size: f.size,
                    lines,
                }
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&entries)?);
        return Ok(0);
    }
    println!("{:>8} {:>15}  FILE", "LINES", "BYTES");
    let mut total_lines: u64 = 0;
    let mut total_bytes: u64 = 0;
    for f in &files {
        let n: Option<u64> = if count {
            Some(count_lines(&f.path)?) // fail fast on unreadable file
        } else {
            None
        };
        total_bytes += f.size;
        if let Some(n) = n {
            total_lines += n;
            println!("{:>8} {:>15}  {}", n, f.size, f.rel_str());
        } else {
            println!("{:>8} {:>15}  {}", "-", f.size, f.rel_str());
        }
    }
    if !json {
        println!("──────────────────────────────────");
        println!(
            "files: {}, bytes: {}, lines: {}",
            files.len(),
            total_bytes,
            if count {
                total_lines.to_string()
            } else {
                "n/a".to_string()
            }
        );
    }
    Ok(0)
}

fn run_validate(
    dir: Option<std::path::PathBuf>,
    file: Option<std::path::PathBuf>,
    sample: u64,
    json: bool,
) -> anyhow::Result<i32> {
    let dir = resolve_log_dir(dir)?;
    let files = discover(&dir)?;
    let files = match &file {
        Some(f) => {
            let fm: Vec<_> = files.iter().filter(|c| c.path == *f).cloned().collect();
            if fm.is_empty() {
                anyhow::bail!("file {} not found under {}", f.display(), dir.display());
            }
            fm
        }
        None => files,
    };
    use std::io::BufRead;
    let mut report = Vec::new();
    for f in &files {
        let std_fs = std::fs::File::open(&f.path);
        let (total_lines, detected, year_inferred, read_errors) = match std_fs {
            Ok(fh) => {
                let reader = std::io::BufReader::new(fh);
                let mut d_count = 0u64;
                let mut y_count = 0u64;
                let mut read_err = 0u64;
                let mut total = 0u64;
                for (i, line) in reader.lines().enumerate() {
                    if i as u64 >= sample {
                        break;
                    }
                    match line {
                        Ok(l) => {
                            total += 1;
                            if let Some((_, _, yi)) = timestamp::parse(&l) {
                                d_count += 1;
                                y_count += u64::from(yi);
                            }
                        }
                        Err(_) => read_err += 1,
                    }
                }
                (total, d_count, y_count, read_err)
            }
            Err(e) => {
                eprintln!("warning: cannot open {}: {e}", f.path.display());
                (0, 0, 0, 1)
            }
        };
        report.push((
            f.rel_str(),
            total_lines,
            detected,
            year_inferred,
            read_errors,
        ));
    }
    if json {
        #[derive(serde::Serialize)]
        struct Entry {
            path: String,
            lines_sampled: u64,
            ts_detected: u64,
            year_inferred: u64,
            read_errors: u64,
        }
        let entries: Vec<Entry> = report
            .iter()
            .map(|(p, t, d, y, e)| Entry {
                path: p.clone(),
                lines_sampled: *t,
                ts_detected: *d,
                year_inferred: *y,
                read_errors: *e,
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&entries)?);
    } else {
        println!("{:<10} {:>6}  {:<20}", "COVERAGE", "SAMPLED", "FILE");
        for (path, total, detected, year_inferred, read_errors) in &report {
            let pct = if *total > 0 {
                *detected as f64 / *total as f64 * 100.0
            } else {
                0.0
            };
            println!(
                "{:>9.1}% {:>6}  {:<20} (year inferred: {}, read errors: {})",
                pct, total, path, year_inferred, read_errors
            );
        }
    }
    Ok(0)
}

fn fmt_bytes(n: u64) -> String {
    if n >= 1 << 30 {
        format!("{:.1} GB", n as f64 / (1 << 30) as f64)
    } else if n >= 1 << 20 {
        format!("{:.1} MB", n as f64 / (1 << 20) as f64)
    } else if n >= 1 << 10 {
        format!("{:.1} KB", n as f64 / (1 << 10) as f64)
    } else {
        format!("{n} B")
    }
}

async fn run_simulation(cli: Cli) -> anyhow::Result<i32> {
    let cfg = resolve_config(cli)?;
    cfg.validate()?;
    let seed = cfg.seed.unwrap_or_else(random_seed);
    if !cfg.quiet {
        eprintln!(
            "mode: {}  transport: {}  endpoint: {}  seed: {}",
            match cfg.mode {
                SimMode::Replay => "replay",
                SimMode::SyntheticToday => "synthetic-today",
            },
            transport_label(&cfg),
            redact(&cfg.endpoint),
            seed
        );
    }

    let mut rng = Rng::new(seed);
    let sampler: Box<dyn EventSource> = match cfg.mode {
        SimMode::Replay => Box::new(make_replay(&cfg, &mut rng)?),
        SimMode::SyntheticToday => Box::new(make_synthetic(&cfg, rng)?),
    };

    let t = transport::build(&cfg)?;
    let summary = pipeline::run(&cfg, sampler, t, seed).await?;
    report(&cfg, &summary);

    // Exit code per architecture: 0 normal (partial failures reported in
    // summary), 2 run aborted by repeated send failures or signal.
    Ok(if summary.aborted { 2 } else { 0 })
}

fn transport_label(cfg: &logpushi::config::Config) -> &'static str {
    use logpushi::config::TransportKind as T;
    match cfg.transport {
        T::Stdout => "stdout",
        T::Otel => "otel",
        T::Hec => "hec",
    }
}

fn make_replay(cfg: &logpushi::config::Config, rng: &mut Rng) -> anyhow::Result<impl EventSource> {
    ReplaySampler::new(
        &cfg.log_dir,
        cfg.file.as_deref(),
        rng,
        cfg.start_offset_random,
        !cfg.no_loop,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))
}

fn make_synthetic(cfg: &logpushi::config::Config, rng: Rng) -> anyhow::Result<impl EventSource> {
    SyntheticSampler::new(&cfg.log_dir, rng, cfg.file.as_deref())
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn report(cfg: &logpushi::config::Config, summary: &Summary) {
    if cfg.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&summary).unwrap_or_default()
        );
        return;
    }
    if cfg.quiet {
        return;
    }
    println!("Logpushi summary");
    println!("----------------------------------------");
    println!("Mode:              {}", summary.mode);
    println!("Transport:         {}", summary.transport);
    println!(
        " Seed:              {}",
        summary
            .seed
            .map(|s| s.to_string())
            .unwrap_or_else(|| "auto".into())
    );
    println!("Events requested:  {}", summary.snapshot.requested);
    println!("Events generated:  {}", summary.snapshot.generated);
    println!("Events sent:       {}", summary.snapshot.sent);
    println!("Events failed:     {}", summary.snapshot.failed);
    println!("Events retried:    {}", summary.snapshot.retried);
    println!("Timestamps detected: {}", summary.snapshot.ts_detected);
    println!("Year inferred:     {}", summary.snapshot.ts_year_inferred);
    println!("Malformed lines:   {}", summary.snapshot.malformed_lines);
    println!("Encoding errors:   {}", summary.snapshot.encoding_errors);
    println!(
        "Bytes sent:        {}",
        fmt_bytes(summary.snapshot.bytes_sent)
    );
    println!("Elapsed:           {:.2}s", summary.elapsed_secs);
    println!(
        "Rate:              {:.0} events/sec",
        summary.events_per_sec
    );
    if summary.aborted {
        println!("Status:            ABORTED");
    }
    for w in &summary.warnings {
        println!("warning: {w}");
    }
}

fn redact(url: &Option<String>) -> String {
    match url {
        Some(u) => match u.split_once('?') {
            Some((base, _)) => format!("{base}?<redacted>"),
            None => u.clone(),
        },
        None => "-".to_string(),
    }
}

fn random_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E3779B97F4A7C15)
}
