# Logpushi

**Logpushi** is a CLI tool for generating realistic log activity and pushing it into a SIEM.
It replays historical log files, synthesizes "recent" activity from them, and stress-tests
ingestion pipelines with configurable event volumes — from a quick 100-event functional
test to million-event load runs.

> **Current state:** implemented in **Rust** (see [`architecture.md`](architecture.md) for
> design rationale and a list of deliberate deviations from the original blueprint).
> `cargo build --release` produces a single static binary; the full test suite runs with
> `cargo test`.

---

## Purpose

Testing a SIEM requires realistic log data at realistic — and unrealistic — volumes.
Hand-crafting test events is tedious and unrepresentative. Logpushi solves this by using
real, heterogeneous historical logs as raw material:

- **Replay** existing log events verbatim.
- **Synthesize** recent activity by taking historical events and rewriting their
  timestamps so they appear to have just happened.
- **Stress-test** with controlled event counts, rates, batching, retries, and transports.

## Features

- **Random file replay** — recursively discover log files, pick one at random (seeded),
  and stream it into your pipeline; bodies stay byte-identical to the source.
- **Synthetic-today mode** — sample events across the corpus and transform their
  historical timestamps into a recent window (uniform distribution), touching only the
  detected timestamp substring (or the matching JSON field).
- **Homogeneous, round-trip-safe timestamp handling** — detect and rewrite timestamps in
  eight formats without corrupting neighboring content (see below).
- **Two transports + dry run** — OpenTelemetry OTLP/HTTP (protobuf) and Splunk HTTP Event
  Collector, behind a common transport interface, plus `stdout` for testing the whole
  pipeline with zero network.
- **Controlled load** — event counts, *global* rate limiting, batching with a bounded
  queue as backpressure, concurrent workers, exponential backoff with full jitter,
  `Retry-After` handling, and one confirmation retry for auth failures before aborting.
- **Reproducibility** — deterministic PRNG seed for repeatable simulations.
- **Observability** — live progress, and a machine-readable (`--json`) summary
  distinguishing requested / generated / sent / failed / retried events, timestamp
  coverage, and throughput.

## Architecture Overview

```text
            ┌───────────────┐
            │      CLI      │   replay | synthetic-today | discover | validate
            └───────┬───────┘
                    ▼
          ┌───────────────────┐
          │ Simulation Engine │        seeded RNG, config precedence, validation
          └─────────┬─────────┘
                    │
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
File Discovery   Sampling     Timestamp
                              Transformation
    └───────────────┼───────────────┘
                    ▼
            ┌─────────────────┐
            │  Event Pipeline │   batcher → bounded queue → rate limiter → workers
            └────────┬────────┘
                     ▼
             ┌─────────────────┐
             │    Transport    │   dyn Transport: send(&[Event]) -> BatchResult
             └────────┬────────┘
                  ┌───┴───┐
                  ▼       ▼
        OpenTelemetry   Splunk HEC   (+ stdout)
                  │       │
                  └───┬───┘
                      ▼
               your SIEM / collector
```

### How the work is split (what was implemented)

| Component | File | Notes |
|---|---|---|
| CLI (clap) | `src/cli.rs` | `replay`, `synthetic-today`, `discover`, `validate` subcommands + global `--config` |
| Config precedence | `src/config.rs` | CLI → env → `logpushi.json` (flat JSON keys) → defaults; validation incl. endpoint/token requirements; URL redaction |
| Discovery | `src/discovery.rs` | iterative recursive walk; excludes dotfiles, `README.md`, `_label(s).txt`, empty files; sorted inventory |
| Sampling | `src/sampling.rs` | `ReplaySampler` (random file + random byte offset, wraps at EOF) and `SyntheticSampler` (random file + offset per event, O(1) memory/fds); deterministic with `--seed` |
| Timestamps | `src/timestamp.rs` | detector chain with per-format render functions; JSON field rewrites with key order preserved; plausibility guards |
| Event model | `src/event.rs` | body preservation, severity detection, dataset family |
| Pipeline | `src/pipeline.rs` | batcher (size or 1s flush), bounded channel, global token-bucket limiter, worker pool, backoff w/ full jitter, retry accounting, graceful shutdown |
| Stats | `src/stats.rs` | atomic counters + JSON summary; abort flag |
| Transports | `src/transport/*` | OTLP/HTTP via hand-written prost structs (no protoc), HEC envelope, stdout |

### Timestamp formats handled

Detected in priority order (most specific first), then rewritten in-place:

1. **ISO-8601** — `2017-05-16 00:00:00.008`, `2015-10-17T15:37:56Z`,
   `2015-10-17 15:37:56,547` (log4j comma-millis), `Z` / `±HH:MM` / `±HHMM` offsets;
   missing timezone treated as local.
2. **Apache CTF** — `[Thu Jun 09 06:07:04 2005]` (weekday/day rendered correctly for the
   new instant).
3. **Apache CLF** — `10/Jun/2005:06:07:04 +0000` (offset preserved and re-rendered).
4. **Numeric epoch** — guarded: whitespace-delimited token of 9–13 digits in a plausible
   range (seconds ≤ 10 digits; millis 11–13). Metric-like values (`onExtend:1514038530000`)
   are **not** corrupted.
5. **Syslog without year** — `Dec 10 06:55:46` → most recent past year (flagged via
   `ts_year_inferred` stat).
6. **HealthApp** — `20171223-22:15:29:606`.
7. **Proxifier** — `[10.30 16:49:06]`.
8. **Android** — `12-17 19:31:36.263`.

JSON lines get field-level rewrites for `timestamp`, `@timestamp`, `ts`, `time`, `date`,
`datetime`, `_time` with key order preserved; non-JSON lines get substring replacement of
exactly the detected span. Untouched content is byte-identical to the source.

## Requirements

- A SIEM instance reachable over HTTP(S), exposing either an OTLP endpoint or a
  Splunk-compatible HEC endpoint.
- Credentials for the chosen transport (OTLP endpoint config, or an HEC token).
- Sufficient disk space for the `logs/` corpus (~1.1 GB as shipped).

Implementation: single static binary built with `cargo` — no runtime dependencies
(TLS via rustls, no OpenSSL).

## Installation

```bash
git clone https://github.com/mauropm/logpushi
cd logpushi
cargo build --release
./target/release/logpushi --help
```

## Quick Start

```bash
# Preview the corpus inventory
logpushi discover

# Check corpus health: readability, timestamp detection coverage
logpushi validate

# Replay 100 events from a randomly chosen log file (bodies untouched)
logpushi replay --lines 100

# Synthesize 1,000 "recent" events from across the corpus
logpushi synthetic-today --lines 1000
```

Transports and endpoints are selected via flags or environment variables:

```bash
# OpenTelemetry OTLP (bare base gets /v1/logs appended)
logpushi replay --transport otel --endpoint https://siem.example:4318/v1/logs

# Splunk HEC (services/collector/event appended)
export LOGPUSHI_HEC_TOKEN=xxxx
logpushi synthetic-today --transport hec \
  --endpoint https://siem.example:8088 \
  --lines 5000
```

## Volume Testing

A core purpose of Logpushi is stress-testing. Example ladder:

```bash
# Small functional test
logpushi replay --lines 100

# Medium test
logpushi replay --lines 10000

# Large stress test
logpushi replay --lines 100000

# Million-event run
logpushi replay --lines 1000000 --rate 0 --batch-size 500 --workers 8
```

> The `--lines` value is the number of events **requested/generated**, not necessarily the
> number successfully ingested. Final statistics distinguish requested, generated, sent,
> failed, and retried events. See [`architecture.md`](architecture.md) for the full
> event-counting model.

The tool stays memory-bounded regardless of volume: events are streamed, batched, and
dispatched through bounded queues — a 1M-event run does not require the corpus or the
generated event set to fit in RAM.

Rate limiting is verified by design: `200 events at --rate 50` with 100-event batches
takes exactly 2.0 s (first batch free, second throttled), and the summary reports achieved
events/sec so expectations match reality.

## Configuration

Configuration resolves in this order (highest precedence first):

```text
CLI flags  →  environment variables  →  configuration file  →  defaults
```

The configuration file is `logpushi.json` with flat keys mirroring the long flag names:

```json
{ "transport": "hec", "batch_size": 500, "rate": 2000 }
```

Environment variables:

| Variable | Purpose |
|---|---|
| `LOGPUSHI_LOG_DIR` | Log corpus directory (default `logs/`) |
| `LOGPUSHI_TRANSPORT` | `otel`, `hec`, or `stdout` |
| `LOGPUSHI_OTEL_ENDPOINT` | OTLP logs endpoint (bare base → `/v1/logs` appended) |
| `LOGPUSHI_HEC_ENDPOINT` | Splunk HEC base endpoint (`/services/collector/event` appended) |
| `LOGPUSHI_HEC_TOKEN` | HEC authentication token (sensitive) |
| `LOGPUSHI_BATCH_SIZE` | Events per batch |
| `LOGPUSHI_WORKERS` | Concurrent send workers |
| `LOGPUSHI_RATE` | Global events/sec limit (`0` = unlimited) |
| `LOGPUSHI_TIMEOUT` | HTTP timeout |
| `LOGPUSHI_RETRIES` | Max retries per batch for transient failures |
| `LOGPUSHI_SEED` | Deterministic PRNG seed |
| `LOGPUSHI_SERVICE_NAME` | OTLP `service.name` resource attribute |

Secrets such as HEC tokens should come from environment variables or a secret manager —
never committed configuration files. Logpushi redacts endpoints (query strings) in output
and never prints the token.

## CLI Reference

```text
logpushi replay            # Mode 1: random-file sequential replay, bodies verbatim
logpushi synthetic-today   # Mode 2: timestamp-shifted synthetic recent events
logpushi discover          # inventory of files under --log-dir (table or --json)
logpushi validate          # corpus health: readability, timestamp detection coverage
```

Common options: `--log-dir`, `--file`, `--seed`, `--lines`, `--transport`,
`--endpoint`, `--batch-size`, `--workers`, `--rate`, `--timeout`, `--retries`,
`--insecure` (development only), `--fail-fast`, `--quiet`, `--verbose`, `--json`.

Exit codes: `0` success (even with per-batch failures reported), `1` fatal
startup/config error, `2` run aborted (fatal auth failure after a confirmation retry,
`--fail-fast` triggered, or signal).

## Test Data Sources & Acknowledgements

The log corpus in [`logs/`](logs/) comes from
**[LogHub](https://github.com/logpai/loghub)** — a large collection of public system log
datasets maintained by the LogPAI project at Tsinghua University and collaborators.

> ### Thank you, LogHub team 🙏
>
> **All the hard, unglamorous work behind this simulator belongs to the LogHub
> contributors:** collecting, cleaning, labeling, and documenting decades' worth of real
> system logs from Apache, syslog (Linux/Mac/SSH), Proxifier, Zookeeper, mobile health
> apps, Android devices, BlueGene/L supercomputers, HPC clusters, OpenStack, and Hadoop.
> Logpushi is only a *simulator of activity* — it would be useless without the genuine,
> heterogeneous real-world logs you volunteered to the community. The least we can do is
> say thank you and point you to the citations each dataset requests in its README under
> `logs/`.
>
> **These logs do not represent any production environment of your own, and they are not
> part of Logpushi.** They are public research datasets, bundled here as simulation
> input. Please review and respect the licensing and citation requirements noted in the
> per-dataset `README.md` files under `logs/` before redistributing.

Included datasets:

| Path | Source type | Size | Example timestamp format |
|---|---|---|---|
| `logs/Apache.log` | Apache web server | 4.9 MB | `[Thu Jun 09 06:07:04 2005]` |
| `logs/Linux.log` | Linux syslog | 2.2 MB | `Jun  9 06:06:20` (no year) |
| `logs/Mac.log` | macOS syslog | 16 MB | `Jul  1 09:00:55` (no year) |
| `logs/SSH.log` | SSH/auth syslog | 70 MB | `Dec 10 06:55:46` (no year) |
| `logs/Proxifier.log` | Proxifier proxy client | 2.4 MB | `[10.30 16:49:06]` (no year) |
| `logs/Zookeeper.log` | Zookeeper (log4j) | 9.9 MB | `2015-07-29 17:41:41,536` |
| `logs/HealthApp.log` | Mobile health app | 22 MB | `20171223-22:15:29:606\|...` |
| `logs/Android_v1/Android.log` | Android (Huawei) | 183 MB *(LFS)* | `12-17 19:31:36.263` (no year) |
| `logs/BGL/BGL.log` | BlueGene/L supercomputer | 709 MB *(LFS)* | epoch + `2005.06.03` |
| `logs/HPC/HPC.log` | HPC cluster (LANL) | 32 MB | epoch seconds (`1145552216`) |
| `logs/OpenStack/` | OpenStack cloud | 59 MB | `2017-05-16 00:00:00.008` |
| `logs/Hadoop/` | Hadoop YARN containers | 48 MB | `2015-10-17 15:37:56,547` |

This mix of formats is deliberate: it exercises Logpushi's timestamp detection and
heterogeneous-source handling. The architecture is designed to accommodate additional
enterprise log types in the future (Windows Event Log, CEF, LEEF, firewalls, identity
systems, databases, etc.), which are **not** currently included.

## Security

Logpushi is a test utility, but it processes real log data, which can contain sensitive
material:

- **PII** — datasets such as SSH and OpenStack logs contain IP addresses, usernames, and
  hostnames.
- **Secrets in logs** — real-world logs sometimes embed tokens or credentials; the bundled
  LogHub datasets are public research data, but inspect any corpus before pushing it to an
  external system.
- **Transport credentials** — HEC tokens are sensitive; use environment variables, and
  never print them. Logpushi does not emit secrets in normal output.
- **TLS** — TLS certificate verification is on by default. An explicit `--insecure` flag
  is intended for local development only.
- **Endpoint safety** — be deliberate about the target endpoint to avoid accidentally
  writing simulated data into a production SIEM.

Run Logpushi against test environments with data you are licensed and permitted to use.

## Development

Implemented in Rust (`src/`). Build and quality gates:

```bash
cargo build --release
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

### Testing

- **Unit tests** (`cargo test`): timestamp detection/round-tripping for every dataset
  family (including guards that metric-like neighboring values survive), seeded sampling
  determinism, rate limiter, backoff jitter, HEC envelope shape, OTLP log-record shape and
  protobuf round-trip, config defaults.
- **Integration tests** (`tests/integration.rs`): full pipeline against an in-process mock
  transport and loopback HTTP mock HEC servers (success, 429-with-Retry-After retry, 401
  abort); source logs verified untouched. No external network required.
- **Load testing**: use the [Volume Testing](#volume-testing) ladder against your SIEM, or
  `--transport stdout` / a local mock server for isolation. Example mock harness:

  ```bash
  # trivial local HEC mock
  python3 - <<'EOF' &
  import http.server
  class H(http.server.BaseHTTPRequestHandler):
      def do_POST(self):
          self.rfile.read(int(self.headers.get('Content-Length', 0)))
          self.send_response(200); self.end_headers(); self.wfile.write(b'{"code":0}')
      def log_message(self, *a): pass
  http.server.HTTPServer(('127.0.0.1', 8088), H).serve_forever()
  EOF

  LOGPUSHI_HEC_TOKEN=t ./target/release/logpushi replay \
    --transport hec --endpoint http://127.0.0.1:8088 \
    --lines 100000 --rate 0 --batch-size 500 --workers 8 --json
  ```

## License

The Logpushi code is released under the [MIT License](LICENSE).

Note that the bundled `logs/` datasets come from
[LogHub](https://github.com/logpai/loghub) and are **not** covered by that license; they
carry their own provenance and citation expectations. See the `README.md` files within
each `logs/` subdirectory for the per-dataset terms.

The two largest corpus files (`logs/Android_v1/Android.log`, `logs/BGL/BGL.log`) exceed
GitHub's ordinary file-size limit and are therefore stored via [Git LFS](https://git-lfs.com).
