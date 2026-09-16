# Logpushi

**Logpushi** is a CLI tool for generating realistic log activity and pushing it into **Quetzalog**, a SIEM. It replays historical log files, synthesizes "recent" activity from them, and stress-tests ingestion pipelines with configurable event volumes — from a quick 100-event functional test to million-event load runs.

> **Current state:** Logpushi is implemented in **Rust** (see [`architecture.md`](architecture.md) for design rationale). `cargo build --release` produces a single static binary; the full test suite runs with `cargo test`.

---

## Purpose

Testing a SIEM requires realistic log data at realistic — and unrealistic — volumes. Hand-crafting test events is tedious and unrepresentative. Logpushi solves this by using real, heterogeneous historical logs as raw material:

- **Replay** existing log events verbatim.
- **Synthesize** recent activity by taking historical events and rewriting their timestamps so they appear to have just happened.
- **Stress-test** Quetzalog with controlled event counts, rates, and transports.

## Features

- **Random file replay** — recursively discover log files, pick one at random, and stream it into Quetzalog.
- **Synthetic-today mode** — sample events across the corpus and transform their historical timestamps into recent ones.
- **Pluggable timestamp transformation** — detect and rewrite timestamps in many formats (syslog, ISO-8601, Apache, epoch, custom) without corrupting event content.
- **Two transports** — OpenTelemetry OTLP/HTTP (protobuf) and Splunk HTTP Event Collector, behind a common transport interface, plus `stdout` for dry runs.
- **Controlled load** — event counts, global rate limiting, batching, bounded queues, and retries.
- **Reproducibility** — deterministic random seeds for repeatable simulations.
- **Observability** — live progress and machine-readable (`--json`) throughput/error statistics.

## Architecture Overview

```text
            ┌───────────────┐
            │      CLI      │
            └───────┬───────┘
                    ▼
          ┌───────────────────┐
          │ Simulation Engine │
          └─────────┬─────────┘
                    │
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
File Discovery   Sampling     Timestamp
                              Transformation
    └───────────────┼───────────────┘
                    ▼
            ┌─────────────────┐
            │  Event Pipeline │
            └────────┬────────┘
                     ▼
             ┌─────────────────┐
             │    Transport    │
             └────────┬────────┘
                  ┌───┴───┐
                  ▼       ▼
        OpenTelemetry   Splunk HEC
                  │       │
                  └───┬───┘
                      ▼
                  Quetzalog
```

See [`architecture.md`](architecture.md) for the full design, data flow, and trade-offs.

## Requirements

- A Quetzalog instance reachable over HTTP(S), exposing either an OTLP endpoint or a Splunk-compatible HEC endpoint.
- Credentials for the chosen transport (OTLP endpoint config, or an HEC token).
- Sufficient disk space for the `logs/` corpus (~1.1 GB as shipped).

Implementation: single static binary built with `cargo` — no runtime dependencies (TLS via rustls, no OpenSSL).

## Installation

```bash
git clone <this repo>
cd logpushi
cargo build --release
./target/release/logpushi --help
```

## Test Data Sources

The log corpus in [`logs/`](logs/) comes from **[LogHub](https://github.com/logpai/loghub)**, a large collection of public system log datasets maintained by the LogPAI project. LogHub provides heterogeneous log datasets that are widely used for log parsing research, experimentation, testing, and simulation.

**These logs do not represent any production environment of your own, and they are not part of Quetzalog or Logpushi.** They are public research datasets, bundled here as simulation input. Please review and respect the licensing and citation requirements noted in the per-dataset `README.md` files under `logs/` before redistributing.

### Included datasets

| Path | Source type | Size | Example timestamp format |
|---|---|---|---|
| `logs/Apache.log` | Apache web server | 4.9 MB | `[Thu Jun 09 06:07:04 2005]` |
| `logs/Linux.log` | Linux syslog | 2.2 MB | `Jun  9 06:06:20` (no year) |
| `logs/Mac.log` | macOS syslog | 16 MB | `Jul  1 09:00:55` (no year) |
| `logs/SSH.log` | SSH/auth syslog | 70 MB | `Dec 10 06:55:46` (no year) |
| `logs/Proxifier.log` | Proxifier proxy client | 2.4 MB | `[10.30 16:49:06]` (no year) |
| `logs/Zookeeper.log` | Zookeeper (log4j) | 9.9 MB | `2015-07-29 17:41:41,536` |
| `logs/HealthApp.log` | Mobile health app | 22 MB | `20171223-22:15:29:606\|...` |
| `logs/Android_v1/Android.log` | Android (Huawei) | 183 MB | `12-17 19:31:36.263` (no year) |
| `logs/BGL/BGL.log` | BlueGene/L supercomputer | 709 MB | epoch + `2005.06.03` + `2005-06-03-15.42.50.363779` |
| `logs/HPC/HPC.log` | HPC cluster (LANL) | 32 MB | epoch seconds (`1145552216`) |
| `logs/OpenStack/` | OpenStack cloud | 59 MB | `2017-05-16 00:00:00.008` |
| `logs/Hadoop/` | Hadoop YARN containers | 48 MB | `2015-10-17 15:37:56,547` |

This mix of formats is deliberate: it exercises Logpushi's timestamp detection and heterogeneous-source handling. The architecture is designed to accommodate additional enterprise log types in the future (Windows Event Log, CEF, LEEF, firewalls, identity systems, databases, etc.), which are **not** currently included.

## Quick Start

```bash
# Replay 100 events from a randomly chosen log file
logpushi replay --lines 100

# Synthesize 1,000 "recent" events from across the corpus
logpushi synthetic-today --lines 1000
```

Transports and endpoints are selected via flags or environment variables:

```bash
logpushi replay --transport otel --endpoint https://quetzalog.example:4318/v1/logs

export LOGPUSHI_HEC_TOKEN=xxxx
logpushi synthetic-today --transport hec \
  --endpoint https://quetzalog.example:8088 \
  --lines 5000
```

## Volume Testing

A core purpose of Logpushi is stress-testing Quetzalog. Example ladder:

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

> The `--lines` value is the number of events **requested/generated**, not necessarily the number successfully ingested. Final statistics distinguish requested, generated, sent, successful, failed, and retried events. See [`architecture.md`](architecture.md) for the full event-counting model.

The tool is designed to stay memory-bounded regardless of volume: events are streamed, batched, and dispatched through bounded queues, so a 1M-event run does not require the corpus or the generated event set to fit in RAM.

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

Secrets such as HEC tokens should come from environment variables or a secret manager — never committed configuration files.

## Troubleshooting

```bash
# List discovered log files with sizes and line counts
logpushi discover

# Check the corpus: unreadable files, encodings, timestamp detection coverage
logpushi validate
```

Common issues:

- **No events ingested / auth failures** — verify the endpoint and token; authentication errors are not retried.
- **Timestamps look historical** — the event's format was not detected; check `logpushi validate` output for timestamp-detection coverage.
- **Run is slower than expected** — check the effective `--rate`, batch size, and worker count; the summary reports achieved events/sec.
- **High failure counts** — inspect retry statistics and server responses; use `--verbose` for detail.

## Security

Logpushi is a test utility, but it processes real log data, which can contain sensitive material:

- **PII** — datasets such as SSH and OpenStack logs contain IP addresses, usernames, and hostnames.
- **Secrets in logs** — real-world logs sometimes embed tokens or credentials; the bundled LogHub datasets are public research data, but inspect any corpus before pushing it to an external system.
- **Transport credentials** — HEC tokens are sensitive; use environment variables, and never print them. Logpushi will not emit secrets in normal output.
- **TLS** — TLS certificate verification is on by default. An explicit `--insecure` flag is intended for local development only.
- **Endpoint safety** — be deliberate about the target endpoint to avoid accidentally writing simulated data into a production SIEM.

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

- **Unit tests** (`cargo test`): timestamp detection/round-tripping for every dataset family, seeded sampling determinism, rate limiter, backoff jitter, HEC envelope shape, OTLP log-record shape and prost round-trip, config precedence.
- **Integration tests** (`tests/integration.rs`): full pipeline against an in-process mock transport and loopback HTTP mock HEC servers (success, 429-with-Retry-After retry, 401 abort); source logs verified untouched.
- **Load testing**: use the [Volume Testing](#volume-testing) ladder against your Quetzalog instance, or `--transport stdout` / a local mock server for isolation. Example mock harness:

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

## Contributing

Contributions are welcome. Because the implementation phase has not started, the most valuable contributions right now are feedback on the architecture document and additional log corpora for testing. Once the codebase exists, standard practices will apply: open an issue, keep changes focused, and add tests for new behavior.

## License

The code is released under the [MIT License](LICENSE).

Note that the bundled `logs/` datasets come from [LogHub](https://github.com/logpai/loghub) and are **not** covered by that license; they carry their own provenance and citation expectations. See the `README.md` files within each `logs/` subdirectory for the per-dataset terms.
