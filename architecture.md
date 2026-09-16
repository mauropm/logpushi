# Logpushi Architecture

**Status:** Implemented in Rust (see `Cargo.toml` and `src/`). This document remains the design reference; deviations from the original blueprint are listed in **§10a**.
**Scope:** This document is the implementation blueprint for Logpushi, a CLI tool that generates realistic log activity and pushes it into Quetzalog, a SIEM.

---

## 1. Purpose

Logpushi generates controlled log activity for testing and stress-testing Quetzalog. Its raw material is a corpus of real, heterogeneous historical logs (under `logs/`, sourced from LogHub). Logpushi replays those logs verbatim, or transforms them so historical events appear to have occurred moments ago, and injects them into Quetzalog at configurable volumes, rates, and protocols.

## 2. Goals

- Replay existing log files into Quetzalog with minimal modification.
- Synthesize "recent" activity from historical logs by rewriting timestamps.
- Support heterogeneous sources: syslog, Apache/Nginx-style, JSON-ish, custom application formats, structured datasets.
- Stream events end-to-end; remain memory-bounded for 1M+ event runs against a multi-gigabyte corpus.
- Support two transports — OpenTelemetry (OTLP) and Splunk HTTP Event Collector (HEC) — behind a common interface.
- Provide controlled load: event counts, a global rate limit, batching, bounded queues, retries.
- Produce reproducible simulations via random seeds.
- Report useful throughput and error statistics, in human- and machine-readable form.

## 3. Non-Goals

- **Not a log parser/research tool.** Loghub-style structure mining is out of scope; we only need enough parsing to detect and rewrite timestamps.
- **Not a production log shipper.** Logpushi is a test/load utility, not a replacement for Fluentd, Vector, OTel Collector, etc.
- **Not a log generator from templates.** Content is sampled from real logs, not synthesized from grammar rules.
- **Not a Quetzalog configuration tool.** Logpushi targets existing endpoints; it does not provision indexes or sources.
- **Not an anonymization tool.** Users are responsible for the content of the corpora they push (see §27).

## 4. Current Repository State

> **Current state:** The repository currently contains **only the log corpus** and documentation. There is no application code, build system, package manifest, or CI configuration. There is no git history in this working tree at the time of writing.

Observed contents of `logs/` (~1.1 GB total, all from [LogHub](https://github.com/logpai/loghub)):

| Path | Source | Size | Lines (approx.) | Timestamp style |
|---|---|---|---|---|
| `Apache.log` | Apache HTTP server | 4.9 MB | 56,481 | `[Thu Jun 09 06:07:04 2005]` |
| `Linux.log` | Linux syslog | 2.2 MB | 25,567 | `Jun  9 06:06:20` (no year) |
| `Mac.log` | macOS syslog | 16 MB | 117,283 | `Jul  1 09:00:55` (no year) |
| `SSH.log` | sshd/auth syslog | 70 MB | — | `Dec 10 06:55:46` (no year) |
| `Proxifier.log` | Proxifier | 2.4 MB | 21,329 | `[10.30 16:49:06]` (no year) |
| `Zookeeper.log` | Zookeeper (log4j) | 9.9 MB | 74,380 | `2015-07-29 17:41:41,536` |
| `HealthApp.log` | Mobile health app | 22 MB | — | `20171223-22:15:29:606` pipe-delimited |
| `Android_v1/Android.log` | Android | 183 MB | — | `12-17 19:31:36.263` (no year) |
| `BGL/BGL.log` | BlueGene/L | 709 MB | — | epoch + `2005.06.03` + `2005-06-03-15.42.50.363779` |
| `HPC/HPC.log` | LANL HPC | 32 MB | — | epoch seconds (`1145552216`) |
| `OpenStack/` | OpenStack (2 normal + 1 abnormal) | 59 MB | — | `2017-05-16 00:00:00.008` |
| `Hadoop/` | Hadoop YARN, 57 application dirs of container logs | 48 MB | — | `2015-10-17 15:37:56,547` |

Per-dataset `README.md` files (LogHub provenance and citations) exist under `BGL/`, `HPC/`, `Android_v1/`, `Hadoop/`. Everything in this document other than §4 describes **proposed** behavior.

Because the repository establishes no language, framework, or tooling, §5.1 records the recommended implementation stack and its rationale.

## 5. High-Level Architecture

```text
                         ┌───────────────┐
                         │      CLI      │
                         └───────┬───────┘
                                 │
                                 ▼
                       ┌───────────────────┐
                       │ Simulation Engine │
                       └─────────┬─────────┘
                                 │
                 ┌───────────────┼───────────────┐
                 │               │               │
                 ▼               ▼               ▼
          File Discovery     Sampling      Transformation
                 │               │          / Timestamp
                 └───────────────┼───────────────┘
                                 │
                                 ▼
                         ┌─────────────────┐
                         │  Event Pipeline │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │    Transport    │
                         └────────┬────────┘
                                  │
                       ┌──────────┴──────────┐
                       ▼                     ▼
                OpenTelemetry            Splunk HEC
                       │                     │
                       └──────────┬──────────┘
                                  ▼
                              Quetzalog
```

### 5.1 Recommended implementation stack

The repository pins no technology. Recommendation, with rationale:

- **Language: Go.** Single static binary, excellent concurrency primitives (goroutines, channels, bounded queues as buffered channels) that map directly onto the pipeline in this document, strong standard library for streaming IO and HTTP, mature OTLP SDK (`go.opentelemetry.io/otel/sdk` + `otlploghttp/otlploggrpc`) and battle-tested CLI ergonomics. Memory behavior for million-event streaming runs is predictable.
  - *Alternative considered:* Python (faster to prototype, LogHub tooling is Python-centric) — rejected as the default due to weaker concurrency and packaging for a load-generation CLI; a Python prototype remains viable if the implementer prefers.
- **CLI library:** Cobra (Go) — subcommands map naturally to the command set in §18; consistent flag handling and help generation.
- **HTTP client:** Go standard `net/http` with configurable timeouts and connection pooling; no heavyweight framework.
- **Configuration:** Viper or hand-rolled precedence resolution (§17). Viper is convenient but optional; the important requirement is the precedence order, not the library.
- **Testing:** Go standard `testing` plus `httptest` for mock HEC/OTLP endpoints (§28).

These are recommendations, not constraints; the architectural contracts (transport interface, event model, pipeline stages) are language-agnostic.

## 6. Component Architecture

| Component | Responsibility | Key interfaces |
|---|---|---|
| **CLI** | Argument parsing, config resolution, output formatting | `Command` per subcommand |
| **Config resolver** | Merge flags > env > file > defaults | `Config` struct |
| **File Discovery** | Recursively enumerate eligible files under `log-dir` | `Discover(dir) ([]LogFile, error)` |
| **Sampler** | Choose files/events (random, seeded) and yield raw lines as a stream | `EventSource` interface: `Next() (RawEvent, error)` |
| **Timestamp Engine** | Detect, parse, and rewrite timestamps | `Detector`, `Parser`, `Transformer` (§11–12) |
| **Event Builder** | Assemble normalized `Event` from raw line + metadata | `Build(RawEvent, SourceInfo) Event` |
| **Batcher** | Accumulate events into batches, apply backpressure | `Batcher` |
| **Rate Limiter** | Global token-bucket over sends | `Limiter.Wait()` |
| **Workers** | Concurrent batch dispatch, retries | `Worker` pool |
| **Transport** | Protocol-specific delivery | `Transport` interface (§14) |
| **Stats / Reporter** | Counters, progress, summary, JSON output | `StatsRecorder` |
| **Orchestrator** | Wire components, own lifecycle and shutdown | `Run(ctx, Config) Summary` |

## 7. Data Flow

```text
File
 ↓                 (File Discovery)
Line/Event        (streamed from disk, never fully loaded)
 ↓                 (Sampler: random file / random offset / reservoir)
Sampling
 ↓                 (Event Builder: raw line + source metadata)
Parsing
 ↓                 (Timestamp Engine — only in synthetic-today mode)
Timestamp transformation
 ↓
Event             (normalized in-memory representation)
 ↓                 (bounded channel — backpressure point)
Batch             (Batcher, size = --batch-size)
 ↓                 (rate limiter applied here, global)
Transport
 ↓
Quetzalog
```

The pipeline is pull-based: the transport side consumes batches, which pulls events through the bounded queue, which pulls lines from the sampler. Generation never outruns consumption by more than the queue bound unless explicitly configured otherwise (§22).

## 8. Log Discovery

- Recursively walk `--log-dir` (default `./logs`).
- Eligible file: regular file, readable, not obviously non-log (proposed default excludes dot-files like `.DS_Store`, zero-byte files, and files listed via `--exclude` glob patterns; LogHub `README.md`/label files inside `logs/` subdirectories should be excluded by default — they are documentation, not events).
- Follow no symlinks by default (loop safety); `--follow-symlinks` may be added later.
- `LogFile` metadata: path, relative path (used as source attribution), size, and lazily-computed line count (computed only when a command needs it, to avoid a full scan of 1 GB+ on every run).
- Encoding: assume UTF-8/ASCII; files with invalid UTF-8 are handled per §23 (proposed: skip invalid bytes or the whole file, reported in stats, never fatal by default).
- **`discover` command** prints the inventory: file, size, line count (optional, slower), and detected format family — useful before large runs.

## 9. Sampling

Two distinct sampling strategies, one per simulation mode:

### Random file replay (`replay`)
1. Discover files.
2. Select one file uniformly at random (seeded).
3. Optionally seek a random byte offset (default: start at the first full line at or after the offset) so repeated runs explore different portions of large files.
4. Stream lines sequentially from that position.
5. If the file is exhausted before `--lines` is reached: proposed default is to **wrap around to the beginning of the same file** and continue (loop), with a configurable alternative `--no-loop` (stop early and report fewer generated events than requested). Restarting a *new random file* mid-run is explicitly not proposed — it complicates reproducibility and source attribution.
6. Edge case: a file with fewer lines than requested is fine because of looping; a file with exactly one line still satisfies any count.

### Random event sampling (`synthetic-today`)
1. Discover files.
2. Select events from potentially different files over the course of the run. Proposed default: weighted file choice per event (uniform over files), then a random line within that file.
   - **Random line within a file:** for files with known line counts and newlines, a seeded random line index + seek is possible but fragile (variable-length lines require scanning). Simpler and sufficient: for each event, pick a random file, then take the *next* line from a per-file cursor initialized at a random offset, wrapping at EOF. This gives spread without precomputed indexes.
   - *Alternative:* reservoir sampling over the whole corpus — gives uniform per-event sampling but requires a full pass over 1+ GB before the first event. **Rejected** for large corpora; revisit if per-event uniformity becomes a requirement.
3. Yield sampled raw lines to the timestamp pipeline.

All randomness flows from a single seeded PRNG (`--seed`). Default seed: derived from time (non-reproducible) unless `--seed` is provided; the chosen seed is echoed in the summary so a run can be re-executed exactly.

## 10. Event Model

```text
RawEvent
├── line        string      // original content, verbatim
├── source      SourceInfo  // relative path, dataset name, line number
└── byteOffset  int64

Event
├── raw         RawEvent
├── timestamp   time.Time   // detected+transformed, or observed-time fallback
├── hasParsedTs bool        // false if timestamp could not be detected
├── severity    string      // best-effort (e.g., log4j "INFO", syslog priority); absent otherwise
└── attributes  map[string]string  // source_file, source_type, dataset, etc.
```

Principles:

- **Content preservation.** The event body (`raw.line`) is never altered except by timestamp-aware transformation in `synthetic-today` mode (§12), which touches only the detected timestamp substring.
- The normalized `Event` is the *only* thing downstream (batcher/transport) sees; simulation modes are invisible to transports.
- Events are small and short-lived; no cross-event state is retained (except per-file cursors and counters).

## 11. Timestamp Detection

A **pluggable detector chain** runs over each raw line. Each detector is (name, regex candidate, parser, confidence). Proposed order — most specific first:

| Priority | Detector | Example match | Notes |
|---|---|---|---|
| 1 | ISO-8601 / RFC3339 | `2017-05-16 00:00:00.008`, `2015-10-17T15:37:56Z` | unambiguous |
| 2 | log4j/logback | `2015-07-29 17:41:41,536` | comma millis |
| 3 | Apache/Nginx CLF | `[Thu Jun 09 06:07:04 2005]`, `09/Jun/2005:06:07:04 +0000` | |
| 4 | Epoch (numeric field) | `1117838570`, `1145552216` | **guarded**: only accept when the numeric token sits in a plausible epoch position (e.g., standalone whitespace-delimited integer of length 9–13), never inside a longer alphanumeric token; BGL's leading `1117838570` qualifies, random IDs do not |
| 5 | Syslog classic | `Jun  9 06:06:20`, `Dec 10 06:55:46` | **no year** — see below |
| 6 | Custom formats | `20171223-22:15:29:606` (HealthApp), `[10.30 16:49:06]` (Proxifier), `12-17 19:31:36.263` (Android) | registered pattern plugins |
| 7 | Structured JSON | `{"ts": "...", ...}` | field-name heuristics: `timestamp`, `ts`, `time`, `@timestamp`, `date` |

Rules:

- **First confident match wins** (longest/most-specific detector priority). A line may contain multiple candidates (e.g., BGL has three timestamp-like tokens); the priority order resolves ties deterministically.
- **Missing year** (Linux/Mac/SSH/Proxifier/Android): assume the most recent year such that the reconstructed timestamp is not in the future. This ambiguity is inherent to the data; it is acceptable for simulation purposes and noted in stats (`year_inferred` counter).
- **Missing timezone:** treat as local time by default, configurable (`--ts-timezone`). Not fatal.
- **No timestamp detected:** the event is passed through with `hasParsedTs=false`. In `synthetic-today` mode the observed time is used as the OTLP/HEC timestamp and the body is untouched. Detection misses are counted, never fatal.
- **Undetectable garbage / malformed lines:** counted (`malformed_lines`), skipped or passed through per §23.

`logpushi validate` reports per-dataset detection coverage (percentage of lines with a confidently detected timestamp), which tells the user how well `synthetic-today` will behave on their corpus.

## 12. Timestamp Transformation

Three transformation levels, mapped to modes:

1. **Raw replay (`replay`):** original event unchanged, including its embedded timestamp. Quetzalog-side timestamps come from transport envelope fields (OTLP `time_unix_nano` / HEC `time`), which may be set to send-time or omitted — proposed default: stamp with send time so ingested events look recent, while bodies remain historical. (`--preserve-body-timestamps` is a possible future flag; not proposed initially.)
2. **Timestamp-aware replay (`synthetic-today`):** only a *confidently detected* timestamp is rewritten; everything before and after the matched substring is preserved byte-for-byte. Arbitrary regex replacement over whole lines is explicitly rejected — it corrupts non-timestamp numbers (epoch-like IDs, durations, IP octets, HealthApp metrics such as `onExtend:1514038530000`), which are common in this corpus. The detector→parser→transformer chain (§11) exists precisely to avoid that.
3. **Structured transformation:** for JSON lines (or future structured formats), the *field value* is replaced (preserving key order and formatting as much as the language's JSON tooling allows); the rest of the structure is untouched. If the file is not confidently JSON, fall back to (2).

Pipeline:

```text
Raw Event
   ↓
Timestamp Detector   (find candidate substring/field)
   ↓
Timestamp Parser     (format → time.Time, with format hints from detector)
   ↓
Timestamp Transformer (choose new time per §13, render in original format)
   ↓
Updated Event        (original line with only the timestamp substring replaced)
```

Rendering must round-trip the *original format* (e.g., a rewritten Apache timestamp stays `[Thu Jun 09 06:07:04 2005]`-shaped, with correct weekday name and zero-padding), so logs remain format-plausible.

## 13. Simulation Modes

### Mode 1 — `replay` (random file replay)

Conceptual layout:

```text
logs/
├── source-a/
│   ├── log1
│   └── log2
└── source-b/
    └── log3
```

As described in §8/§9: pick a random file (random optional start offset), stream sequentially, wrap on EOF until `--lines` events have been generated, subject to failure policy (§23). Bodies are unmodified.

```bash
logpushi replay --lines 100
logpushi replay --lines 100000
```

### Mode 2 — `synthetic-today`

```text
Historical Logs
      ↓
Random Sampling        (§9)
      ↓
Timestamp Detection    (§11)
      ↓
Timestamp Transformation (§12)
      ↓
Synthetic Event
      ↓
Transport
      ↓
Quetzalog
```

Historical events from (potentially different) files are sampled; each event's content is preserved and its embedded timestamp — when confidently detected — is rewritten to fall inside a recent window.

```bash
logpushi synthetic-today --lines 1000
logpushi synthetic-today --lines 100000 --recent-window 1h --seed 42
```

### Synthetic "today" semantics

`--recent-window` defines the span into which generated timestamps are distributed. Proposed accepted values: `now` (single instant ≈ now), `5m`, `15m`, `1h` (default), `today` (since local midnight), `24h`, `Nd`. Example: `--recent-window 1h` distributes generated timestamps uniformly across the last hour ending at run start.

**Distribution strategy.** Options considered:

- **Uniform random in window (proposed default).** Simple, predictable, avoids artifacts of the source's temporal clustering (e.g., HPC logs burst every 5 seconds for hours); ideal for testing dashboards and alert timing under steady load.
- **Preserve relative spacing** (map original inter-event deltas onto the window, rescaled). Faithful to source rhythm but inherits source bursts and gaps; better suited for replaying incident shapes. Proposed as an optional strategy: `--ts-distribution uniform|preserve|source`.
- **Follow original temporal distribution** (kernel-density resampling). Highest realism, most complexity; future extension.

Default is **uniform**: for a load/simulation tool, predictable spread across the window is more valuable than inherited source artifacts, and it makes rate expectations (events/sec in Quetzalog) easy to reason about. `preserve` is recommended as the first additional strategy to implement.

Timestamps are computed from the run's anchor time (run start, or `--anchor-time` for reproducibility) and the seeded PRNG, so a fixed seed + fixed anchor yields identical synthetic timestamps.

## 14. Transport Abstraction

```text
                    Simulation Engine
                           │
                           ▼
                         Event
                           ▼
                       Transport interface
                      /         \
                     ▼           ▼
             OpenTelemetry    Splunk HEC
                     \           /
                      ▼         ▼
                        Quetzalog
```

```text
Transport (interface)
├── Send(ctx, []Event) BatchResult   // one batch; returns per-event success/failure
├── Name() string
└── Close() error

Known implementations:
├── OpenTelemetryTransport
├── SplunkHECTransport
└── Future transports (syslog, Kafka, file/stdout for testing)
```

Contract:

- The simulation engine **never** contains protocol-specific code; it produces `[]Event` and consumes `BatchResult`.
- `BatchResult` reports per-event outcomes so partial-batch failures are representable (§23).
- Adding a transport = implementing the interface + registering it in the CLI/transport registry. No simulator changes.
- A `stdout`/`file` transport is recommended as the first implementation for testing the pipeline without a SIEM.

Selection: `--transport otel|hec|stdout` (default: `stdout` until a Quetzalog endpoint is configured; env `LOGPUSHI_TRANSPORT`).

## 15. OpenTelemetry

Mapping of a Logpushi `Event` to an OTLP `LogRecord` (semantic-conventions aligned):

| OTLP LogRecord field | Source |
|---|---|
| `Body` | original log line (post timestamp-transformation if `synthetic-today`) |
| `Timestamp` | transformed/detected event timestamp; send-time fallback |
| `ObservedTimestamp` | time the event was read/generated by Logpushi |
| `SeverityText` / `SeverityNumber` | detected severity (log4j `INFO`, syslog priority, Android `I/D/W/E`); omitted or `INFO` default when undetected |
| Resource `service.name` | configured `--service-name` (default `logpushi`) |
| Resource `host.name` | derived from source log where detectable, else `--host` (default `simulated-host`) |
| Resource `source.type` | dataset family (e.g., `linux-syslog`, `apache`, `bgl`) |
| Attributes | `log.file.path` (relative source path), `log.file.name`, `logpushi.dataset`, `logpushi.mode`, `logpushi.seed`, `logpushi.ts_detected` |

- Endpoint is **fully configurable** (`--endpoint` / `LOGPUSHI_OTEL_ENDPOINT`); no Quetzalog-specific endpoint is assumed. Convention: if the user supplies the base collector URL, the OTLP default paths (`/v1/logs`) are appended; an explicit full path overrides it.
- **HTTP/protobuf (OTLP/HTTP) is the recommended default** — simpler to mock and debug (plain HTTP + protobuf, and JSON encoding for tests), works through proxies, and streaming a million events over one connection benefits less from gRPC multiplexing than typical telemetry. gRPC support is proposed as an option (`--otel-protocol http|grpc`) using the standard exporter, not a hand-rolled client.
- Batching maps 1:1 onto OTLP `LogsService/Export` request sizes (`--batch-size`).
- Retries: honor OTLP-standard behavior — retry on `429`, and on `5xx` with `Retry-After`; never retry `4xx` other than `429` (see §24).

## 16. Splunk HEC

Each Event is wrapped in a HEC envelope:

```json
{
  "time": 1234567890.123,
  "host": "simulated-host",
  "source": "logs/Linux.log",
  "sourcetype": "linux-syslog",
  "event": "Jun  9 06:06:20 combo syslogd 1.4.1: restart."
}
```

Field provenance:

| Field | Derived from source logs | Generated by Logpushi | Configured by user |
|---|---|---|---|
| `event` | ✔ (the original line) | | |
| `time` | parsed timestamp (in `synthetic-today`) | else send time | |
| `host` | when extractable (e.g., syslog hostname `combo`, `LabSZ`) | else default `simulated-host` | `--host` overrides all |
| `source` | ✔ relative file path | | `--source` override |
| `sourcetype` | dataset family heuristic | | `--sourcetype` override |
| `index` | | | `--index` (optional; no default assumed) |

- Endpoint configurable (`--endpoint` / `LOGPUSHI_HEC_ENDPOINT`, e.g. `https://quetzalog.example:8088`); the `/services/collector/event` path is appended per HEC convention unless a full URL is given. No Quetzalog-specific index or HEC configuration is assumed or documented here.
- Token via `--hec-token` or `LOGPUSHI_HEC_TOKEN` (env preferred; §17, §27). Sent as the `Authorization: Splunk <token>` header.
- HEC responses: `{"code":0}` success; `code:5` data format error, `code:4` invalid token, HTTP 429 quota. Batch mode: proposed default sends an array of envelopes per request with `--batch-size` items; per-event errors are extracted from HEC's per-event code responses (`/services/collector/event` with array payloads) — fall back to single-event sends for failing batches to attribute failures precisely (§23).
- For very high event counts, the bulk endpoint (`/services/collector/raw`) is a proposed optional optimization (`--hec-endpoint-mode event|raw`), trading per-event error attribution for throughput.

## 17. Configuration

Precedence (highest first), per key:

```text
CLI flags  →  environment variables  →  configuration file  →  defaults
```

- Config file: proposed `logpushi.yaml` in the working directory or `--config <path>`.
- Secrets (HEC token, any future credentials) are readable from env or config file, but **not** from CLI flags in examples/documentation; flags exist for convenience but env is the documented practice. Secret values are redacted in all log output.
- Validation at startup: unknown combinations fail fast with actionable messages (e.g., `--transport hec` without any token source ⇒ immediate error, not a run that fails 100% at send time).

Proposed environment variables:

| Variable | Maps to | Default |
|---|---|---|
| `LOGPUSHI_LOG_DIR` | `--log-dir` | `./logs` |
| `LOGPUSHI_TRANSPORT` | `--transport` | `stdout` |
| `LOGPUSHI_OTEL_ENDPOINT` | `--endpoint` (otel) | — |
| `LOGPUSHI_HEC_ENDPOINT` | `--endpoint` (hec) | — |
| `LOGPUSHI_HEC_TOKEN` | HEC token | — |
| `LOGPUSHI_BATCH_SIZE` | `--batch-size` | `100` |
| `LOGPUSHI_WORKERS` | `--workers` | `4` |
| `LOGPUSHI_RATE` | `--rate` | `0` (unlimited) |
| `LOGPUSHI_TIMEOUT` | `--timeout` | `30s` |
| `LOGPUSHI_SEED` | `--seed` | random |

## 18. CLI

Implemented command set (use `logpushi --help` and `logpushi <cmd> --help` for the full flags):

```text
logpushi replay          # Mode 1: random-file sequential replay
logpushi synthetic-today # Mode 2: timestamp-shifted synthetic recent events
logpushi discover        # inventory of files under --log-dir
logpushi validate        # corpus health: readability, encoding, timestamp detection coverage
logpushi (top-level flags: --version, --help)
```

Common options (retained only where architecturally justified):

```text
      --log-dir string        directory to discover logs in (default "./logs")
      --file string           restrict to a single file (skips random selection)
      --seed int              deterministic PRNG seed
      --lines int             number of events to generate (replay/synthetic)
      --transport string      otel | hec | stdout (default "stdout")
      --endpoint string       transport endpoint URL
      --batch-size int        events per batch (default 100)
      --workers int           concurrent send workers (default 4)
      --rate float            global events/sec limit; 0 = unlimited (default 0)
      --timeout duration      per-request timeout (default 30s)
      --retries int           max retries per batch (default 3)
      --insecure              disable TLS verification (development only)
      --quiet                 suppress progress output
      --verbose               detailed per-batch logging
      --json                  machine-readable summary output
```

`synthetic-today` adds: `--recent-window` (§13), `--ts-distribution`, `--ts-timezone`, `--ts-format` (restrict detectors). `replay` adds: `--start-offset random|start`, `--no-loop`. HEC adds: `--hec-token`, `--index`, `--source`, `--sourcetype`, `--host`. OTel adds: `--otel-protocol http|grpc`, `--service-name`. `discover`/`validate` use only `--log-dir`, `--file`, and output flags. Deprecated/unused options are omitted rather than accepted-and-ignored.

Exit codes: `0` success (even with partial failures reported), `1` fatal startup/config error, `2` run aborted (fatal auth/fail-fast failure or signal).

## 19. Batching

```text
Event stream
     ↓
Batcher (accumulates N events or T duration, whichever first)
     ↓
Batch ([]Event, size ≤ --batch-size)
     ↓
Workers (--workers concurrent)
     ↓
Transport.Send
```

- Proposed defaults: `--batch-size 100`, flush timeout 1s (so low-rate runs don't stall).
- **Throughput:** batching amortizes HTTP overhead; this is the single biggest throughput lever for OTLP/HEC.
- **Memory:** batch size bounds per-request memory; total buffered events ≤ `queue bound + batch-size × workers`.
- **Failure semantics:** a batch may partially fail (HEC per-event codes; OTLP is all-or-nothing per request in practice). `BatchResult` must represent partial success; failed events are counted and optionally retried individually (§24).
- **Ordering:** with `workers > 1`, delivery order is not guaranteed. Ordering is a non-goal for load testing; set `--workers 1` for ordered delivery.
- **Shutdown:** on SIGINT/SIGTERM, stop the generator, drain in-flight batches with the configured timeout, then print the summary. No double-send of in-flight batches; unsent generated events are counted as `not_sent`.

## 20. Concurrency

- Generator (sampler + transformer) runs as one goroutine feeding a bounded channel; `W` workers consume batches; a stats goroutine aggregates counters atomically.
- **Why not more concurrency everywhere:** the bottleneck in load generation is usually the server (Quetzalog) or the network, not the generator (timestamp transformation is cheap regex work). Workers beyond the saturation point add latency, socket contention, and server-side 429/5xx noise. Proposed default `--workers 4`; load tests (§29) should establish the knee for a given Quetzalog deployment.
- Worker count affects ordering (§19) and the relationship with rate limiting (§21).

## 21. Rate Limiting

- Semantics: `--rate R` = maximum **events successfully offered to the transport** per second, globally. `--rate 0` = unlimited (default) — the explicit stress-test mode.
- **Global limiter, not per-worker.** With per-worker limits, effective rate = R × workers, which surprises users and makes `--rate` meaningless as a contract. A single token-bucket (capacity ~1 batch, refill R/sec) is shared by all workers; a worker blocks on `Limiter.Wait()` before sending a batch. Concurrency then controls *how many requests are in flight*, while the limiter controls *aggregate events/sec* — the two knobs stay orthogonal.
- Rate limiting is applied per-batch (wait for `len(batch)/R` worth of tokens), not per-event, to avoid per-event lock contention at high rates.
- `0 = unlimited` is the selected design (documented in `--help`), because stress tests legitimately want unthrottled injection; safety comes from the bounded queue and the explicitness of the flag.
- Stats report both requested rate and achieved rate (events/sec), so an unreachable target is visible.

## 22. Backpressure

```text
Generator
    │
    ▼
Bounded Queue  (capacity ~ workers × batch-size × small factor, e.g. 2–4 batches' worth)
    │
    ▼
Batcher
    │
    ▼
Workers
    │
    ▼
Transport
```

- The bounded queue is the **only** buffering between generation and sending. If the transport is slower than the generator (or the rate limiter is engaged), the generator blocks on enqueue — the system is self-regulating by construction.
- Unlimited in-memory buffering is rejected: at 1M events × ~200 bytes, buffering the full run would need hundreds of MB for zero benefit, and hides server-side slowness until memory pressure forces failure.
- Consequently, Logpushi **cannot** generate faster than it can send (modulo queue slack), unless `--rate 0` and a fast transport allow it to — and even then only by the queue bound. This is intentional: load tests should measure *delivered* load, not queued aspiration. (If a future use case needs decoupled "generate-then-flush", it should be a separate explicit mode, not silent buffering.)

## 23. Error Handling

Classification (proposed):

| Error | Class | Behavior |
|---|---|---|
| Malformed input line (no timestamp, garbage) | skippable | count `malformed_lines`; pass through body as-is (timestamp fallback) — never fatal |
| Unreadable file / permission denied | skippable at discovery | warn, exclude from sampling, count; fatal only if `--file` was explicitly requested |
| Encoding errors (invalid UTF-8) | skippable | skip invalid bytes or file; count `encoding_errors` |
| Missing/undetectable timestamp | skippable | count; use fallback timestamp semantics (§11) |
| Invalid timestamp (parsed but nonsense, e.g. month 13) | skippable | treat as undetected; count |
| HTTP timeout / connection refused / reset | retryable | retry with backoff (§24) |
| HTTP 429 | retryable | honor `Retry-After` if present |
| HTTP 5xx | retryable | retry with backoff |
| HTTP 401/403 (auth) | fatal-ish | retry **once** then abort run (misconfiguration, retry storms are pointless); clear message |
| HEC non-zero event code (e.g. code 5) | skippable | count event as failed; do not retry (deterministic data error) |
| OTLP export error response | retryable per §15 rules | else failed |
| Partial batch failure | mixed | failed subset handled per above; successes counted |
| Context cancellation (Ctrl-C) | shutdown | drain, summarize, exit 0/2 |

Failure policy for the run: proposed default is **continue on failures** (count and proceed until `--lines` generated events have been produced), with `--fail-fast` to abort on the first batch failure. The summary always reports requested vs sent vs failed, so "requested ≠ ingested" is explicit.

## 24. Retry Strategy

- **Exponential backoff with jitter:** base 500ms, factor 2, cap 30s, full jitter; `--retries` bounds attempts (default 3) per batch.
- Retryable: network errors, timeouts, `429` (honor `Retry-After`, overriding computed backoff when longer), `5xx`.
- **Not retried:** `401/403` (auth — abort after one confirmation attempt), `4xx` validation errors (HEC code 5, malformed payloads — deterministic failures), context cancellation.
- **Retry-storm prevention:** global cap on in-flight retries (retried batches go through the same rate limiter), jitter, bounded attempts, and the abort-on-auth rule. A failing endpoint therefore produces at most `retries+1` attempts per batch, with backoff-spread timing, never a tight loop.
- Retried events/batches are counted (`retried`) separately from failures.

## 25. Memory Management

- Nothing requires the whole corpus or the whole event set in RAM: files are streamed line-by-line (`bufio.Scanner` with a generous token limit for very long lines, falling back to chunked reads), events are transient, and buffering is bounded by the queue + in-flight batches (§22).
- Expected steady-state footprint (proposed): queue bound + `batch-size × workers` events + per-file cursors (one small struct per discovered file) + HTTP buffers. Independent of corpus size (1.1 GB here) and of `--lines`.
- Random access to large files uses seek, not reads-to-position.
- Line counts are computed lazily and never cached for the whole corpus unless `discover` explicitly requests them.
- Implication for sampling (§9): per-event uniform sampling across the corpus would require either an index or a full pass; the chosen cursor-based strategy keeps memory O(#files).

## 26. Observability

- Live progress (non-quiet, non-TTY-agnostic): events generated/sent/failed, current rate, elapsed — updated in place when attached to a TTY, appended periodically otherwise.
- Final summary, human-readable:

```text
Logpushi
──────────────────────────────────────
Mode:              synthetic-today
Source files:      143
Events requested:  100,000
Events generated:  100,000
Events sent:       99,842
Events failed:     158
Transport:         OpenTelemetry
Rate:              4,231 events/sec
Elapsed:           23.63s
──────────────────────────────────────
```

- Distinct counters (§21/§23/§24 vocabulary): `requested`, `generated`, `sent`, `successful`, `failed`, `retried`, plus `malformed_lines`, `ts_detected`, `ts_year_inferred`, `bytes_sent`.
- `--json`: same summary as a machine-readable JSON object (seed, config echo, counters, achieved rate, duration) — for CI and scripted load tests.
- `--quiet`: suppress progress, still print summary (or only exit code + JSON if combined with `--json`). `--verbose`: per-batch outcomes, retry decisions, per-file sampling breakdown.
- Secrets (tokens) never appear in any output, including verbose and JSON (§27).

## 27. Security

- **Sensitive data in inputs.** Public research logs still contain real-looking IPs, usernames, hostnames, and emails; other corpora a user points at `logs/` may contain PII, credentials, tokens, or production secrets. **Recommendation: inspect datasets before sending to any external system, and run against controlled test endpoints.**
- **Transport credentials.** HEC tokens via env (`LOGPUSHI_HEC_TOKEN`) preferably; redacted everywhere in output and never logged at any verbosity level. Config files containing tokens should be chmod-restricted and git-ignored.
- **TLS verification on by default.** `--insecure` exists for local development against self-signed endpoints; documentation must state it is for development only. Insecure mode prints a warning banner at startup.
- **Endpoint confusion.** Simulated data pushed to a production SIEM is an incident-in-waiting. Mitigations: require an explicit `--endpoint` (no implicit production default), and print the resolved target at startup.
- **Reproducibility artifacts.** `--json` output includes config echo; ensure it redacts secrets.

## 28. Testing

### Unit tests
- File discovery: nested dirs, exclusion defaults (READMEs, dot-files), symlinks, empty dirs.
- Random selection & seed behavior: same seed ⇒ same file choice, same offsets, same synthetic timestamps; different seeds ⇒ (probabilistically) different.
- Timestamp detection: one golden test per dataset family in §4 (Apache, syslog-no-year, log4j, ISO, epoch-guard cases incl. BGL false-positive traps, HealthApp, Proxifier, Android, OpenStack, JSON).
- Timestamp parsing/transformation: format round-tripping, year inference, timezone handling, invalid values.
- JSON transformation: field replacement, structure preservation.
- Malformed input handling; sampling strategies; batcher (size/timeout flush); rate limiter (global semantics, burst behavior); backpressure (queue bounds respected).

### Transport tests
- OTLP: serialization correctness (body, timestamps, severity, resource/attributes), export request shape, retry-on-429/5xx with `Retry-After`, no-retry-on-401.
- HEC: envelope correctness, per-event code parsing, array vs single fallback, raw-mode option, token header, auth failure handling.
- Timeout behavior, partial-failure attribution, context-cancellation mid-batch.

### Integration tests
- Full pipeline against a **local mock endpoint** (`httptest` server implementing a minimal HEC; OTLP via a local collector or mock). Assert end-state counters and event content on the wire. A `stdout` transport variant tests the pipeline with zero network.
- Both modes (`replay`, `synthetic-today`) end-to-end on a small fixture corpus.

### Load tests
- Ladder: 100 / 1,000 / 10,000 / 100,000 / 1,000,000+ events (against a mock or staging Quetzalog), measuring **throughput (events/sec), CPU, memory (RSS), latency (per-batch), and failure rate**.
- Assertions: memory stays bounded as `--lines` grows (e.g., RSS at 1M ≈ RSS at 10K + ε); achieved rate tracks `--rate` within tolerance; no goroutine leaks (run with race detector and goroutine-leak checks in CI).

## 29. Performance Testing

Separate from CI load tests: scheduled/bench-style runs that characterize the tool itself.

- Generator ceiling: events/sec with `--transport stdout --rate 0` (upper bound of sampling + transformation).
- Transport ceilings: per-protocol throughput vs `--batch-size` × `--workers` matrix on a loopback mock.
- Realistic target: establish achievable events/sec against a reference Quetzalog instance and document it (e.g., the example figure "4,231 events/sec" in §26 is illustrative, not a benchmark claim).
- Profile (`pprof`) at high rates to verify transformation is not a bottleneck; the queue-depth metric should sit near zero when the limiter is the binding constraint.

## 30. Extensibility

Extension points and the cost of each:

| Extension | Mechanism | Expected effort |
|---|---|---|
| New transport | implement `Transport`, register in CLI/config | small, no simulator changes |
| New timestamp format | register detector (regex + parser + renderer) in the chain | small, table-driven |
| New structured format (CEF, LEEF) | structured transformer behind the §12 level-3 hook | medium |
| New distribution strategy | `--ts-distribution` strategy interface | small |
| New sampling strategy | `EventSource` implementation | medium |
| Config file formats | config resolver layer | small |

The corpus itself is an extension point: dropping files under `logs/` (or pointing `--log-dir` elsewhere) requires no code, provided discovery defaults stay permissive-but-safe (§8).

## 31. Future Architecture

- **Additional enterprise log sources:** Linux, Windows (Event Log), macOS, Solaris, SunOS, AIX, OS/2, mainframe, Apache, Nginx, databases, firewalls, VPNs, identity systems, cloud services, security products, CEF, LEEF, JSON, syslog, custom enterprise formats. **None of these are currently supported or bundled** beyond the LogHub datasets in §4; the detector/transformer plug-in model (§11–12) is the designed path for them.
- **Time-shaping beyond uniform:** source-preserving and density-based distributions (§13).
- **Scenario engine:** scripted mixes (e.g., "90% noise + SSH brute-force burst") for detection testing — would layer on the sampler.
- **Distributed load generation:** multi-process/multi-host coordination with a shared rate budget.
- **Quetzalog-native protocol,** if one emerges: another `Transport` implementation.
- **Checkpoint/resume** for very long runs.

## 32. Open Questions

1. **Language/runtime confirmation.** Go is recommended (§5.1); confirm before implementation, since CLI packaging and the OTLP SDK choice follow from it.
2. **Exact Quetzalog ingestion endpoints.** OTLP and HEC are assumed compatible; exact paths, TLS requirements, and any Quetzalog-specific headers need confirmation against a real deployment. Everything is kept configurable pending that.
3. **HEC batch mode default.** Array-per-request (`event` endpoint) vs `raw` bulk (§16) — decide after measuring both against a real Quetzalog.
4. **Wrap-around behavior in `replay`** (§9): loop-by-default vs stop-at-EOF. Proposed: loop; confirm intent.
5. **No-year datasets:** is "most recent past year" inference acceptable for all use cases, or should it be configurable per dataset?
6. **Per-event uniform sampling:** is cursor-based sampling (§9) sufficient, or is full-corpus uniformity required (implying indexing or a full pre-pass)?
7. **License** for the Logpushi code — **resolved**: MIT (see `LICENSE`); the LogHub corpus under `logs/` carries its own provenance separately.
8. **Config file format and location** (YAML proposed; confirm whether a Quetzalog-adjacent convention should be followed).

## 10a. Implementation Status & Deviations (post-implementation)

The blueprint is implemented in Rust. Where the implementation differs from the
original proposal, the deviation and rationale are listed here.

| Area | Blueprint | Implemented | Rationale |
|---|---|---|---|
| Language | Go recommended (§5.1, question 1) | **Rust** (tokio, reqwest+rustls, prost) | Task requirement; single static binary, memory safety, no OpenSSL dependency |
| Config file format | YAML (question 8) | **JSON** flat keys (`logpushi.json`) | `serde_yaml` unmaintained; flat JSON keys mirror CLI flags directly |
| OTLP wire protocol | gRPC + HTTP (§4.1) | **OTLP/HTTP only** (protobuf over HTTP POST) | gRPC would require tonic/protoc; HTTP is what Quetzalog exposes for logs. `--otel-protocol grpc` is rejected with a config-validation error |
| OTLP serialization | OTLP SDK | **Hand-written prost `Message` structs** (correct field tags per OTLP v1) | Avoids codegen build dependency; payload verified against the spec |
| Replay body timestamp | stamp recent observed time into body where compatible (§9?) | **Bodies verbatim in replay**; transport envelopes carry observed (send) time | Replay means "bodies untouched"; ingestion recency comes from the transport envelope (`time`/`observed_time_unix_nano`), so historical content is preserved byte-for-byte |
| No-year datasets | question 5 | Most-recent-past-year for syslog/proxifier/android detectors | Confirmed sensible for synthetic-today; reportable via `ts_year_inferred` stat |
| Per-event sampling | question 6 | Full-corpus uniform via **random file + random byte offset per event**, O(1) memory/fds | Cursor-based approaches correlate consecutive events; random offsets give uniform coverage with deterministic seeding |
| HEC batch mode | question 3 | Single-event-per-line batches posted to `/services/collector/event` (array of one event per line) | Protocol-compatible default; `raw` bulk endpoint remains a possible extension |

### Implemented timestamp formats (§11 order)

ISO-8601 (space/T separator, `.`,`,` fractions, `Z`/±HH:MM/±HHMM, missing-tz→local) →
Apache CTF `[Thu Jun 09 06:07:04 2005]` → Apache CLF `10/Jun/2005:06:07:04 +0000` →
numeric epoch (guarded: whitespace-delimited token, 9–13 digits, plausible range: seconds ≤10 digits, millis 11–13) →
syslog no-year `Dec 10 06:55:46` (with year inference) →
HealthApp `20171223-22:15:29:606` (epoch-like metric values excluded by the guard) →
Proxifier `[10.30 16:49:06]` →
Android `12-17 19:31:36.263`.

JSON lines get field-level rewrites for keys `timestamp, @timestamp, ts, time, date, datetime, _time` with key order preserved; non-JSON lines get substring replacement of exactly the detected span. Untouched content is byte-identical to source.

### Event-counting model (verified)

requested → generated (== requested unless sampling/encoding errors) → sent (delivered to transport; transport reported success) / failed (final failure after retries) / retried (re-delivery attempts; both sent and failed may contain retried events) → aborted flag when auth fails after one confirmation retry or `--fail-fast` trips; exit code 2.

### Test suite

`cargo test`: 41 unit tests (discovery, sampling/seeding, timestamp golden tests per format incl. corrupted-neighborhood guards, rate limiter, backoff jitter, replay/synthetic event-building, HEC envelope, OTLP record + protobuf round-trip, config defaults) and 7 integration tests (mock-transport pipeline counts; synthetic JSON field transform; batch flush timing; loopback-HTTP HEC success/429-retry/401-abort; corpus unmodified asssertions).
