//! OpenTelemetry (OTLP/HTTP, protobuf) transport.
//!
//! Minimal hand-written protobuf structs (prost `Message` derive) covering the
//! `ExportLogsServiceRequest` shape needed for logs. This avoids a protoc
//! build dependency while staying wire-compatible with any OTLP receiver,
//! including Quetzalog. Field numbers follow opentelemetry-proto v1.

use std::sync::Arc;

use bytes::BytesMut;
use prost::Message as ProstMessage;
use std::time::Duration;

use crate::event::Event;
use crate::transport::{BatchResult, Transport};

// ---------------------------------------------------------------------------
// Minimal OTLP protobuf v1 messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ExportLogsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ExportLogsServiceResponse {
    #[prost(message, optional, tag = "1")]
    pub partial_success: Option<ExportLogsPartialSuccess>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ExportLogsPartialSuccess {
    #[prost(int64, optional, tag = "1")]
    pub rejected_log_records: Option<i64>,
    #[prost(string, optional, tag = "2")]
    pub error_message: Option<String>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ResourceLogs {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(string, optional, tag = "3")]
    pub schema_url: Option<String>,
    #[prost(message, repeated, tag = "2")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ScopeLogs {
    #[prost(string, optional, tag = "1")]
    pub schema_url: Option<String>,
    #[prost(message, repeated, tag = "2")]
    pub log_records: Vec<LogRecord>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct AnyValue {
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 7")]
    pub value: Option<any_value::Value>,
}

pub mod any_value {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
        #[prost(int64, tag = "2")]
        IntValue(i64),
        #[prost(bool, tag = "3")]
        BoolValue(bool),
        #[prost(double, tag = "4")]
        DoubleValue(f64),
        #[prost(bytes, tag = "7")]
        BytesValue(Vec<u8>),
    }
}

impl From<String> for any_value::Value {
    fn from(s: String) -> Self {
        any_value::Value::StringValue(s)
    }
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct LogRecord {
    /// Event time (ns since unix epoch).
    #[prost(fixed64, tag = "1")]
    pub time_unix_nano: u64,
    /// SeverityNumber enum (uint32).
    #[prost(uint32, optional, tag = "2")]
    pub severity_number: Option<u32>,
    #[prost(string, optional, tag = "3")]
    pub severity_text: Option<String>,
    #[prost(message, optional, tag = "5")]
    pub body: Option<AnyValue>,
    #[prost(message, repeated, tag = "6")]
    pub attributes: Vec<KeyValue>,
    /// Observed time (ns) — when Logpushi generated the event.
    #[prost(fixed64, tag = "11")]
    pub observed_time_unix_nano: u64,
}

/// Map a detected level keyword to OTLP SeverityNumber (v1 numeric scale).
fn severity_number(level: Option<&str>) -> u32 {
    match level {
        Some("DEBUG") | Some("TRACE") | Some("VERBOSE") => 5,
        Some("NOTICE") | Some("INFO") => 9,
        Some("WARN") => 13,
        Some("ERROR") => 17,
        Some("CRITICAL") | Some("FATAL") => 21,
        _ => 9,
    }
}

fn attr(key: &str, value: impl Into<any_value::Value>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(value.into()),
        }),
    }
}

/// Map a Logpushi Event to an OTLP LogRecord (pure; unit-tested).
pub fn to_log_record(e: &Event, _service_name: &str, host_default: &str, seed: u64) -> LogRecord {
    let host = e.extracted_host();
    let _host = if host.is_empty() { host_default } else { &host };
    LogRecord {
        time_unix_nano: e
            .timestamp
            .unwrap_or(e.observed)
            .timestamp_nanos_opt()
            .unwrap_or_default() as u64,
        observed_time_unix_nano: e.observed.timestamp_nanos_opt().unwrap_or_default() as u64,
        severity_number: Some(severity_number(e.severity)),
        severity_text: Some(e.severity.unwrap_or("INFO").to_string()),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(e.body.clone())),
        }),
        attributes: vec![
            attr("log.file.path", e.source_file.clone()),
            attr(
                "log.file.name",
                e.source_file.rsplit('/').next().unwrap_or("").to_string(),
            ),
            attr("source.type", e.source_type.to_string()),
            attr("logpushi.mode", "simulation".to_string()),
            attr("logpushi.seed", format!("{seed}")),
            attr(
                "logpushi.ts_detected",
                if e.ts_detected { "true" } else { "false" }.to_string(),
            ),
        ],
    }
}

/// Build one ExportLogsServiceRequest around a resource/span-scope pair.
pub fn build_request(
    events: &[Event],
    service_name: &str,
    host_default: &str,
    seed: u64,
) -> ExportLogsServiceRequest {
    let resource = Resource {
        attributes: vec![attr("service.name", service_name.to_string())],
    };
    let resource_logs = ResourceLogs {
        resource: Some(resource),
        schema_url: None,
        scope_logs: vec![ScopeLogs {
            schema_url: None,
            log_records: events
                .iter()
                .map(|e| to_log_record(e, service_name, host_default, seed))
                .collect(),
        }],
    };
    ExportLogsServiceRequest {
        resource_logs: vec![resource_logs],
    }
}

/// Convention: a bare base URL gets the default OTLP/HTTP logs path appended;
/// an explicit full path (anything after the authority) overrides it.
pub fn normalize_otel_url(endpoint: &str) -> anyhow::Result<String> {
    let base = endpoint.trim_end_matches('/');
    let has_path = base
        .split_once("://")
        .map(|(_, rest)| match rest.split_once('/') {
            Some((_, p)) => !p.is_empty(),
            None => false,
        })
        .unwrap_or(false);
    Ok(if has_path {
        base.to_string()
    } else {
        format!("{base}/v1/logs")
    })
}

pub struct OtelTransport {
    client: Arc<reqwest::Client>,
    url: String,
    service_name: String,
    host_default: String,
}

impl OtelTransport {
    pub fn new(
        client: Arc<reqwest::Client>,
        endpoint: String,
        service_name: &str,
        default_host: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            url: normalize_otel_url(&endpoint)?,
            service_name: service_name.to_string(),
            host_default: if default_host.is_empty() {
                "simulated-host".to_string()
            } else {
                default_host.to_string()
            },
        })
    }
}

#[async_trait::async_trait]
impl Transport for OtelTransport {
    async fn send(&self, batch: &[Event]) -> BatchResult {
        let n = batch.len() as u64;
        let request = build_request(batch, &self.service_name, &self.host_default, 0);
        let mut buf = BytesMut::with_capacity(256 * batch.len());
        if ProstMessage::encode(&request, &mut buf).is_err() {
            return BatchResult::none(n, false, "protobuf encoding failed");
        }
        let bytes = buf.len() as u64;

        let resp = self
            .client
            .post(&self.url)
            .header("Content-Type", "application/x-protobuf")
            .body(buf.freeze())
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                let reason = if e.is_timeout() || e.is_connect() {
                    "request timed out or connection failed"
                } else {
                    "network error"
                };
                return BatchResult::none(n, true, reason);
            }
        };

        let status = resp.status();
        let retry_after = resp
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);

        if status.as_u16() == 401 || status.as_u16() == 403 {
            return BatchResult {
                succeeded: 0,
                failed: n,
                bytes: 0,
                retryable: true,
                auth_error: true,
                retry_after: None,
                reason: Some(format!("authentication failed (HTTP {})", status.as_u16())),
            };
        }
        if status.as_u16() == 429 || status.as_u16() >= 500 {
            let reason = if status.as_u16() == 429 {
                "rate limited (429)"
            } else {
                "server error (5xx)"
            };
            return BatchResult {
                retry_after,
                ..BatchResult::none(n, true, reason)
            };
        }
        if !status.is_success() {
            // OTLP contract: 4xx other than 429 is NOT retryable.
            return BatchResult::none(
                n,
                false,
                format!("OTLP export rejected (HTTP {})", status.as_u16()),
            );
        }

        // HTTP 200: either full success or a PartialSuccess payload.
        let body_bytes = resp.bytes().await.unwrap_or_default();
        let mut failed = 0u64;
        if let Ok(resp) = ExportLogsServiceResponse::decode(&body_bytes[..]) {
            if let Some(ps) = resp.partial_success {
                failed = ps.rejected_log_records.unwrap_or(0).max(0) as u64;
            }
        }
        BatchResult {
            succeeded: n.saturating_sub(failed),
            failed,
            bytes,
            retryable: false,
            auth_error: false,
            retry_after: None,
            reason: if failed > 0 {
                Some("OTLP partial success (records rejected)".into())
            } else {
                None
            },
        }
    }

    fn name(&self) -> &'static str {
        "otel"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(body: &str) -> Event {
        Event {
            body: body.into(),
            timestamp: Some(chrono::Utc::now()),
            observed: chrono::Utc::now(),
            severity: Some("ERROR"),
            source_file: "x/app.log".into(),
            source_type: "linux-syslog",
            ts_detected: true,
            ts_year_inferred: false,
            line_no: 4,
        }
    }

    #[test]
    fn log_record_fields() {
        let r = to_log_record(&ev("boom"), "svc", "dh", 7);
        assert!(r.time_unix_nano > 0);
        assert!(r.observed_time_unix_nano > 0);
        assert_eq!(
            r.severity_number,
            Some(17),
            "ERROR → SeverityNumber 17 per OTLP"
        );
        assert_eq!(r.severity_text.as_deref(), Some("ERROR"));
        match &r.body {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) => assert_eq!(s, "boom"),
            other => panic!("bad body {other:?}"),
        }
        let keys: Vec<String> = r.attributes.iter().map(|a| a.key.clone()).collect();
        assert!(keys.contains(&"log.file.name".to_string()));
        assert!(keys.contains(&"source.type".to_string()));
    }

    #[test]
    fn build_request_bundles_resource_and_records() {
        let req = build_request(&[ev("a"), ev("b")], "logpushi", "host", 3);
        let rl = &req.resource_logs[0];
        match &rl.resource.as_ref().unwrap().attributes[0].value {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) => assert_eq!(s, "logpushi"),
            other => panic!("bad attr {other:?}"),
        }
        let n = rl.scope_logs[0].log_records.len();
        assert_eq!(n, 2);
        // Wire round-trip: bytes decode back with the same record count.
        let bytes = req.encode_to_vec();
        let back = ExportLogsServiceRequest::decode(&bytes[..]).unwrap();
        assert_eq!(back.resource_logs[0].scope_logs[0].log_records.len(), 2);
    }

    #[test]
    fn normalize_otel_url_variants() {
        assert_eq!(
            normalize_otel_url("http://otel:4318").unwrap(),
            "http://otel:4318/v1/logs"
        );
        assert_eq!(
            normalize_otel_url("http://otel:4318/my/ingest").unwrap(),
            "http://otel:4318/my/ingest"
        );
        assert_eq!(
            normalize_otel_url("http://otel:4318/v1/logs").unwrap(),
            "http://otel:4318/v1/logs"
        );
    }
}
