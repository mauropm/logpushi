//! Splunk HEC transport: batches of JSON envelopes posted to
//! `{endpoint}/services/collector/event`. Per-event codes from the response
//! enable partial-success attribution. Tokens are sent only via the
//! `Authorization: Splunk <token>` header and never logged.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};

use crate::event::Event;

use crate::transport::{BatchResult, Transport};

pub struct SplunkHecTransport {
    client: Arc<reqwest::Client>,
    url: String,
    token: String,
    index: Option<String>,
    source_override: Option<String>,
    sourcetype_override: Option<String>,
    host_default: String,
}

impl SplunkHecTransport {
    pub fn new(
        client: Arc<reqwest::Client>,
        endpoint: String,
        token: String,
        index: Option<String>,
        source: Option<String>,
        sourcetype: Option<String>,
        default_host: &str,
    ) -> anyhow::Result<Self> {
        // HEC convention: append the event endpoint unless a full URL is given.
        let url = normalize_hec_url(&endpoint)?;
        Ok(Self {
            client,
            url,
            token,
            index,
            source_override: source,
            sourcetype_override: sourcetype,
            host_default: if default_host.is_empty() {
                "simulated-host".to_string()
            } else {
                default_host.to_string()
            },
        })
    }

    /// Serialize one event into a HEC envelope (pure; unit-tested).
    pub fn envelope(&self, e: &Event) -> Value {
        let ts = e.timestamp.unwrap_or(e.observed);
        let host = crate::timestamp::syslog_host(&e.body).unwrap_or(self.host_default.clone());
        let mut env = json!({
            "time": hec_time(ts),
            "host": host,
            "source": self.source_override.clone().unwrap_or_else(|| e.source_file.clone()),
            "sourcetype": self.sourcetype_override.clone().unwrap_or_else(|| e.source_type.to_string()),
            "event": e.body.clone(),
        });
        if let Some(idx) = &self.index {
            env["index"] = json!(idx);
        }
        env
    }
}

/// `{base}` + `/services/collector/event` unless a full path is supplied.
pub fn normalize_hec_url(endpoint: &str) -> anyhow::Result<String> {
    let base = endpoint.trim_end_matches('/');
    if base.contains("/services/collector/") {
        return Ok(base.to_string());
    }
    Ok(format!("{base}/services/collector/event"))
}

#[async_trait::async_trait]
impl Transport for SplunkHecTransport {
    async fn send(&self, batch: &[Event]) -> BatchResult {
        let n = batch.len() as u64;
        let payload: Vec<Value> = batch.iter().map(|e| self.envelope(e)).collect();
        let Ok(body) = serde_json::to_vec(&payload) else {
            return BatchResult::none(n, false, "event body is not JSON-serializable");
        };
        let bytes = body.len() as u64;

        let resp = self
            .client
            .post(&self.url)
            .header("Authorization", format!("Splunk {}", self.token))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) if is_timeout(&e) => return BatchResult::none(n, true, "request timed out"),
            Err(e) => return BatchResult::none(n, true, sanitize(&e.to_string())),
        };

        let status = resp.status();
        let retry_after = resp
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);
        let text = resp.text().await.unwrap_or_default();

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
            return BatchResult::none(n, false, format!("unexpected HTTP {}", status.as_u16()));
        }

        // Parse per-event codes: array body => envelope-per-event codes.
        let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let mut succeeded = 0u64;
        let mut failed = 0u64;
        match parsed {
            Value::Array(items) => {
                for item in items {
                    match item.get("code") {
                        Some(c) if c.as_i64() == Some(0) => succeeded += 1,
                        _ => failed += 1,
                    }
                }
            }
            Value::Object(_) => {
                let code = parsed.get("code").and_then(|c| c.as_i64());
                match code {
                    Some(0) | None => succeeded = n,
                    Some(_) => failed = n,
                }
            }
            // Non-JSON success bodies (raw endpoint) count delivered.
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => succeeded = n,
        }
        BatchResult {
            succeeded,
            failed,
            bytes,
            retryable: false,
            auth_error: false,
            retry_after: None,
            reason: if failed > 0 {
                Some("HEC rejected some events".to_string())
            } else {
                None
            },
        }
    }

    fn name(&self) -> &'static str {
        "hec"
    }
}

fn is_timeout(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect()
}

fn sanitize(msg: &str) -> String {
    // Truncate potentially noisy reqwest errors; header tokens never appear in
    // client-side error strings (verified by construction: we pass no secrets
    // in URLs).
    const MAX: usize = 300;
    if msg.len() > MAX {
        msg[..MAX].to_string()
    } else {
        msg.to_string()
    }
}

/// Clamped sub-second precision for the HEC `time` field (JSON number, secs.millis).
pub fn hec_time(ts: chrono::DateTime<Utc>) -> f64 {
    ts.timestamp() as f64 + ts.timestamp_subsec_millis() as f64 / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use chrono::TimeZone;

    fn transport() -> SplunkHecTransport {
        SplunkHecTransport::new(
            Arc::new(reqwest::Client::new()),
            "https://splunk.example.com".to_string(),
            "TOKEN".to_string(),
            Some("idx-test".to_string()),
            None,
            None,
            "fallback-host",
        )
        .unwrap()
    }

    fn ev(body: &str) -> Event {
        Event {
            body: body.into(),
            timestamp: Some(Utc.timestamp_millis_opt(1731916800123).unwrap()),
            observed: Utc.timestamp_millis_opt(1731916800123).unwrap(),
            severity: crate::event::detect_severity("2015-10-17 15:37:56 INFO x"),
            source_file: "sub/dir/a.log".into(),
            source_type: "hadoop",
            ts_detected: true,
            ts_year_inferred: false,
            line_no: 12,
        }
    }

    #[test]
    fn envelope_shape() {
        let t = transport();
        let env = t.envelope(&ev("hello world"));
        assert!(env["time"].as_f64().unwrap() > 0.0);
        assert!(env["time"].as_f64().unwrap().fract() > 0.0, "millis kept");
        assert_eq!(env["host"], "fallback-host");
        assert_eq!(env["source"], "sub/dir/a.log");
        assert_eq!(env["sourcetype"], "hadoop");
        assert_eq!(env["index"], "idx-test");
        assert_eq!(env["event"], "hello world");
    }

    #[test]
    fn envelope_uses_syslog_host_when_present() {
        let t = transport();
        let env = t.envelope(&ev("Dec 10 06:55:46 LabSZ sshd[1]: hi"));
        assert_eq!(env["host"], "LabSZ");
    }

    #[test]
    fn envelope_time_falls_back_to_observed() {
        let t = transport();
        let mut e = ev("x");
        e.timestamp = None;
        let env = t.envelope(&e);
        assert_eq!(env["time"], hec_time(e.observed));
    }

    #[test]
    fn normalize_hec_url_appends_event_path() {
        assert_eq!(
            normalize_hec_url("https://splunk.example.com").unwrap(),
            "https://splunk.example.com/services/collector/event"
        );
        assert_eq!(
            normalize_hec_url("http://host:8088/").unwrap(),
            "http://host:8088/services/collector/event"
        );
        assert_eq!(
            normalize_hec_url("https://example.com/services/collector/event").unwrap(),
            "https://example.com/services/collector/event"
        );
    }

    #[test]
    fn hec_time_is_secs_with_millis() {
        let t = Utc.timestamp_millis_opt(1731916800123).unwrap();
        assert!((hec_time(t) - 1731916800.123).abs() < 1e-6);
    }
}
