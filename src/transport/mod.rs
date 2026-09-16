//! Transport abstraction: the simulation engine produces `Vec<Event>` batches
//! and consumes `BatchResult`, never seeing protocol specifics.

pub mod hec;
pub mod otel;
pub mod stdout;

use crate::event::Event;

/// Per-batch outcomes (partial success representable).
#[derive(Debug, Clone)]
pub struct BatchResult {
    pub succeeded: u64,
    pub failed: u64,
    /// Approximate bytes counted as delivered by the transport.
    pub bytes: u64,
    /// True when the failure may resolve on retry (network, 429, 5xx).
    pub retryable: bool,
    /// Auth error: retry storms are pointless; pipelines abort after one retry.
    pub auth_error: bool,
    /// Honor Retry-After when longer than computed backoff.
    pub retry_after: Option<std::time::Duration>,
    /// Short, sanitized diagnostics safe for logs (no tokens, no bodies).
    pub reason: Option<String>,
}

impl BatchResult {
    /// Full batch success.
    pub fn delivered(n: u64, bytes: u64) -> Self {
        BatchResult {
            succeeded: n,
            failed: 0,
            bytes,
            retryable: false,
            auth_error: false,
            retry_after: None,
            reason: None,
        }
    }

    pub fn none(n: u64, retryable: bool, reason: impl Into<String>) -> Self {
        BatchResult {
            succeeded: 0,
            failed: n,
            bytes: 0,
            retryable,
            auth_error: false,
            retry_after: None,
            reason: Some(reason.into()),
        }
    }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// Send one batch. The batch is borrowed: retries re-send without cloning.
    async fn send(&self, batch: &[Event]) -> BatchResult;
    fn name(&self) -> &'static str;
    async fn close(&self) {}
}

/// Build the configured transport.
pub fn build(cfg: &crate::config::Config) -> anyhow::Result<std::sync::Arc<dyn Transport>> {
    use crate::config::TransportKind as T;
    let builder = reqwest::Client::builder()
        .danger_accept_invalid_certs(cfg.insecure)
        .timeout(cfg.timeout);
    let client = std::sync::Arc::new(builder.build()?);
    match cfg.transport {
        T::Stdout => Ok(std::sync::Arc::new(stdout::StdoutTransport::new())),
        T::Otel => {
            let endpoint = cfg.endpoint.clone().expect("validated: endpoint required");
            Ok(std::sync::Arc::new(otel::OtelTransport::new(
                client,
                endpoint,
                &cfg.service_name,
                &cfg.host,
            )?))
        }
        T::Hec => {
            let token = cfg.hec_token.clone().expect("validated: token");
            Ok(std::sync::Arc::new(hec::SplunkHecTransport::new(
                client,
                cfg.endpoint.clone().expect("validated: endpoint"),
                token,
                cfg.hec_index.clone(),
                cfg.hec_source.clone(),
                cfg.hec_sourcetype.clone(),
                &cfg.host,
            )?))
        }
    }
}
