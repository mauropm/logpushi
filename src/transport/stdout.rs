//! Default/stdout transport: writes event bodies to stdout; useful for
//! end-to-end pipeline testing with zero network.

use std::io::Write;

use crate::event::Event;
use crate::transport::{BatchResult, Transport};

#[derive(Default)]
pub struct StdoutTransport;

impl StdoutTransport {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Transport for StdoutTransport {
    async fn send(&self, batch: &[Event]) -> BatchResult {
        let mut bytes = 0u64;
        let mut out = std::io::stdout();
        for e in batch {
            let mut line = e.body.clone();
            line.push('\n');
            bytes += line.len() as u64;
            if let Err(err) = out.write_all(line.as_bytes()) {
                return BatchResult::none(
                    batch.len() as u64,
                    false,
                    format!("stdout write failed: {err}"),
                );
            }
        }
        let _ = out.flush();
        BatchResult::delivered(batch.len() as u64, bytes)
    }

    fn name(&self) -> &'static str {
        "stdout"
    }
}
