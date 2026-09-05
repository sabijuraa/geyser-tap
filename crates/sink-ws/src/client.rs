//! WebSocket client tracking.

use geyser_tap_common::SinkResult;
use tokio::sync::mpsc;

/// A connected WebSocket client.
#[allow(dead_code)]
pub struct WsClient {
    id: u64,
    tx: mpsc::Sender<String>,
}

#[allow(dead_code)]
impl WsClient {
    /// Create a new client.
    pub fn new(id: u64, tx: mpsc::Sender<String>) -> Self {
        Self { id, tx }
    }

    /// Get client ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Send a message to the client.
    pub async fn send(&self, msg: &str) -> SinkResult<()> {
        self.tx
            .send(msg.to_string())
            .await
            .map_err(|_| geyser_tap_common::SinkError::Send("client disconnected".to_string()))
    }
}
