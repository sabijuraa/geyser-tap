//! WebSocket server implementation.

use crate::client::WsClient;
use geyser_tap_common::{SinkError, SinkResult, Update, WsSinkConfig};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct WsServerConfig {
    /// Bind address
    pub bind_address: SocketAddr,
    /// Max concurrent clients
    pub max_clients: usize,
    /// Per-client send buffer size
    pub send_buffer_size: usize,
}

impl From<&WsSinkConfig> for WsServerConfig {
    fn from(config: &WsSinkConfig) -> Self {
        Self {
            bind_address: config
                .bind_address
                .parse()
                .unwrap_or_else(|_| "0.0.0.0:10001".parse().unwrap()),
            max_clients: config.max_clients,
            send_buffer_size: config.send_buffer_size,
        }
    }
}

/// Server state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    NotStarted,
    Running,
    ShuttingDown,
    Stopped,
}

/// WebSocket server.
#[allow(dead_code)]
pub struct WsServer {
    config: WsServerConfig,
    clients: DashMap<u64, WsClient>,
    state: RwLock<ServerState>,
    next_client_id: AtomicU64,
    shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,
}

impl WsServer {
    /// Create a new server.
    pub fn new(config: WsServerConfig) -> Self {
        Self {
            config,
            clients: DashMap::new(),
            state: RwLock::new(ServerState::NotStarted),
            next_client_id: AtomicU64::new(1),
            shutdown_tx: RwLock::new(None),
        }
    }

    /// Start the server.
    ///
    /// Binds before marking the server running, so a bind failure leaves the
    /// state accurate and is reported to the caller.
    pub async fn start(&self) -> SinkResult<()> {
        // Scope the guard: a parking_lot guard is not Send, and holding one
        // across the await below makes the whole future non-Send, which the
        // Sink trait requires.
        {
            let state = self.state.read();
            if *state != ServerState::NotStarted {
                return Err(SinkError::NotReady("Server already started".to_string()));
            }
        }

        let bind_addr = self.config.bind_address;

        let listener = TcpListener::bind(bind_addr).await.map_err(|e| {
            SinkError::Connection {
                endpoint: bind_addr.to_string(),
                message: format!("failed to bind websocket listener: {e}"),
            }
        })?;

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.write() = Some(shutdown_tx);
        *self.state.write() = ServerState::Running;

        tracing::info!(address = %bind_addr, "WebSocket server listening");

        // Accept connections in a background task
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                tracing::debug!(remote = %addr, "New WebSocket connection");
                                tokio::spawn(Self::handle_connection(stream, addr));
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Accept error");
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("WebSocket server shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_connection(stream: TcpStream, addr: SocketAddr) {
        match accept_async(stream).await {
            Ok(ws_stream) => {
                let (mut write, mut read) = ws_stream.split();

                // Simple echo/subscription handling
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Handle subscription request
                            if text.starts_with('{') {
                                // JSON subscription request
                                tracing::debug!(remote = %addr, "Subscription request: {}", text);
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            tracing::debug!(remote = %addr, error = %e, "WebSocket error");
                            break;
                        }
                        _ => {}
                    }
                }

                tracing::debug!(remote = %addr, "WebSocket connection closed");
            }
            Err(e) => {
                tracing::warn!(remote = %addr, error = %e, "WebSocket handshake failed");
            }
        }
    }

    /// Broadcast an update to all connected clients.
    pub async fn broadcast(&self, update: Update) -> SinkResult<usize> {
        if *self.state.read() != ServerState::Running {
            return Err(SinkError::NotReady("Server not running".to_string()));
        }

        // Serialize update to JSON
        let json = match serde_json::to_string(&SerializableUpdate::from(&update)) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to serialize update");
                return Ok(0);
            }
        };

        let mut sent = 0;
        for client in self.clients.iter() {
            if client.send(&json).await.is_ok() {
                sent += 1;
            }
        }

        Ok(sent)
    }

    /// Get client count.
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Shutdown the server.
    pub async fn shutdown(&self) -> SinkResult<()> {
        let mut state = self.state.write();
        if *state == ServerState::Stopped || *state == ServerState::ShuttingDown {
            return Ok(());
        }
        *state = ServerState::ShuttingDown;
        drop(state);

        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }

        // Close all clients
        self.clients.clear();

        *self.state.write() = ServerState::Stopped;
        Ok(())
    }
}

/// Serializable wrapper for updates.
#[derive(serde::Serialize)]
struct SerializableUpdate {
    #[serde(rename = "type")]
    update_type: &'static str,
    slot: Option<u64>,
    data: serde_json::Value,
}

impl From<&Update> for SerializableUpdate {
    fn from(update: &Update) -> Self {
        let (update_type, slot, data) = match update {
            Update::Account(a) => (
                "account",
                Some(a.slot),
                serde_json::json!({
                    "pubkey": bs58::encode(&a.pubkey).into_string(),
                    "owner": bs58::encode(&a.owner).into_string(),
                    "lamports": a.lamports,
                    "executable": a.executable,
                    "data_len": a.data.len(),
                }),
            ),
            Update::Transaction(t) => (
                "transaction",
                Some(t.slot),
                serde_json::json!({
                    "signature": bs58::encode(&t.signature).into_string(),
                    "index": t.index,
                    "is_vote": t.is_vote,
                    "data_len": t.transaction_data.len(),
                }),
            ),
            Update::Slot(s) => (
                "slot",
                Some(s.slot),
                serde_json::json!({
                    "parent": s.parent,
                    "status": s.status.as_str(),
                }),
            ),
            Update::Entry(e) => (
                "entry",
                Some(e.slot),
                serde_json::json!({
                    "index": e.index,
                    "num_hashes": e.num_hashes,
                    "hash": bs58::encode(&e.hash).into_string(),
                    "executed_transaction_count": e.executed_transaction_count,
                }),
            ),
            Update::BlockMetadata(b) => (
                "block_metadata",
                Some(b.slot),
                serde_json::json!({
                    "blockhash": bs58::encode(&b.blockhash).into_string(),
                    "block_time": b.block_time,
                    "block_height": b.block_height,
                }),
            ),
        };

        Self {
            update_type,
            slot,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_defaults() {
        let ws_config = WsSinkConfig {
            enabled: true,
            bind_address: "127.0.0.1:10001".to_string(),
            max_clients: 50,
            send_buffer_size: 1000,
            filters: Default::default(),
        };
        let config = WsServerConfig::from(&ws_config);
        assert_eq!(config.max_clients, 50);
    }
}
