//! WebSocket/TCP sink for geyser-tap.
//!
//! Streams validator updates to connected WebSocket clients. Each client
//! maintains its own subscription filter and bounded send buffer.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms)]

mod server;
mod client;

pub use server::{WsServer, WsServerConfig};

use geyser_tap_common::{Sink, SinkHealth, SinkResult, SinkStats, Update, WsSinkConfig};
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// WebSocket sink implementation.
///
/// Runs a WebSocket server that accepts client connections. Each client
/// subscribes with filters and receives matching updates.
pub struct WsSink {
    server: Arc<WsServer>,
    health: Arc<RwLock<SinkHealth>>,
    stats: Arc<RwLock<SinkStats>>,
    config: WsSinkConfig,
}

impl WsSink {
    /// Create a new WebSocket sink.
    pub fn new(config: WsSinkConfig) -> Self {
        Self {
            server: Arc::new(WsServer::new(WsServerConfig::from(&config))),
            health: Arc::new(RwLock::new(SinkHealth::Healthy)),
            stats: Arc::new(RwLock::new(SinkStats::default())),
            config,
        }
    }

    /// Start the WebSocket server.
    pub async fn start(&self) -> SinkResult<()> {
        tracing::info!(
            bind_address = %self.config.bind_address,
            "Starting WebSocket server"
        );
        self.server.start().await
    }

    /// Get the number of connected clients.
    pub fn client_count(&self) -> usize {
        self.server.client_count()
    }
}

impl Sink for WsSink {
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            let update_type = update.type_name();
            let size = update.size_bytes();

            let sent_count = self.server.broadcast(update).await?;

            {
                let mut stats = self.stats.write();
                stats.updates_sent += sent_count as u64;
                stats.bytes_sent += (size * sent_count) as u64;
            }

            geyser_tap_common::metrics::record_update_sent("websocket", update_type);
            Ok(())
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("Shutting down WebSocket sink");
            *self.health.write() = SinkHealth::Shutdown;
            self.server.shutdown().await
        })
    }

    fn health(&self) -> SinkHealth {
        *self.health.read()
    }

    fn stats(&self) -> SinkStats {
        self.stats.read().clone()
    }

    fn name(&self) -> &'static str {
        "websocket"
    }
}
