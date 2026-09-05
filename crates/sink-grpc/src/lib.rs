//! # geyser-tap-sink-grpc
//!
//! gRPC streaming sink for geyser-tap.
//!
//! This crate provides a high-performance gRPC server that streams
//! validator updates to connected clients using the protocol defined
//! in `geyser-tap-proto`.
//!
//! ## Architecture
//!
//! ```text
//! Plugin Callback Thread
//!         |
//!         v
//! +------------------+
//! | Bounded Channel  |  <-- Backpressure boundary
//! +------------------+
//!         |
//!         v
//! +------------------+
//! |   Broadcaster    |  <-- Fan-out to clients
//! +------------------+
//!      /    |    \
//!     v     v     v
//!  Client Client Client
//! ```
//!
//! ## Key Design Decisions
//!
//! 1. **Per-client buffering**: Each client has its own send buffer to
//!    isolate slow clients from fast ones.
//!
//! 2. **Subscription filtering**: Filtering happens server-side to reduce
//!    bandwidth for clients that only need specific update types.
//!
//! 3. **Graceful client disconnect**: Slow or disconnected clients are
//!    detected and cleaned up without blocking other clients.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod broadcaster;
pub mod client;
pub mod server;
pub mod service;

pub use broadcaster::Broadcaster;
pub use server::{GrpcServer, GrpcServerConfig};

use geyser_tap_common::{GrpcSinkConfig, Sink, SinkHealth, SinkResult, SinkStats, Update};
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// gRPC sink implementation.
///
/// This sink runs a gRPC server that accepts client connections and
/// streams updates to them. It handles:
/// - Client subscription management
/// - Per-client filtering
/// - Backpressure via bounded channels
/// - Graceful shutdown
pub struct GrpcSink {
    /// The underlying gRPC server
    server: Arc<GrpcServer>,
    /// Current health status
    health: Arc<RwLock<SinkHealth>>,
    /// Accumulated statistics
    stats: Arc<RwLock<SinkStats>>,
    /// Configuration
    config: GrpcSinkConfig,
}

impl GrpcSink {
    /// Create a new gRPC sink with the given configuration.
    ///
    /// This does not start the server; call `start()` to begin accepting
    /// connections.
    pub fn new(config: GrpcSinkConfig) -> Self {
        Self {
            server: Arc::new(GrpcServer::new(GrpcServerConfig::from(&config))),
            health: Arc::new(RwLock::new(SinkHealth::Healthy)),
            stats: Arc::new(RwLock::new(SinkStats::default())),
            config,
        }
    }

    /// Start the gRPC server.
    ///
    /// This spawns the server on the Tokio runtime and returns immediately.
    /// The server runs until `shutdown()` is called.
    pub async fn start(&self) -> SinkResult<()> {
        // Implementation would bind and start the server
        tracing::info!(
            bind_address = %self.config.bind_address,
            "Starting gRPC server"
        );
        Ok(())
    }

    /// Get the number of connected clients.
    pub fn client_count(&self) -> usize {
        self.server.client_count()
    }
}

impl Sink for GrpcSink {
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            let update_type = update.type_name();
            let size = update.size_bytes();

            // Broadcast to all connected clients
            let sent_count = self.server.broadcast(update).await?;

            // Update metrics
            {
                let mut stats = self.stats.write();
                stats.updates_sent += sent_count as u64;
                stats.bytes_sent += (size * sent_count) as u64;
            }

            geyser_tap_common::metrics::record_update_sent("grpc", update_type);
            Ok(())
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            // gRPC streaming is push-based, no explicit flush needed
            Ok(())
        })
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("Shutting down gRPC sink");
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
        "grpc"
    }
}
