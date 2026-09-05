//! gRPC server implementation.
//!
//! This module provides the gRPC server that accepts client connections
//! and streams updates to them.

use crate::broadcaster::Broadcaster;
use crate::service::{GeyserService, GeyserStreamServer};
use geyser_tap_common::{GrpcSinkConfig, SinkError, SinkResult, Update};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;

/// gRPC server configuration.
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Address to bind to
    pub bind_address: SocketAddr,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Per-client send buffer size
    pub send_buffer_size: usize,
    /// TLS configuration
    pub tls: Option<TlsConfig>,
}

impl From<&GrpcSinkConfig> for GrpcServerConfig {
    fn from(config: &GrpcSinkConfig) -> Self {
        Self {
            bind_address: config
                .bind_address
                .parse()
                .unwrap_or_else(|_| "0.0.0.0:10000".parse().unwrap()),
            max_connections: config.max_connections,
            send_buffer_size: config.send_buffer_size,
            tls: None, // TLS config conversion would go here
        }
    }
}

/// TLS configuration for the gRPC server.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Server certificate (PEM)
    pub cert: Vec<u8>,
    /// Server private key (PEM)
    pub key: Vec<u8>,
    /// Optional CA certificate for client verification
    pub ca_cert: Option<Vec<u8>>,
}

/// State of the gRPC server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    /// Server is not yet started
    NotStarted,
    /// Server is starting
    Starting,
    /// Server is running and accepting connections
    Running,
    /// Server is shutting down
    ShuttingDown,
    /// Server has stopped
    Stopped,
}

/// gRPC server for streaming updates to clients.
pub struct GrpcServer {
    /// Server configuration
    config: GrpcServerConfig,
    /// Update broadcaster
    broadcaster: Arc<Broadcaster>,
    /// Current server state
    state: RwLock<ServerState>,
    /// Shutdown signal sender
    shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,
}

impl GrpcServer {
    /// Create a new gRPC server with the given configuration.
    pub fn new(config: GrpcServerConfig) -> Self {
        Self {
            broadcaster: Arc::new(Broadcaster::new(config.send_buffer_size)),
            config,
            state: RwLock::new(ServerState::NotStarted),
            shutdown_tx: RwLock::new(None),
        }
    }

    /// Start the server.
    ///
    /// Binds the listener before returning, so a bind failure (port in use,
    /// permission denied) surfaces here rather than being swallowed inside a
    /// detached task. The serving loop itself is spawned and runs until
    /// `shutdown()` is called.
    pub async fn start(&self) -> SinkResult<()> {
        {
            let state = self.state.read();
            if *state != ServerState::NotStarted {
                return Err(SinkError::NotReady("Server already started".to_string()));
            }
        }
        *self.state.write() = ServerState::Starting;

        let bind_addr = self.config.bind_address;

        // Bind up front so the caller learns about failures synchronously.
        let listener = match TcpListener::bind(bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                *self.state.write() = ServerState::Stopped;
                return Err(SinkError::Connection {
                    endpoint: bind_addr.to_string(),
                    message: format!("failed to bind gRPC listener: {e}"),
                });
            }
        };

        let local_addr = listener.local_addr().unwrap_or(bind_addr);

        let incoming = match TcpIncoming::from_listener(listener, true, None) {
            Ok(i) => i,
            Err(e) => {
                *self.state.write() = ServerState::Stopped;
                return Err(SinkError::Connection {
                    endpoint: bind_addr.to_string(),
                    message: format!("failed to wrap listener for tonic: {e}"),
                });
            }
        };

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.write() = Some(shutdown_tx);

        let service = GeyserService::new(
            Arc::clone(&self.broadcaster),
            self.config.max_connections,
        );
        let svc = GeyserStreamServer::new(service);

        tokio::spawn(async move {
            tracing::info!(address = %local_addr, "gRPC server serving");

            let result = Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, async move {
                    shutdown_rx.await.ok();
                })
                .await;

            match result {
                Ok(()) => tracing::info!("gRPC server stopped cleanly"),
                Err(e) => tracing::error!(error = %e, "gRPC server terminated with error"),
            }
        });

        *self.state.write() = ServerState::Running;
        tracing::info!(address = %local_addr, "gRPC server listening");

        Ok(())
    }

    /// The address the server actually bound to.
    pub fn bind_address(&self) -> SocketAddr {
        self.config.bind_address
    }

    /// Broadcast an update to all connected clients.
    pub async fn broadcast(&self, update: Update) -> SinkResult<usize> {
        if *self.state.read() != ServerState::Running {
            return Err(SinkError::NotReady("Server not running".to_string()));
        }
        self.broadcaster.broadcast(update).await
    }

    /// Get the number of connected clients.
    pub fn client_count(&self) -> usize {
        self.broadcaster.client_count()
    }

    /// Shutdown the server gracefully.
    pub async fn shutdown(&self) -> SinkResult<()> {
        let mut state = self.state.write();
        if *state == ServerState::Stopped || *state == ServerState::ShuttingDown {
            return Ok(());
        }

        *state = ServerState::ShuttingDown;

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }

        *state = ServerState::Stopped;
        Ok(())
    }

    /// Get the current server state.
    pub fn state(&self) -> ServerState {
        *self.state.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_state_lifecycle() {
        let config = GrpcServerConfig {
            bind_address: "127.0.0.1:0".parse().unwrap(),
            max_connections: 10,
            send_buffer_size: 1000,
            tls: None,
        };
        let server = GrpcServer::new(config);
        assert_eq!(server.state(), ServerState::NotStarted);
    }
}
