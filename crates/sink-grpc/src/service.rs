//! gRPC service implementation.
//!
//! This module implements the `GeyserStream` gRPC service defined in
//! the protobuf schema. It handles:
//!
//! - Subscription request validation
//! - Filter construction
//! - Streaming updates to clients
//! - Connection lifecycle

use crate::broadcaster::{Broadcaster, ClientFilter};
use crate::client::{FilterBuilder, SessionId};
use geyser_tap_common::Update;
use geyser_tap_proto::geyser;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

/// Result type for streaming responses.
pub type StreamResult<T> = Result<Response<T>, Status>;

/// The stream type returned by Subscribe.
pub type UpdateStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<geyser::StreamUpdate, Status>> + Send>>;

/// Implementation of the GeyserStream gRPC service.
pub struct GeyserService {
    /// The broadcaster for distributing updates
    broadcaster: Arc<Broadcaster>,
    /// Counter for generating session IDs
    next_session_id: AtomicU64,
    /// Maximum clients allowed
    max_clients: usize,
}

impl GeyserService {
    /// Create a new gRPC service instance.
    pub fn new(broadcaster: Arc<Broadcaster>, max_clients: usize) -> Self {
        Self {
            broadcaster,
            next_session_id: AtomicU64::new(1),
            max_clients,
        }
    }

    /// Handle a subscription request.
    ///
    /// This method:
    /// 1. Validates the request
    /// 2. Builds a filter from the request
    /// 3. Registers the client with the broadcaster
    /// 4. Returns a stream of updates
    pub async fn subscribe(
        &self,
        request: Request<geyser::SubscribeRequest>,
    ) -> StreamResult<UpdateStream> {
        // Check client limit
        if self.broadcaster.client_count() >= self.max_clients {
            return Err(Status::resource_exhausted("Maximum client limit reached"));
        }

        let remote_addr = request
            .remote_addr()
            .unwrap_or_else(|| "unknown".parse().unwrap());

        let req = request.into_inner();

        // Build filter from request
        let filter = self.build_filter(&req)?;

        // Register with broadcaster
        let (client_id, rx) = self.broadcaster.register(filter.clone());
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            session_id,
            client_id,
            remote_addr = %remote_addr,
            "Client subscribed"
        );

        // Convert to stream of protobuf messages
        let stream = self.make_update_stream(rx, session_id);

        Ok(Response::new(Box::pin(stream) as UpdateStream))
    }

    /// Build a client filter from a subscription request.
    #[allow(clippy::result_large_err)]
    fn build_filter(&self, req: &geyser::SubscribeRequest) -> Result<ClientFilter, Status> {
        let mut builder = FilterBuilder::new();

        // Account filter
        if let Some(ref accounts) = req.accounts {
            builder = builder.with_accounts(true);

            if !accounts.owners.is_empty() {
                let owners: Result<Vec<[u8; 32]>, _> = accounts
                    .owners
                    .iter()
                    .map(|b| {
                        b.as_ref()
                            .try_into()
                            .map_err(|_| Status::invalid_argument("Invalid owner pubkey"))
                    })
                    .collect();
                builder = builder.with_account_owners(owners?);
            }

            if !accounts.pubkeys.is_empty() {
                let pubkeys: Result<Vec<[u8; 32]>, _> = accounts
                    .pubkeys
                    .iter()
                    .map(|b| {
                        b.as_ref()
                            .try_into()
                            .map_err(|_| Status::invalid_argument("Invalid account pubkey"))
                    })
                    .collect();
                builder = builder.with_account_pubkeys(pubkeys?);
            }
        }

        // Transaction filter
        if let Some(ref txns) = req.transactions {
            builder = builder
                .with_transactions(true)
                .with_votes(txns.include_votes);
        }

        // Simple boolean filters
        builder = builder
            .with_slots(req.slots)
            .with_entries(req.entries)
            .with_block_metadata(req.block_metadata);

        Ok(builder.build())
    }

    /// Create a stream that converts internal updates to protobuf messages.
    fn make_update_stream(
        &self,
        rx: mpsc::Receiver<Arc<Update>>,
        _session_id: SessionId,
    ) -> impl tokio_stream::Stream<Item = Result<geyser::StreamUpdate, Status>> {
        let sequence = AtomicU64::new(0);

        ReceiverStream::new(rx).map(move |update| {
            let seq = sequence.fetch_add(1, Ordering::Relaxed);
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);

            let proto_update = geyser_tap_proto::convert::to_stream_update(&update, seq, timestamp_ns);
            Ok(proto_update)
        })
    }

    /// Handle a ping request (health check).
    pub async fn ping(
        &self,
        request: Request<geyser::PingRequest>,
    ) -> StreamResult<geyser::PingResponse> {
        let req = request.into_inner();
        let server_timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Ok(Response::new(geyser::PingResponse {
            client_timestamp_ns: req.timestamp_ns,
            server_timestamp_ns,
            current_slot: 0, // Would be filled from actual state
        }))
    }
}

/// Tonic gRPC server implementation.
///
/// This wraps GeyserService with the tonic Server trait for the streaming RPC.
#[derive(Clone)]
pub struct GeyserStreamServer {
    inner: Arc<GeyserService>,
}

impl GeyserStreamServer {
    /// Create a new server wrapping the given service.
    pub fn new(service: GeyserService) -> Self {
        Self {
            inner: Arc::new(service),
        }
    }

    /// Access the inner service.
    pub fn inner(&self) -> &GeyserService {
        &self.inner
    }
}

/// Service trait for the geyser streaming RPC.
///
/// Clients call `Subscribe` to receive a stream of validator updates.
#[tonic::async_trait]
pub trait GeyserStream: Send + Sync + 'static {
    /// Server-streaming response type for Subscribe.
    type SubscribeStream: tokio_stream::Stream<Item = Result<geyser_tap_proto::geyser::StreamUpdate, Status>>
        + Send
        + 'static;

    /// Subscribe to validator updates with optional filtering.
    async fn subscribe(
        &self,
        request: Request<geyser_tap_proto::geyser::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status>;

    /// Ping the server for health checking.
    async fn ping(
        &self,
        request: Request<geyser_tap_proto::geyser::PingRequest>,
    ) -> Result<Response<geyser_tap_proto::geyser::PingResponse>, Status>;
}

#[tonic::async_trait]
impl GeyserStream for GeyserStreamServer {
    type SubscribeStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<geyser_tap_proto::geyser::StreamUpdate, Status>> + Send>
    >;

    async fn subscribe(
        &self,
        request: Request<geyser_tap_proto::geyser::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.inner.subscribe(request).await
    }

    async fn ping(
        &self,
        request: Request<geyser_tap_proto::geyser::PingRequest>,
    ) -> Result<Response<geyser_tap_proto::geyser::PingResponse>, Status> {
        self.inner.ping(request).await
    }
}

/// The service name for gRPC reflection/routing.
pub const GEYSER_STREAM_SERVICE_NAME: &str = "geyser.GeyserStream";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_creation() {
        let broadcaster = Arc::new(Broadcaster::new(1000));
        let service = GeyserService::new(broadcaster, 100);
        assert_eq!(service.max_clients, 100);
    }
}
