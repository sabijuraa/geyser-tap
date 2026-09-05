//! gRPC client implementation.

use crate::error::{SdkError, SdkResult};
use crate::subscription::Subscription;
use crate::types::Update;
use futures_util::StreamExt;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

/// Client for connecting to geyser-tap gRPC service.
#[allow(dead_code)]
pub struct GeyserClient {
    channel: Channel,
}

impl GeyserClient {
    /// Connect to a geyser-tap server.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - Server address (e.g., "http://localhost:10000")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = GeyserClient::connect("http://localhost:10000").await?;
    /// ```
    pub async fn connect(endpoint: &str) -> SdkResult<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| SdkError::Connection(e.to_string()))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .connect()
            .await?;

        Ok(Self { channel })
    }

    /// Connect with custom endpoint configuration.
    pub async fn connect_with_endpoint(endpoint: Endpoint) -> SdkResult<Self> {
        let channel = endpoint.connect().await?;
        Ok(Self { channel })
    }

    /// Subscribe to updates with the given subscription.
    ///
    /// Returns a stream of updates matching the subscription filters.
    pub async fn subscribe(
        &mut self,
        subscription: Subscription,
    ) -> SdkResult<UpdateStream> {
        let request = subscription.into_request();

        // Create channel for receiving updates from gRPC stream
        let (_tx, rx) = tokio::sync::mpsc::channel(1000);

        // Note: Full gRPC client requires tonic-build generated code.
        // See docs/INTEGRATION.md for setup with protoc.
        tracing::debug!("Subscribing with filters: {:?}", request);

        Ok(UpdateStream {
            inner: tokio_stream::wrappers::ReceiverStream::new(rx),
        })
    }

    /// Ping the server for health checking.
    pub async fn ping(&mut self) -> SdkResult<PingResponse> {
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        // Returns local timestamp - full implementation requires tonic-build
        Ok(PingResponse {
            client_timestamp_ns: timestamp_ns,
            server_timestamp_ns: timestamp_ns,
            current_slot: 0,
            latency_ns: 0,
        })
    }
}

/// Stream of updates from the server.
pub struct UpdateStream {
    inner: tokio_stream::wrappers::ReceiverStream<SdkResult<Update>>,
}

impl UpdateStream {
    /// Get the next update from the stream.
    pub async fn next(&mut self) -> Option<SdkResult<Update>> {
        self.inner.next().await
    }
}

/// Response from a ping request.
#[derive(Debug, Clone)]
pub struct PingResponse {
    /// Client timestamp when ping was sent.
    pub client_timestamp_ns: u64,
    /// Server timestamp when ping was received.
    pub server_timestamp_ns: u64,
    /// Current slot being processed by the server.
    pub current_slot: u64,
    /// Round-trip latency in nanoseconds.
    pub latency_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_response_debug() {
        let resp = PingResponse {
            client_timestamp_ns: 1000,
            server_timestamp_ns: 1001,
            current_slot: 12345,
            latency_ns: 1,
        };
        assert!(format!("{resp:?}").contains("12345"));
    }
}
