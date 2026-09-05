//! gRPC client implementation.

use crate::error::{SdkError, SdkResult};
use crate::subscription::Subscription;
use crate::types::Update;
use geyser_tap_proto::geyser;
use geyser_tap_proto::geyser::geyser_stream_client::GeyserStreamClient;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

/// Client for connecting to geyser-tap gRPC service.
pub struct GeyserClient {
    inner: GeyserStreamClient<Channel>,
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
        // Note: no per-request `timeout` here. Subscribe is a long-lived
        // server-streaming RPC, and a request timeout would tear the
        // subscription down after it elapsed even while data was flowing.
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| SdkError::Connection(e.to_string()))?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await?;

        Ok(Self {
            inner: GeyserStreamClient::new(channel),
        })
    }

    /// Connect with custom endpoint configuration.
    pub async fn connect_with_endpoint(endpoint: Endpoint) -> SdkResult<Self> {
        let channel = endpoint.connect().await?;
        Ok(Self {
            inner: GeyserStreamClient::new(channel),
        })
    }

    /// Subscribe to updates with the given subscription.
    ///
    /// Returns a stream of updates matching the subscription filters.
    pub async fn subscribe(&mut self, subscription: Subscription) -> SdkResult<UpdateStream> {
        let request = subscription.into_request();
        tracing::debug!("Subscribing with filters: {:?}", request);

        let response = self.inner.subscribe(request).await?;

        Ok(UpdateStream {
            inner: response.into_inner(),
        })
    }

    /// Ping the server for health checking.
    pub async fn ping(&mut self) -> SdkResult<PingResponse> {
        let sent_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let response = self
            .inner
            .ping(geyser::PingRequest {
                timestamp_ns: sent_ns,
            })
            .await?
            .into_inner();

        let received_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Ok(PingResponse {
            client_timestamp_ns: response.client_timestamp_ns,
            server_timestamp_ns: response.server_timestamp_ns,
            current_slot: response.current_slot,
            latency_ns: received_ns.saturating_sub(sent_ns),
        })
    }
}

/// Stream of updates from the server.
pub struct UpdateStream {
    inner: tonic::Streaming<geyser::StreamUpdate>,
}

impl UpdateStream {
    /// Get the next update from the stream.
    ///
    /// Returns `None` when the server closes the stream.
    pub async fn next(&mut self) -> Option<SdkResult<Update>> {
        match self.inner.message().await {
            Ok(Some(proto)) => Some(Update::try_from(proto)),
            Ok(None) => None,
            Err(status) => Some(Err(SdkError::from(status))),
        }
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
