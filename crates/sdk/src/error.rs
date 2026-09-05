//! SDK error types.

use thiserror::Error;

/// SDK-specific errors.
#[derive(Error, Debug)]
pub enum SdkError {
    /// Failed to connect to the server.
    #[error("connection failed: {0}")]
    Connection(String),

    /// gRPC transport error.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error.
    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    /// Stream ended unexpectedly.
    #[error("stream ended")]
    StreamEnded,

    /// Invalid subscription configuration.
    #[error("invalid subscription: {0}")]
    InvalidSubscription(String),

    /// Decoding error.
    #[error("decode error: {0}")]
    Decode(String),
}

/// Result type for SDK operations.
pub type SdkResult<T> = Result<T, SdkError>;
