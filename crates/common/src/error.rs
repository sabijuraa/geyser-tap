//! Error types for geyser-tap.
//!
//! Error handling in a Geyser plugin is critical because:
//! 1. Panics can crash the validator process
//! 2. Errors must be logged but often cannot halt processing
//! 3. FFI boundary requires special handling (no unwinding)

use thiserror::Error;

/// Top-level error type for the geyser-tap plugin ecosystem.
///
/// These errors are designed to be:
/// - Informative for debugging
/// - Safe to log without leaking sensitive data
/// - Recoverable where possible
#[derive(Error, Debug)]
pub enum GeyserTapError {
    /// Configuration error during plugin initialization
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    /// Sink-related error during data transmission
    #[error("sink error: {0}")]
    Sink(#[from] SinkError),

    /// Serialization error when encoding updates
    #[error("serialization error: {0}")]
    Serialization(#[from] SerializationError),

    /// Channel error (typically backpressure-related)
    #[error("channel error: {0}")]
    Channel(String),

    /// Internal error that should not occur in normal operation
    #[error("internal error: {0}")]
    Internal(String),
}

/// Configuration-specific errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Failed to read configuration file
    #[error("failed to read config file at '{path}': {source}")]
    FileRead {
        /// Path to the configuration file
        path: String,
        /// Underlying I/O error
        #[source]
        source: std::io::Error,
    },

    /// Failed to parse configuration
    #[error("failed to parse config: {0}")]
    Parse(String),

    /// Invalid configuration value
    #[error("invalid config value for '{field}': {message}")]
    InvalidValue {
        /// Configuration field name
        field: String,
        /// Description of why the value is invalid
        message: String,
    },

    /// Missing required configuration field
    #[error("missing required config field: {0}")]
    MissingField(String),
}

/// Sink-specific errors.
#[derive(Error, Debug)]
pub enum SinkError {
    /// Failed to connect to downstream sink
    #[error("connection failed to {endpoint}: {message}")]
    Connection {
        /// Endpoint that failed to connect
        endpoint: String,
        /// Error message
        message: String,
    },

    /// Failed to send data to sink
    #[error("send failed: {0}")]
    Send(String),

    /// Sink is not ready to receive data
    #[error("sink not ready: {0}")]
    NotReady(String),

    /// Sink has been shut down
    #[error("sink has been shut down")]
    Shutdown,

    /// Backpressure limit reached
    #[error("backpressure limit reached, dropped {count} updates")]
    Backpressure {
        /// Number of updates dropped
        count: u64,
    },

    /// gRPC-specific error
    #[error("gRPC error: {0}")]
    Grpc(String),

    /// Kafka-specific error
    #[error("Kafka error: {0}")]
    Kafka(String),

    /// All sinks in a fanout failed
    #[error("all sinks failed")]
    AllSinksFailed,
}

/// Serialization-specific errors.
#[derive(Error, Debug)]
pub enum SerializationError {
    /// Failed to encode data
    #[error("encoding failed: {0}")]
    Encode(String),

    /// Failed to decode data
    #[error("decoding failed: {0}")]
    Decode(String),

    /// Data exceeds maximum allowed size
    #[error("data size {size} exceeds maximum {max}")]
    SizeExceeded {
        /// Actual size in bytes
        size: usize,
        /// Maximum allowed size in bytes
        max: usize,
    },
}

/// Result type alias for geyser-tap operations.
pub type Result<T> = std::result::Result<T, GeyserTapError>;

/// Result type alias for sink operations.
pub type SinkResult<T> = std::result::Result<T, SinkError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GeyserTapError>();
        assert_send_sync::<SinkError>();
        assert_send_sync::<ConfigError>();
    }
}
