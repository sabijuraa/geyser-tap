//! Configuration types for geyser-tap.
//!
//! Configuration is loaded from a JSON file specified in the validator's
//! geyser plugin config. The path is passed to `on_load` as a C string.
//!
//! ## Configuration File Format
//!
//! ```json
//! {
//!   "libpath": "/path/to/libgeyser_tap_plugin.so",
//!   "grpc": {
//!     "enabled": true,
//!     "bind_address": "0.0.0.0:10000"
//!   },
//!   "kafka": {
//!     "enabled": true,
//!     "brokers": "localhost:9092",
//!     "topic": "solana-updates"
//!   }
//! }
//! ```

use crate::error::ConfigError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Top-level plugin configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Path to the shared library (used by validator, not by plugin itself)
    #[serde(default)]
    pub libpath: Option<String>,

    /// gRPC sink configuration
    #[serde(default)]
    pub grpc: Option<GrpcSinkConfig>,

    /// Kafka sink configuration
    #[serde(default)]
    pub kafka: Option<KafkaSinkConfig>,

    /// WebSocket sink configuration
    #[serde(default)]
    pub ws: Option<WsSinkConfig>,

    /// General plugin settings
    #[serde(default)]
    pub plugin: PluginSettings,

    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Metrics configuration
    #[serde(default)]
    pub metrics: MetricsConfig,
}

impl PluginConfig {
    /// Load configuration from a JSON file.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - The file cannot be read
    /// - The JSON is malformed
    /// - Required fields are missing
    pub fn load_from_file(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|e| ConfigError::FileRead {
            path: path.display().to_string(),
            source: e,
        })?;

        let config: Self =
            serde_json::from_str(&contents).map_err(|e| ConfigError::Parse(e.to_string()))?;

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // At least one sink must be enabled
        let grpc_enabled = self.grpc.as_ref().map(|c| c.enabled).unwrap_or(false);
        let kafka_enabled = self.kafka.as_ref().map(|c| c.enabled).unwrap_or(false);
        let ws_enabled = self.ws.as_ref().map(|c| c.enabled).unwrap_or(false);

        if !grpc_enabled && !kafka_enabled && !ws_enabled {
            return Err(ConfigError::InvalidValue {
                field: "grpc/kafka/ws".to_string(),
                message: "at least one sink must be enabled".to_string(),
            });
        }

        // Validate individual sink configs
        if let Some(ref grpc) = self.grpc {
            if grpc.enabled {
                grpc.validate()?;
            }
        }

        if let Some(ref kafka) = self.kafka {
            if kafka.enabled {
                kafka.validate()?;
            }
        }

        if let Some(ref ws) = self.ws {
            if ws.enabled {
                ws.validate()?;
            }
        }

        Ok(())
    }
}

/// gRPC sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcSinkConfig {
    /// Whether the gRPC sink is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Address to bind the gRPC server to
    #[serde(default = "default_grpc_bind")]
    pub bind_address: String,

    /// Maximum number of concurrent client connections
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Send buffer size per connection
    #[serde(default = "default_send_buffer_size")]
    pub send_buffer_size: usize,

    /// Whether to enable TLS
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Filter configuration
    #[serde(default)]
    pub filters: FilterConfig,
}

impl GrpcSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.bind_address.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "grpc.bind_address".to_string(),
                message: "bind address cannot be empty".to_string(),
            });
        }
        Ok(())
    }
}

/// WebSocket sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsSinkConfig {
    /// Whether the WebSocket sink is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Address to bind the WebSocket server to
    #[serde(default = "default_ws_bind")]
    pub bind_address: String,

    /// Maximum concurrent client connections
    #[serde(default = "default_ws_max_clients")]
    pub max_clients: usize,

    /// Send buffer size per connection
    #[serde(default = "default_send_buffer_size")]
    pub send_buffer_size: usize,

    /// Filter configuration
    #[serde(default)]
    pub filters: FilterConfig,
}

impl WsSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.bind_address.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "ws.bind_address".to_string(),
                message: "bind address cannot be empty".to_string(),
            });
        }
        Ok(())
    }
}

/// Kafka sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    /// Whether the Kafka sink is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Comma-separated list of Kafka brokers
    pub brokers: String,

    /// Topic to publish updates to
    pub topic: String,

    /// Optional topic for account updates (if different from main topic)
    #[serde(default)]
    pub account_topic: Option<String>,

    /// Optional topic for transaction updates
    #[serde(default)]
    pub transaction_topic: Option<String>,

    /// Producer configuration
    #[serde(default)]
    pub producer: KafkaProducerConfig,

    /// SASL authentication
    #[serde(default)]
    pub sasl: Option<SaslConfig>,

    /// Filter configuration
    #[serde(default)]
    pub filters: FilterConfig,
}

impl KafkaSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.brokers.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "kafka.brokers".to_string(),
                message: "brokers cannot be empty".to_string(),
            });
        }
        if self.topic.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "kafka.topic".to_string(),
                message: "topic cannot be empty".to_string(),
            });
        }
        Ok(())
    }
}

/// Kafka producer settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaProducerConfig {
    /// Producer acks setting ("0", "1", "all")
    #[serde(default = "default_acks")]
    pub acks: String,

    /// Compression type ("none", "gzip", "snappy", "lz4", "zstd")
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Batch size in bytes
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Linger time in milliseconds
    #[serde(default = "default_linger_ms")]
    pub linger_ms: u64,

    /// Maximum in-flight requests per connection
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: u32,
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            acks: "1".to_string(),
            compression: "zstd".to_string(),
            batch_size: 1_000_000,
            linger_ms: 5,
            max_in_flight: 5,
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to certificate file
    pub cert_path: String,
    /// Path to private key file
    pub key_path: String,
    /// Path to CA certificate for client verification (optional)
    #[serde(default)]
    pub ca_path: Option<String>,
}

/// SASL authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslConfig {
    /// SASL mechanism ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
    pub mechanism: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
}

/// Filter configuration for selecting which updates to process.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilterConfig {
    /// Account owners to include (empty = all)
    #[serde(default)]
    pub account_owners: Vec<String>,

    /// Account pubkeys to include (empty = all)
    #[serde(default)]
    pub account_pubkeys: Vec<String>,

    /// Whether to include vote transactions
    #[serde(default)]
    pub include_votes: bool,

    /// Whether to include failed transactions
    #[serde(default = "default_true")]
    pub include_failed: bool,

    /// Update types to include
    #[serde(default)]
    pub update_types: UpdateTypeFilter,
}

/// Filter for update types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTypeFilter {
    /// Include account updates.
    pub accounts: bool,
    /// Include transaction updates.
    pub transactions: bool,
    /// Include slot status updates.
    pub slots: bool,
    /// Include entry updates (high volume).
    pub entries: bool,
    /// Include block metadata updates.
    pub block_metadata: bool,
}

impl Default for UpdateTypeFilter {
    fn default() -> Self {
        Self {
            accounts: true,
            transactions: true,
            slots: true,
            entries: false, // Entries are high volume, off by default
            block_metadata: true,
        }
    }
}

/// General plugin settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginSettings {
    /// Channel capacity for updates (backpressure threshold)
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Number of worker threads for async processing
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,

    /// Whether to panic on initialization failure
    #[serde(default = "default_true")]
    pub panic_on_init_failure: bool,
}

impl Default for PluginSettings {
    fn default() -> Self {
        Self {
            channel_capacity: 100_000,
            worker_threads: 4,
            panic_on_init_failure: true,
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level ("trace", "debug", "info", "warn", "error")
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Whether to output logs as JSON
    #[serde(default)]
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            json: false,
        }
    }
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Whether to enable metrics
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Address to bind the metrics HTTP server to
    #[serde(default = "default_metrics_bind")]
    pub bind_address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: "127.0.0.1:9090".to_string(),
        }
    }
}

// Default value functions
fn default_grpc_bind() -> String {
    "0.0.0.0:10000".to_string()
}
fn default_max_connections() -> usize {
    100
}
fn default_send_buffer_size() -> usize {
    65536
}
fn default_acks() -> String {
    "1".to_string()
}
fn default_compression() -> String {
    "zstd".to_string()
}
fn default_batch_size() -> usize {
    1_000_000
}
fn default_linger_ms() -> u64 {
    5
}
fn default_max_in_flight() -> u32 {
    5
}
fn default_true() -> bool {
    true
}
fn default_channel_capacity() -> usize {
    100_000
}
fn default_worker_threads() -> usize {
    4
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_metrics_bind() -> String {
    "127.0.0.1:9090".to_string()
}
fn default_ws_bind() -> String {
    "0.0.0.0:10001".to_string()
}
fn default_ws_max_clients() -> usize {
    100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let settings = PluginSettings::default();
        assert_eq!(settings.channel_capacity, 100_000);
        assert_eq!(settings.worker_threads, 4);
    }

    #[test]
    fn update_type_filter_defaults() {
        let filter = UpdateTypeFilter::default();
        assert!(filter.accounts);
        assert!(filter.transactions);
        assert!(filter.slots);
        assert!(!filter.entries); // Off by default
        assert!(filter.block_metadata);
    }
}
