//! Integration tests for geyser-tap.
//!
//! These tests verify the plugin works correctly with a real Solana test-validator.
//! They require `solana-test-validator` to be available in PATH.

use std::process::{Child, Command};
use std::time::Duration;

/// Timeout for test operations
const TIMEOUT: Duration = Duration::from_secs(30);

/// Helper to start a test validator with our plugin loaded.
struct TestValidator {
    child: Option<Child>,
    config_path: std::path::PathBuf,
}

impl TestValidator {
    /// Start a new test validator with the geyser plugin.
    fn start(plugin_config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_dir = std::env::temp_dir().join("geyser-tap-test");
        std::fs::create_dir_all(&config_dir)?;

        let config_path = config_dir.join("geyser-config.json");
        std::fs::write(&config_path, plugin_config)?;

        // Check if solana-test-validator is available
        let status = Command::new("solana-test-validator")
            .arg("--version")
            .output();

        if status.is_err() {
            return Err("solana-test-validator not found in PATH".into());
        }

        // Start the validator with plugin
        let child = Command::new("solana-test-validator")
            .arg("--geyser-plugin-config")
            .arg(&config_path)
            .arg("--reset")
            .arg("--quiet")
            .spawn()?;

        // Wait for validator to start
        std::thread::sleep(Duration::from_secs(5));

        Ok(Self {
            child: Some(child),
            config_path,
        })
    }

    /// Stop the test validator.
    fn stop(&mut self) {
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}

impl Drop for TestValidator {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the plugin library can be loaded.
    #[test]
    #[ignore] // Requires solana-test-validator
    fn test_plugin_loads() {
        let config = r#"{
            "libpath": "./target/release/libgeyser_tap_plugin.so",
            "grpc": {
                "enabled": true,
                "bind_address": "127.0.0.1:10000"
            },
            "plugin": {
                "channel_capacity": 1000
            }
        }"#;

        let validator = TestValidator::start(config);
        assert!(validator.is_ok(), "Failed to start validator: {:?}", validator.err());
    }

    /// Test configuration validation.
    #[test]
    fn test_config_validation() {
        use geyser_tap_common::PluginConfig;

        // Valid config with gRPC enabled
        let valid_json = r#"{
            "grpc": {
                "enabled": true,
                "bind_address": "0.0.0.0:10000"
            }
        }"#;

        let config: Result<PluginConfig, _> = serde_json::from_str(valid_json);
        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(config.validate().is_ok());

        // Invalid config - no sinks enabled
        let invalid_json = r#"{
            "grpc": {
                "enabled": false
            },
            "kafka": {
                "enabled": false,
                "brokers": "localhost:9092",
                "topic": "test"
            }
        }"#;

        let config: Result<PluginConfig, _> = serde_json::from_str(invalid_json);
        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(config.validate().is_err());
    }

    /// Test update type serialization.
    #[test]
    fn test_update_serialization() {
        use bytes::Bytes;
        use geyser_tap_common::{AccountUpdate, Update};

        let account = AccountUpdate {
            pubkey: [1u8; 32],
            data: Bytes::from(vec![0u8; 100]),
            slot: 12345,
            owner: [2u8; 32],
            lamports: 1_000_000,
            rent_epoch: 0,
            executable: false,
            write_version: 1,
            txn_signature: None,
        };

        let update = Update::Account(account);

        // Verify size calculation
        assert!(update.size_bytes() > 100);
        assert_eq!(update.slot(), Some(12345));
        assert_eq!(update.type_name(), "account");
    }

    /// Test sink health status.
    #[test]
    fn test_sink_health() {
        use geyser_tap_common::SinkHealth;

        assert!(SinkHealth::Healthy.can_accept());
        assert!(SinkHealth::Degraded.can_accept());
        assert!(!SinkHealth::Unhealthy.can_accept());
        assert!(!SinkHealth::Shutdown.can_accept());
    }

    /// Test filter configuration defaults.
    #[test]
    fn test_filter_defaults() {
        use geyser_tap_common::UpdateTypeFilter;

        let filter = UpdateTypeFilter::default();
        assert!(filter.accounts);
        assert!(filter.transactions);
        assert!(filter.slots);
        assert!(!filter.entries); // High volume, off by default
        assert!(filter.block_metadata);
    }
}
