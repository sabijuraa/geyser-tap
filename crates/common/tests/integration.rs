//! Integration tests for the shared geyser-tap types.
//!
//! These live in `crates/common/tests/` so cargo actually builds and runs
//! them. They previously sat in a `tests/` directory at the workspace root;
//! because the root manifest is a virtual workspace with no `[package]`, that
//! file belonged to no crate and was never compiled, so none of it had ever
//! executed despite being cited as coverage.

use bytes::Bytes;
use geyser_tap_common::{AccountUpdate, PluginConfig, SinkHealth, Update, UpdateTypeFilter};

/// A config is only valid if at least one sink is enabled.
#[test]
fn test_config_validation() {
    // Valid config with gRPC enabled
    let valid_json = r#"{
        "grpc": {
            "enabled": true,
            "bind_address": "0.0.0.0:10000"
        }
    }"#;

    let config: PluginConfig = serde_json::from_str(valid_json).expect("should deserialize");
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

    let config: PluginConfig = serde_json::from_str(invalid_json).expect("should deserialize");
    assert!(
        config.validate().is_err(),
        "a config with every sink disabled must be rejected"
    );
}

/// A disabled Kafka sink still requires `brokers`, so a half-populated block
/// fails to deserialize. This is the exact shape that used to crash the
/// validator: on_load returned an error whose vtable lived in the library the
/// validator had just unloaded. See SYSTEM_DESIGN.md, "Toolchain and ABI
/// Compatibility".
#[test]
fn test_disabled_kafka_block_still_requires_brokers() {
    let json = r#"{
        "grpc": { "enabled": true, "bind_address": "127.0.0.1:10000" },
        "kafka": { "enabled": false }
    }"#;

    let parsed: Result<PluginConfig, _> = serde_json::from_str(json);
    assert!(
        parsed.is_err(),
        "a kafka block without `brokers` must fail to parse, so the omission \
         is caught at config load rather than surfacing later"
    );
}

/// Update carries its payload and reports size, slot and type name.
#[test]
fn test_update_serialization() {
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

    assert!(update.size_bytes() > 100);
    assert_eq!(update.slot(), Some(12345));
    assert_eq!(update.type_name(), "account");
}

/// Only healthy and degraded sinks accept updates.
#[test]
fn test_sink_health() {
    assert!(SinkHealth::Healthy.can_accept());
    assert!(SinkHealth::Degraded.can_accept());
    assert!(!SinkHealth::Unhealthy.can_accept());
    assert!(!SinkHealth::Shutdown.can_accept());
}

/// Entries are high volume and off by default; everything else is on.
#[test]
fn test_filter_defaults() {
    let filter = UpdateTypeFilter::default();
    assert!(filter.accounts);
    assert!(filter.transactions);
    assert!(filter.slots);
    assert!(!filter.entries);
    assert!(filter.block_metadata);
}
