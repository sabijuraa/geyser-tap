//! FFI-safe Geyser plugin implementation.
//!
//! This module implements the `GeyserPlugin` trait with careful attention to
//! FFI safety. Every callback from the validator is wrapped in `catch_unwind`
//! to prevent panics from propagating across the FFI boundary.
//!
//! ## CStr Handling
//!
//! The validator passes configuration paths as C strings. We handle these
//! carefully:
//! 1. Check for null pointers
//! 2. Use `CStr::from_ptr` with clear lifetime bounds
//! 3. Handle invalid UTF-8 gracefully
//!
//! ## Error Handling
//!
//! Plugin methods return `GeyserPluginError` which the validator logs.
//! We never panic - errors are either:
//! 1. Returned as `GeyserPluginError` for validator to handle
//! 2. Logged and swallowed to allow continued operation
//!
//! ## Panic Safety
//!
//! All hot-path methods (`update_account`, `notify_transaction`, etc.) are
//! wrapped in `catch_unwind`. If a panic occurs:
//! 1. It's caught before reaching FFI boundary
//! 2. An error is logged
//! 3. The method returns gracefully
//! 4. The validator continues operating (degraded mode)

use crate::runtime::PluginRuntime;
use crate::state::PluginState;
use geyser_tap_common::{
    AccountUpdate, BlockMetadataUpdate, EntryUpdate,
    PluginConfig, Sink, SlotStatus, SlotUpdate, TransactionUpdate, Update,
};
use geyser_tap_sink_grpc::GrpcSink;
use geyser_tap_sink_kafka::KafkaSink;
use geyser_tap_sink_ws::WsSink;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
    SlotStatus as SolanaSlotStatus,
};
use solana_sdk::clock::Slot;
use std::ffi::CStr;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

/// The main Geyser plugin implementation.
///
/// This struct is loaded by the validator and receives callbacks for
/// all account, transaction, slot, and entry updates.
pub struct GeyserTapPlugin {
    /// Plugin state (configuration, sinks, etc.)
    /// Wrapped in RwLock for thread-safe access from validator callbacks
    state: RwLock<Option<Arc<PluginState>>>,

    /// Async runtime for sink operations
    runtime: RwLock<Option<PluginRuntime>>,

    /// Plugin name for logging
    name: &'static str,
}

impl GeyserTapPlugin {
    /// Create a new uninitialized plugin.
    ///
    /// The plugin is not usable until `on_load` is called with configuration.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(None),
            runtime: RwLock::new(None),
            name: "geyser-tap",
        }
    }

    /// Execute a closure with panic catching.
    ///
    /// If the closure panics, the panic is caught and an error is returned.
    /// This prevents panics from crossing the FFI boundary.
    fn catch_panic<F, R>(&self, context: &str, f: F) -> PluginResult<R>
    where
        F: FnOnce() -> PluginResult<R> + panic::UnwindSafe,
    {
        match panic::catch_unwind(f) {
            Ok(result) => result,
            Err(panic_info) => {
                // Extract panic message if possible
                let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };

                tracing::error!(
                    context,
                    panic_message = %msg,
                    "Panic caught in Geyser plugin"
                );

                Err(GeyserPluginError::Custom(Box::new(std::io::Error::other(
                    format!("panic in {context}: {msg}"),
                ))))
            }
        }
    }

    /// Convert C string to Rust path.
    ///
    /// # Safety
    ///
    /// Caller must ensure `ptr` is a valid null-terminated C string.
    #[allow(dead_code)]
    unsafe fn cstr_to_path(ptr: *const i8) -> PluginResult<std::path::PathBuf> {
        if ptr.is_null() {
            return Err(GeyserPluginError::Custom(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "null config path",
            ))));
        }

        // SAFETY: Caller guarantees ptr is valid
        let c_str = unsafe { CStr::from_ptr(ptr) };
        let path_str = c_str.to_str().map_err(|e| {
            GeyserPluginError::Custom(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid UTF-8 in config path: {e}"),
            )))
        })?;

        Ok(std::path::PathBuf::from(path_str))
    }

    /// Create sinks based on configuration.
    fn create_sinks(config: &PluginConfig) -> Result<Vec<Box<dyn Sink>>, geyser_tap_common::GeyserTapError> {
        let mut sinks: Vec<Box<dyn Sink>> = Vec::new();

        // Create gRPC sink if enabled
        if let Some(ref grpc_config) = config.grpc {
            if grpc_config.enabled {
                tracing::info!(
                    bind_address = %grpc_config.bind_address,
                    "Creating gRPC sink"
                );
                let sink = GrpcSink::new(grpc_config.clone());
                sinks.push(Box::new(sink));
            }
        }

        // Create Kafka sink if enabled
        if let Some(ref kafka_config) = config.kafka {
            if kafka_config.enabled {
                tracing::info!(
                    brokers = %kafka_config.brokers,
                    topic = %kafka_config.topic,
                    "Creating Kafka sink"
                );
                let sink = KafkaSink::new(kafka_config.clone())
                    .map_err(geyser_tap_common::GeyserTapError::Sink)?;
                sinks.push(Box::new(sink));
            }
        }

        // Create WebSocket sink if enabled
        if let Some(ref ws_config) = config.ws {
            if ws_config.enabled {
                tracing::info!(
                    bind_address = %ws_config.bind_address,
                    "Creating WebSocket sink"
                );
                let sink = WsSink::new(ws_config.clone());
                sinks.push(Box::new(sink));
            }
        }

        Ok(sinks)
    }
}

impl Default for GeyserTapPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GeyserTapPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeyserTapPlugin")
            .field("name", &self.name)
            .field("initialized", &self.state.read().is_some())
            .finish()
    }
}

impl GeyserPlugin for GeyserTapPlugin {
    fn name(&self) -> &'static str {
        self.name
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        // Initialize logging
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        tracing::info!(config_file, "Loading Geyser plugin");

        // Load configuration
        let config_path = std::path::Path::new(config_file);
        let config = PluginConfig::load_from_file(config_path).map_err(|e| {
            GeyserPluginError::Custom(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("failed to load config: {e}"),
            )))
        })?;

        tracing::info!(?config, "Configuration loaded");

        // Create runtime
        let mut runtime = PluginRuntime::new(&config).map_err(|e| {
            GeyserPluginError::Custom(Box::new(std::io::Error::other(
                format!("failed to create runtime: {e}"),
            )))
        })?;

        // Create sinks based on configuration
        let sinks = Self::create_sinks(&config).map_err(|e| {
            GeyserPluginError::Custom(Box::new(std::io::Error::other(
                format!("failed to create sinks: {e}"),
            )))
        })?;

        // Start runtime with sinks
        runtime.start(sinks).map_err(|e| {
            GeyserPluginError::Custom(Box::new(std::io::Error::other(
                format!("failed to start runtime: {e}"),
            )))
        })?;

        let state = PluginState::new(&config, runtime.sender()).map_err(|e| {
            GeyserPluginError::Custom(Box::new(std::io::Error::other(
                format!("failed to create state: {e}"),
            )))
        })?;

        // Store state
        *self.state.write() = Some(Arc::new(state));
        *self.runtime.write() = Some(runtime);

        // Set metrics
        geyser_tap_common::metrics::CHANNEL_CAPACITY.set(config.plugin.channel_capacity as f64);

        tracing::info!("Geyser plugin loaded successfully");
        Ok(())
    }

    fn on_unload(&mut self) {
        tracing::info!("Unloading Geyser plugin");

        // Shutdown runtime (will flush sinks)
        if let Some(runtime) = self.runtime.write().take() {
            if let Err(e) = runtime.shutdown() {
                tracing::error!(error = %e, "Error during runtime shutdown");
            }
        }

        // Clear state
        *self.state.write() = None;

        tracing::info!("Geyser plugin unloaded");
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions<'_>,
        slot: Slot,
        is_startup: bool,
    ) -> PluginResult<()> {
        // Skip startup replay if configured
        if is_startup {
            return Ok(());
        }

        self.catch_panic("update_account", AssertUnwindSafe(|| {
            let state = match self.state.read().as_ref() {
                Some(s) => Arc::clone(s),
                None => return Ok(()), // Plugin not initialized
            };

            // Check filter
            if !state.filter().update_types.accounts {
                return Ok(());
            }

            // Extract account info (handle different versions)
            let info = match account {
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    // Older version not supported
                    return Ok(());
                }
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    return Ok(());
                }
            };

            // Convert to our update type
            let update = AccountUpdate {
                pubkey: info.pubkey.try_into().unwrap_or([0u8; 32]),
                data: Bytes::copy_from_slice(info.data),
                slot,
                owner: info.owner.try_into().unwrap_or([0u8; 32]),
                lamports: info.lamports,
                rent_epoch: info.rent_epoch,
                executable: info.executable,
                write_version: info.write_version,
                txn_signature: info.txn.map(|t| {
                    t.signature().as_ref().try_into().unwrap_or([0u8; 64])
                }),
            };

            // Send to runtime
            state.send(Update::Account(update));
            geyser_tap_common::metrics::record_update_received("account");

            Ok(())
        }))
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: Slot,
    ) -> PluginResult<()> {
        self.catch_panic("notify_transaction", AssertUnwindSafe(|| {
            let state = match self.state.read().as_ref() {
                Some(s) => Arc::clone(s),
                None => return Ok(()),
            };

            if !state.filter().update_types.transactions {
                return Ok(());
            }

            let info = match transaction {
                ReplicaTransactionInfoVersions::V0_0_2(info) => info,
                ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                    return Ok(());
                }
            };

            // Check vote filter
            if info.is_vote && !state.filter().include_votes {
                return Ok(());
            }

            // Serialize the transaction to VersionedTransaction for downstream consumers.
            // SanitizedTransaction can be converted to VersionedTransaction which implements Serialize.
            let tx_data = match info.transaction.to_versioned_transaction().into_legacy_transaction() {
                Some(legacy_tx) => {
                    match bincode::serialize(&legacy_tx) {
                        Ok(bytes) => Bytes::from(bytes),
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to serialize legacy transaction");
                            Bytes::new()
                        }
                    }
                }
                None => {
                    // For versioned transactions, serialize the message hash as a fallback
                    let hash = info.transaction.message_hash();
                    Bytes::copy_from_slice(hash.as_ref())
                }
            };

            // Transaction status metadata - use None for now as the types don't support serde
            let meta_bytes: Option<Bytes> = None;

            let update = TransactionUpdate {
                signature: info.signature.as_ref().try_into().unwrap_or([0u8; 64]),
                transaction_data: tx_data,
                slot,
                index: info.index as u64,
                is_vote: info.is_vote,
                meta: meta_bytes,
            };

            state.send(Update::Transaction(update));
            geyser_tap_common::metrics::record_update_received("transaction");

            Ok(())
        }))
    }

    fn update_slot_status(
        &self,
        slot: Slot,
        parent: Option<Slot>,
        status: SolanaSlotStatus,
    ) -> PluginResult<()> {
        self.catch_panic("update_slot_status", AssertUnwindSafe(|| {
            let state = match self.state.read().as_ref() {
                Some(s) => Arc::clone(s),
                None => return Ok(()),
            };

            if !state.filter().update_types.slots {
                return Ok(());
            }

            let status = match status {
                SolanaSlotStatus::Processed => SlotStatus::Processed,
                SolanaSlotStatus::Rooted => SlotStatus::Rooted,
                SolanaSlotStatus::Confirmed => SlotStatus::Confirmed,
            };

            let update = SlotUpdate {
                slot,
                parent,
                status,
            };

            state.send(Update::Slot(update));
            geyser_tap_common::metrics::record_update_received("slot");
            geyser_tap_common::metrics::set_current_slot(slot);

            Ok(())
        }))
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions<'_>) -> PluginResult<()> {
        self.catch_panic("notify_entry", AssertUnwindSafe(|| {
            let state = match self.state.read().as_ref() {
                Some(s) => Arc::clone(s),
                None => return Ok(()),
            };

            if !state.filter().update_types.entries {
                return Ok(());
            }

            let info = match entry {
                ReplicaEntryInfoVersions::V0_0_2(info) => info,
                ReplicaEntryInfoVersions::V0_0_1(_) => return Ok(()),
            };

            // Entry data is the hash - transactions are sent separately via notify_transaction.
            // We include the hash bytes as the entry data for downstream consumers.
            let entry_data = Bytes::copy_from_slice(info.hash);

            let update = EntryUpdate {
                slot: info.slot,
                index: info.index as u64,
                num_hashes: info.num_hashes,
                hash: info.hash.try_into().unwrap_or([0u8; 32]),
                entry_data,
                executed_transaction_count: info.executed_transaction_count,
            };

            state.send(Update::Entry(update));
            geyser_tap_common::metrics::record_update_received("entry");

            Ok(())
        }))
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        self.catch_panic("notify_block_metadata", AssertUnwindSafe(|| {
            let state = match self.state.read().as_ref() {
                Some(s) => Arc::clone(s),
                None => return Ok(()),
            };

            if !state.filter().update_types.block_metadata {
                return Ok(());
            }

            let info = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_3(info) => info,
                ReplicaBlockInfoVersions::V0_0_2(_info) => {
                    return Ok(());
                }
                ReplicaBlockInfoVersions::V0_0_1(_info) => {
                    return Ok(());
                }
            };

            let blockhash_bytes: [u8; 32] = bs58::decode(info.blockhash)
                .into_vec()
                .ok()
                .and_then(|v| v.try_into().ok())
                .unwrap_or([0u8; 32]);

            let update = BlockMetadataUpdate {
                slot: info.slot,
                blockhash: blockhash_bytes,
                block_time: info.block_time,
                block_height: info.block_height,
                rewards: None,
            };

            state.send(Update::BlockMetadata(update));
            geyser_tap_common::metrics::record_update_received("block_metadata");

            Ok(())
        }))
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.state
            .read()
            .as_ref()
            .map(|s| s.filter().update_types.accounts)
            .unwrap_or(false)
    }

    fn transaction_notifications_enabled(&self) -> bool {
        self.state
            .read()
            .as_ref()
            .map(|s| s.filter().update_types.transactions)
            .unwrap_or(false)
    }

    fn entry_notifications_enabled(&self) -> bool {
        self.state
            .read()
            .as_ref()
            .map(|s| s.filter().update_types.entries)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_can_be_created() {
        let plugin = GeyserTapPlugin::new();
        assert_eq!(plugin.name(), "geyser-tap");
    }

    #[test]
    fn plugin_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GeyserTapPlugin>();
    }
}
