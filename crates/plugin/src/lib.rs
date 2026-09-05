//! # geyser-tap-plugin
//!
//! Solana Geyser plugin for streaming validator data to gRPC and Kafka.
//!
//! This crate implements the `solana-geyser-plugin-interface` and is compiled
//! as a shared library (`.so`) that the Solana validator loads at runtime.
//!
//! ## FFI Safety
//!
//! This plugin runs inside the validator process and is loaded via C FFI.
//! Bugs here can crash the validator. Key safety measures:
//!
//! 1. **No panics across FFI**: All public methods wrap in `catch_unwind`
//! 2. **CStr handling**: Careful conversion of C strings from validator
//! 3. **Memory ownership**: Clear boundaries between validator and plugin memory
//! 4. **Graceful degradation**: Errors are logged, not propagated
//!
//! ## Thread Model
//!
//! The validator calls plugin methods from multiple threads:
//! - `on_load`/`on_unload`: Called once from main thread
//! - `update_account`: Called from multiple banking threads
//! - `notify_transaction`: Called from multiple threads
//! - `notify_entry`/`update_slot_status`: Called from replay thread
//!
//! The plugin uses bounded channels to bridge to its async runtime,
//! which handles actual transmission to sinks.
//!
//! ## Backpressure
//!
//! If sinks are slow, the bounded channel fills up. The plugin then
//! drops updates to protect the validator from blocking. Dropped
//! updates are counted in metrics for alerting.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms)]

mod ffi;
mod runtime;
mod state;

pub use ffi::GeyserTapPlugin;

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

/// Export the plugin factory function.
///
/// This is the entry point called by the validator when loading the plugin.
/// The validator expects this exact symbol name.
///
/// # Safety
///
/// This function is called via C FFI. It must:
/// - Never panic (would crash validator)
/// - Return a valid GeyserPlugin trait object
/// - Not leak memory
#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    // Create the plugin and box it
    let plugin = GeyserTapPlugin::new();
    let boxed: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(boxed)
}
