//! Plugin async runtime.
//!
//! The plugin callbacks run on validator threads (sync). This module
//! provides the bridge to an async runtime for sink operations.
//!
//! ## Architecture
//!
//! ```text
//! Validator Callback Threads                  Plugin Runtime
//! ┌─────────────────────────┐               ┌─────────────────┐
//! │  update_account()       │               │                 │
//! │  notify_transaction()   │──> Channel ──>│  Sink Worker    │
//! │  update_slot_status()   │               │                 │
//! └─────────────────────────┘               └─────────────────┘
//!                                                   │
//!                                                   v
//!                                           ┌─────────────────┐
//!                                           │  gRPC / Kafka   │
//!                                           └─────────────────┘
//! ```
//!
//! ## Backpressure
//!
//! The channel between validator threads and the runtime is bounded.
//! When full, the plugin drops updates to protect the validator.

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use geyser_tap_common::{EnvelopedUpdate, GeyserTapError, PluginConfig, Sink, Update};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tokio::runtime::Runtime;

/// Handle for sending updates from validator threads.
#[derive(Clone)]
pub struct UpdateSender {
    /// The bounded channel sender
    sender: Sender<EnvelopedUpdate>,
    /// Sequence counter for updates
    sequence: Arc<AtomicU64>,
}

impl UpdateSender {
    /// Send an update, dropping if the channel is full.
    ///
    /// Returns `true` if the update was sent, `false` if dropped.
    pub fn send(&self, update: Update) -> bool {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let envelope = EnvelopedUpdate::new(update, seq);

        match self.sender.try_send(envelope) {
            Ok(()) => {
                geyser_tap_common::metrics::set_channel_depth(self.sender.len());
                true
            }
            Err(TrySendError::Full(_)) => {
                // Channel is full - drop to protect validator
                geyser_tap_common::metrics::record_update_dropped("channel");
                tracing::warn!(
                    channel_depth = self.sender.len(),
                    "Channel full, dropping update"
                );
                false
            }
            Err(TrySendError::Disconnected(_)) => {
                // Runtime has shut down
                false
            }
        }
    }
}

/// The plugin's async runtime.
///
/// Manages the Tokio runtime and sink worker threads.
pub struct PluginRuntime {
    /// Tokio runtime for async operations
    runtime: Option<Runtime>,
    /// Worker thread handle
    worker_handle: Option<JoinHandle<()>>,
    /// Update sender for validator threads
    sender: UpdateSender,
    /// Update receiver for worker
    receiver: Option<Receiver<EnvelopedUpdate>>,
}

impl PluginRuntime {
    /// Create a new runtime with the given configuration.
    pub fn new(config: &PluginConfig) -> Result<Self, GeyserTapError> {
        // Create bounded channel
        let (tx, rx) = bounded::<EnvelopedUpdate>(config.plugin.channel_capacity);

        let sender = UpdateSender {
            sender: tx,
            sequence: Arc::new(AtomicU64::new(0)),
        };

        // Create Tokio runtime
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.plugin.worker_threads)
            .thread_name("geyser-tap-worker")
            .enable_all()
            .build()
            .map_err(|e| GeyserTapError::Internal(format!("failed to create runtime: {e}")))?;

        Ok(Self {
            runtime: Some(runtime),
            worker_handle: None,
            sender,
            receiver: Some(rx),
        })
    }

    /// Get a sender for validator threads.
    pub fn sender(&self) -> UpdateSender {
        self.sender.clone()
    }

    /// Start the worker with the given sinks.
    pub fn start(&mut self, sinks: Vec<Box<dyn Sink>>) -> Result<(), GeyserTapError> {
        let runtime = self
            .runtime
            .take()
            .ok_or_else(|| GeyserTapError::Internal("runtime already started".to_string()))?;

        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| GeyserTapError::Internal("receiver already taken".to_string()))?;

        let handle = thread::Builder::new()
            .name("geyser-tap-main".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    run_worker(receiver, sinks).await;
                });
            })
            .map_err(|e| GeyserTapError::Internal(format!("failed to spawn worker: {e}")))?;

        self.worker_handle = Some(handle);
        Ok(())
    }

    /// Shutdown the runtime gracefully.
    pub fn shutdown(self) -> Result<(), GeyserTapError> {
        // Drop sender to signal shutdown
        drop(self.sender);

        // Wait for worker to finish
        if let Some(handle) = self.worker_handle {
            handle
                .join()
                .map_err(|_| GeyserTapError::Internal("worker thread panicked".to_string()))?;
        }

        Ok(())
    }
}

/// Worker loop that processes updates and sends to sinks.
async fn run_worker(receiver: Receiver<EnvelopedUpdate>, sinks: Vec<Box<dyn Sink>>) {
    tracing::info!(
        sink_count = sinks.len(),
        "Starting Geyser plugin worker"
    );

    loop {
        // Receive update (blocking on crossbeam channel from sync context)
        let envelope = match receiver.recv() {
            Ok(e) => e,
            Err(_) => {
                // Channel closed - shutdown
                tracing::info!("Update channel closed, shutting down worker");
                break;
            }
        };

        // Update metrics
        geyser_tap_common::metrics::set_channel_depth(receiver.len());

        // Send to all sinks concurrently
        let update = envelope.update;
        for sink in &sinks {
            if sink.health().can_accept() {
                if let Err(e) = sink.send(update.clone()).await {
                    tracing::error!(
                        sink = sink.name(),
                        error = %e,
                        "Failed to send update to sink"
                    );
                }
            }
        }
    }

    // Shutdown sinks gracefully
    tracing::info!("Shutting down sinks");
    for sink in &sinks {
        if let Err(e) = sink.flush().await {
            tracing::error!(sink = sink.name(), error = %e, "Failed to flush sink");
        }
        if let Err(e) = sink.shutdown().await {
            tracing::error!(sink = sink.name(), error = %e, "Failed to shutdown sink");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<UpdateSender>();
    }
}
