//! Sink trait and related abstractions.
//!
//! The [`Sink`] trait defines the interface for downstream data sinks.
//! Implementations must handle:
//! - Asynchronous data transmission
//! - Backpressure from slow consumers
//! - Graceful shutdown
//! - Reconnection on failure
//!
//! ## Thread Safety
//!
//! Sinks are called from the plugin's async runtime, not directly from
//! validator callback threads. The plugin handles the sync-to-async
//! boundary via bounded channels.
//!
//! ## Backpressure Model
//!
//! Sinks should:
//! 1. Use internal buffering with bounded capacity
//! 2. Return `SinkError::Backpressure` when buffers are full
//! 3. Expose metrics for monitoring buffer utilization
//! 4. Never block indefinitely

use crate::{SinkError, SinkResult, Update};
use std::future::Future;
use std::pin::Pin;

/// Health status of a sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkHealth {
    /// Sink is healthy and accepting updates
    Healthy,
    /// Sink is degraded (e.g., high latency, reconnecting)
    Degraded,
    /// Sink is unhealthy (e.g., connection lost)
    Unhealthy,
    /// Sink is shut down
    Shutdown,
}

impl SinkHealth {
    /// Returns true if the sink can accept updates.
    pub fn can_accept(&self) -> bool {
        matches!(self, SinkHealth::Healthy | SinkHealth::Degraded)
    }
}

/// Statistics about a sink's current state.
#[derive(Debug, Clone, Default)]
pub struct SinkStats {
    /// Number of updates successfully sent
    pub updates_sent: u64,
    /// Number of updates dropped due to backpressure
    pub updates_dropped: u64,
    /// Number of bytes sent
    pub bytes_sent: u64,
    /// Current buffer utilization (0.0 - 1.0)
    pub buffer_utilization: f64,
    /// Number of reconnection attempts
    pub reconnect_count: u64,
    /// Last error message, if any
    pub last_error: Option<String>,
}

/// Trait for downstream data sinks.
///
/// Sinks receive updates from the Geyser plugin and transmit them
/// to external systems (gRPC streams, Kafka topics, etc.).
///
/// # Implementation Requirements
///
/// 1. **Non-blocking**: `send` must not block for extended periods
/// 2. **Backpressure**: Return `SinkError::Backpressure` when overwhelmed
/// 3. **Idempotent shutdown**: `shutdown` can be called multiple times
/// 4. **Metrics**: Expose statistics via `stats()`
///
/// # Example
///
/// ```ignore
/// struct MySink { /* ... */ }
///
/// #[async_trait]
/// impl Sink for MySink {
///     async fn send(&self, update: Update) -> SinkResult<()> {
///         // Transmit to downstream
///         Ok(())
///     }
///
///     // ... other methods
/// }
/// ```
pub trait Sink: Send + Sync + 'static {
    /// Send an update to the downstream sink.
    ///
    /// This method should:
    /// - Return quickly (queue internally if needed)
    /// - Return `SinkError::Backpressure` if internal buffers are full
    /// - Return `SinkError::Shutdown` if the sink has been shut down
    ///
    /// The update is owned and can be serialized/transmitted asynchronously.
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;

    /// Start the sink.
    ///
    /// Called once by the plugin worker, from inside the async runtime, before
    /// any updates are delivered. Sinks that own a listening socket (gRPC,
    /// websocket) bind it here; a sink that needs no startup can rely on the
    /// default no-op.
    ///
    /// Binding must happen here rather than in the constructor because
    /// `create_sinks()` runs on the validator's thread during `on_load`, where
    /// there is no reactor to register a listener with.
    fn start(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    /// Flush any buffered updates to the downstream.
    ///
    /// Called periodically and before shutdown to ensure data is persisted.
    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;

    /// Gracefully shut down the sink.
    ///
    /// This method should:
    /// 1. Stop accepting new updates
    /// 2. Flush remaining buffered data
    /// 3. Close connections
    ///
    /// After shutdown, `send` should return `SinkError::Shutdown`.
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;

    /// Return the current health status of the sink.
    fn health(&self) -> SinkHealth;

    /// Return current statistics about the sink.
    fn stats(&self) -> SinkStats;

    /// Return the sink's name for logging and metrics.
    fn name(&self) -> &'static str;
}

/// Configuration for sink behavior.
#[derive(Debug, Clone)]
pub struct SinkBehavior {
    /// Maximum number of updates to buffer before applying backpressure
    pub buffer_capacity: usize,
    /// How long to wait before retrying a failed send (milliseconds)
    pub retry_delay_ms: u64,
    /// Maximum number of retry attempts before dropping an update
    pub max_retries: u32,
    /// Whether to drop updates on backpressure (true) or block (false)
    pub drop_on_backpressure: bool,
    /// Flush interval in milliseconds (0 = no automatic flush)
    pub flush_interval_ms: u64,
}

impl Default for SinkBehavior {
    fn default() -> Self {
        Self {
            buffer_capacity: 10_000,
            retry_delay_ms: 100,
            max_retries: 3,
            drop_on_backpressure: true, // Default to dropping to protect validator
            flush_interval_ms: 1000,
        }
    }
}

/// A composite sink that fans out to multiple downstream sinks.
///
/// Updates are sent to all sinks concurrently. If any sink fails,
/// the error is logged but other sinks continue receiving updates.
pub struct FanoutSink {
    sinks: Vec<Box<dyn Sink>>,
}

impl FanoutSink {
    /// Create a new fanout sink with the given downstream sinks.
    pub fn new(sinks: Vec<Box<dyn Sink>>) -> Self {
        Self { sinks }
    }

    /// Add a sink to the fanout.
    pub fn add_sink(&mut self, sink: Box<dyn Sink>) {
        self.sinks.push(sink);
    }
}

impl Sink for FanoutSink {
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            if self.sinks.is_empty() {
                return Ok(());
            }

            // Send to all sinks concurrently
            let futures: Vec<_> = self
                .sinks
                .iter()
                .filter(|sink| sink.health().can_accept())
                .map(|sink| {
                    let update = update.clone();
                    async move {
                        let name = sink.name();
                        match sink.send(update).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::warn!(sink = name, error = %e, "Fanout sink error");
                                Err(e)
                            }
                        }
                    }
                })
                .collect();

            // Execute all sends concurrently
            let results = futures::future::join_all(futures).await;

            // Return error if ALL sinks failed
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            if success_count == 0 && !self.sinks.is_empty() {
                return Err(SinkError::AllSinksFailed);
            }

            Ok(())
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            let futures: Vec<_> = self.sinks.iter().map(|sink| sink.flush()).collect();
            let results = futures::future::join_all(futures).await;

            // Log any errors but don't fail
            for result in results {
                if let Err(e) = result {
                    tracing::warn!(error = %e, "Fanout flush error");
                }
            }

            Ok(())
        })
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            let futures: Vec<_> = self.sinks.iter().map(|sink| sink.shutdown()).collect();
            let results = futures::future::join_all(futures).await;

            for result in results {
                if let Err(e) = result {
                    tracing::warn!(error = %e, "Fanout shutdown error");
                }
            }

            Ok(())
        })
    }

    fn health(&self) -> SinkHealth {
        if self.sinks.is_empty() {
            return SinkHealth::Healthy;
        }

        let healthy_count = self
            .sinks
            .iter()
            .filter(|s| s.health() == SinkHealth::Healthy)
            .count();

        if healthy_count == self.sinks.len() {
            SinkHealth::Healthy
        } else if healthy_count > 0 {
            SinkHealth::Degraded
        } else {
            SinkHealth::Unhealthy
        }
    }

    fn stats(&self) -> SinkStats {
        let mut combined = SinkStats::default();
        for sink in &self.sinks {
            let s = sink.stats();
            combined.updates_sent += s.updates_sent;
            combined.updates_dropped += s.updates_dropped;
            combined.bytes_sent += s.bytes_sent;
            combined.reconnect_count += s.reconnect_count;
        }
        if !self.sinks.is_empty() {
            combined.buffer_utilization = self
                .sinks
                .iter()
                .map(|s| s.stats().buffer_utilization)
                .sum::<f64>()
                / self.sinks.len() as f64;
        }
        combined
    }

    fn name(&self) -> &'static str {
        "fanout"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_behavior_defaults() {
        let behavior = SinkBehavior::default();
        assert_eq!(behavior.buffer_capacity, 10_000);
        assert!(behavior.drop_on_backpressure);
    }

    #[test]
    fn sink_health_can_accept() {
        assert!(SinkHealth::Healthy.can_accept());
        assert!(SinkHealth::Degraded.can_accept());
        assert!(!SinkHealth::Unhealthy.can_accept());
        assert!(!SinkHealth::Shutdown.can_accept());
    }
}
