//! # geyser-tap-sink-kafka
//!
//! Kafka sink implementation for geyser-tap.
//!
//! This crate provides a high-performance Kafka producer that publishes
//! validator updates to Kafka topics. It supports:
//!
//! - Per-type topic routing (accounts, transactions, etc.)
//! - Partitioning by pubkey/signature for ordering guarantees
//! - Compression (zstd recommended)
//! - SASL/SSL authentication
//!
//! ## Architecture
//!
//! ```text
//! Plugin Callback Thread
//!         |
//!         v
//! +------------------+
//! | Bounded Channel  |  <-- Backpressure boundary
//! +------------------+
//!         |
//!         v
//! +------------------+
//! | Producer Worker  |  <-- Batches and sends to Kafka
//! +------------------+
//!         |
//!         v
//! +------------------+
//! |  Kafka Brokers   |
//! +------------------+
//! ```
//!
//! ## Backpressure Handling
//!
//! The Kafka producer uses librdkafka's internal message queue. When the
//! queue is full, we either:
//! 1. Drop messages (if configured) to protect the validator
//! 2. Block briefly and retry (with timeout)
//!
//! Dropped messages are counted in metrics for alerting.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms)]

pub mod partitioner;
pub mod producer;
pub mod serializer;

pub use producer::KafkaProducer;

use geyser_tap_common::{KafkaSinkConfig, Sink, SinkHealth, SinkResult, SinkStats, Update};
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Kafka sink implementation.
///
/// This sink publishes updates to Kafka topics using a high-performance
/// producer with batching and compression.
pub struct KafkaSink {
    /// The underlying Kafka producer
    producer: Arc<KafkaProducer>,
    /// Current health status
    health: Arc<RwLock<SinkHealth>>,
    /// Accumulated statistics
    stats: Arc<RwLock<SinkStats>>,
    /// Configuration
    config: KafkaSinkConfig,
}

impl KafkaSink {
    /// Create a new Kafka sink with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Kafka producer cannot be created
    /// (e.g., invalid broker configuration).
    pub fn new(config: KafkaSinkConfig) -> SinkResult<Self> {
        let producer = KafkaProducer::new(&config)?;

        Ok(Self {
            producer: Arc::new(producer),
            health: Arc::new(RwLock::new(SinkHealth::Healthy)),
            stats: Arc::new(RwLock::new(SinkStats::default())),
            config,
        })
    }

    /// Get the topic for an update type.
    fn topic_for_update(&self, update: &Update) -> &str {
        match update {
            Update::Account(_) => self
                .config
                .account_topic
                .as_deref()
                .unwrap_or(&self.config.topic),
            Update::Transaction(_) => self
                .config
                .transaction_topic
                .as_deref()
                .unwrap_or(&self.config.topic),
            _ => &self.config.topic,
        }
    }
}

impl Sink for KafkaSink {
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            let update_type = update.type_name();
            let size = update.size_bytes();
            let topic = self.topic_for_update(&update);

            // Serialize and send
            let result = self.producer.send(topic, &update).await;

            match result {
                Ok(()) => {
                    let mut stats = self.stats.write();
                    stats.updates_sent += 1;
                    stats.bytes_sent += size as u64;
                    geyser_tap_common::metrics::record_update_sent("kafka", update_type);
                }
                Err(ref e) => {
                    let mut stats = self.stats.write();
                    stats.updates_dropped += 1;
                    stats.last_error = Some(e.to_string());
                    geyser_tap_common::metrics::record_update_dropped("kafka");
                }
            }

            result
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::debug!("Flushing Kafka producer");
            self.producer.flush().await
        })
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("Shutting down Kafka sink");
            *self.health.write() = SinkHealth::Shutdown;

            // Flush remaining messages before shutdown
            self.producer.flush().await?;
            self.producer.shutdown().await
        })
    }

    fn health(&self) -> SinkHealth {
        *self.health.read()
    }

    fn stats(&self) -> SinkStats {
        self.stats.read().clone()
    }

    fn name(&self) -> &'static str {
        "kafka"
    }
}
