//! Kafka producer implementation.
//!
//! This module wraps librdkafka's producer with:
//! - Async interface using tokio
//! - Proper error handling
//! - Metrics integration
//! - Graceful shutdown

use crate::partitioner::Partitioner;
use crate::serializer::UpdateSerializer;
use geyser_tap_common::{KafkaSinkConfig, SinkError, SinkResult, Update};
use parking_lot::Mutex;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;

/// Kafka producer wrapper.
///
/// Provides an async interface over librdkafka's FutureProducer with
/// integration for metrics and error handling.
pub struct KafkaProducer {
    /// The underlying rdkafka producer
    producer: FutureProducer,
    /// Serializer for converting updates to bytes
    serializer: UpdateSerializer,
    /// Partitioner for determining partition assignment
    partitioner: Partitioner,
    /// Timeout for send operations
    send_timeout: Duration,
    /// Whether the producer has been shut down
    shutdown: Mutex<bool>,
}

impl KafkaProducer {
    /// Create a new Kafka producer.
    ///
    /// # Configuration
    ///
    /// The producer is configured with:
    /// - Compression (zstd by default)
    /// - Batching for throughput
    /// - Idempotent producer for exactly-once semantics
    pub fn new(config: &KafkaSinkConfig) -> SinkResult<Self> {
        let mut client_config = ClientConfig::new();

        // Required settings
        client_config.set("bootstrap.servers", &config.brokers);

        // Producer settings
        client_config.set("acks", &config.producer.acks);
        client_config.set("compression.type", &config.producer.compression);
        client_config.set("batch.size", config.producer.batch_size.to_string());
        client_config.set("linger.ms", config.producer.linger_ms.to_string());
        client_config.set(
            "max.in.flight.requests.per.connection",
            config.producer.max_in_flight.to_string(),
        );

        // Enable idempotent producer for exactly-once semantics
        client_config.set("enable.idempotence", "true");

        // Queue settings for backpressure
        client_config.set("queue.buffering.max.messages", "100000");
        client_config.set("queue.buffering.max.kbytes", "1048576"); // 1GB

        // SASL configuration
        if let Some(ref sasl) = config.sasl {
            client_config.set("security.protocol", "SASL_SSL");
            client_config.set("sasl.mechanism", &sasl.mechanism);
            client_config.set("sasl.username", &sasl.username);
            client_config.set("sasl.password", &sasl.password);
        }

        let producer = client_config
            .create()
            .map_err(|e| SinkError::Connection {
                endpoint: config.brokers.clone(),
                message: e.to_string(),
            })?;

        Ok(Self {
            producer,
            serializer: UpdateSerializer::new(),
            partitioner: Partitioner::new(),
            send_timeout: Duration::from_secs(5),
            shutdown: Mutex::new(false),
        })
    }

    /// Send an update to a Kafka topic.
    ///
    /// The update is serialized to protobuf and sent with a key derived
    /// from the update content (for partitioning).
    pub async fn send(&self, topic: &str, update: &Update) -> SinkResult<()> {
        if *self.shutdown.lock() {
            return Err(SinkError::Shutdown);
        }

        // Serialize the update
        let payload = self
            .serializer
            .serialize(update)
            .map_err(|e| SinkError::Send(e.to_string()))?;

        // Get partition key
        let key = self.partitioner.key_for_update(update);

        // Create the record
        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(&payload);

        // Send with timeout
        let start = std::time::Instant::now();
        let result = self
            .producer
            .send(record, Timeout::After(self.send_timeout))
            .await;

        let latency = start.elapsed().as_secs_f64();
        geyser_tap_common::metrics::record_sink_latency("kafka", latency);

        match result {
            Ok((partition, offset)) => {
                tracing::trace!(
                    topic,
                    partition,
                    offset,
                    "Message sent to Kafka"
                );
                Ok(())
            }
            Err((err, _)) => {
                tracing::error!(
                    error = %err,
                    topic,
                    "Failed to send message to Kafka"
                );
                Err(SinkError::Kafka(err.to_string()))
            }
        }
    }

    /// Flush all pending messages.
    ///
    /// This blocks until all messages in the producer queue have been
    /// sent to Kafka or the timeout is reached.
    pub async fn flush(&self) -> SinkResult<()> {
        use rdkafka::producer::Producer;

        let producer = self.producer.clone();
        let timeout = Duration::from_secs(30);

        tokio::task::spawn_blocking(move || {
            producer.flush(Timeout::After(timeout))
        })
        .await
        .map_err(|e| SinkError::Send(format!("Flush task failed: {e}")))?
        .map_err(|e| SinkError::Kafka(format!("Flush failed: {e:?}")))
    }

    /// Shutdown the producer.
    pub async fn shutdown(&self) -> SinkResult<()> {
        *self.shutdown.lock() = true;

        // Final flush before shutdown
        self.flush().await
    }

    /// Get the current queue depth (messages pending send).
    pub fn queue_depth(&self) -> usize {
        use rdkafka::producer::Producer;
        self.producer.in_flight_count().max(0) as usize
    }
}

#[cfg(test)]
mod tests {
    
    

    // Note: Integration tests would require a Kafka broker
}
