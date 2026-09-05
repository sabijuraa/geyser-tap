//! Prometheus metrics for geyser-tap.
//!
//! Metrics are critical for monitoring plugin health in production.
//! All metrics use the `geyser_tap_` prefix for easy identification.
//!
//! ## Key Metrics
//!
//! - `geyser_tap_updates_received_total`: Updates received from validator
//! - `geyser_tap_updates_sent_total`: Updates sent to sinks
//! - `geyser_tap_updates_dropped_total`: Updates dropped due to backpressure
//! - `geyser_tap_channel_depth`: Current channel utilization
//! - `geyser_tap_sink_latency_seconds`: Sink send latency histogram

use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec, CounterVec,
    Gauge, GaugeVec, HistogramVec,
};

/// Total updates received from the validator, labeled by update type.
pub static UPDATES_RECEIVED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "geyser_tap_updates_received_total",
        "Total number of updates received from the validator",
        &["type"]
    )
    .expect("failed to register UPDATES_RECEIVED metric")
});

/// Total updates successfully sent to sinks, labeled by sink name and update type.
pub static UPDATES_SENT: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "geyser_tap_updates_sent_total",
        "Total number of updates successfully sent to sinks",
        &["sink", "type"]
    )
    .expect("failed to register UPDATES_SENT metric")
});

/// Total updates dropped due to backpressure, labeled by sink name.
pub static UPDATES_DROPPED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "geyser_tap_updates_dropped_total",
        "Total number of updates dropped due to backpressure",
        &["sink"]
    )
    .expect("failed to register UPDATES_DROPPED metric")
});

/// Current depth of the internal channel (updates pending processing).
pub static CHANNEL_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "geyser_tap_channel_depth",
        "Current number of updates in the internal channel"
    )
    .expect("failed to register CHANNEL_DEPTH metric")
});

/// Maximum capacity of the internal channel.
pub static CHANNEL_CAPACITY: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "geyser_tap_channel_capacity",
        "Maximum capacity of the internal channel"
    )
    .expect("failed to register CHANNEL_CAPACITY metric")
});

/// Sink health status (1 = healthy, 0 = unhealthy).
pub static SINK_HEALTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "geyser_tap_sink_health",
        "Sink health status (1 = healthy, 0 = unhealthy)",
        &["sink"]
    )
    .expect("failed to register SINK_HEALTH metric")
});

/// Histogram of sink send latencies in seconds.
pub static SINK_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "geyser_tap_sink_latency_seconds",
        "Histogram of sink send latencies",
        &["sink"],
        // Buckets: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    )
    .expect("failed to register SINK_LATENCY metric")
});

/// Bytes sent to sinks, labeled by sink name.
pub static BYTES_SENT: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "geyser_tap_bytes_sent_total",
        "Total bytes sent to sinks",
        &["sink"]
    )
    .expect("failed to register BYTES_SENT metric")
});

/// Current slot being processed.
pub static CURRENT_SLOT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "geyser_tap_current_slot",
        "Most recent slot received from the validator"
    )
    .expect("failed to register CURRENT_SLOT metric")
});

/// Number of connected gRPC clients.
pub static GRPC_CLIENTS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "geyser_tap_grpc_clients",
        "Number of connected gRPC clients"
    )
    .expect("failed to register GRPC_CLIENTS metric")
});

/// Record that an update was received.
pub fn record_update_received(update_type: &str) {
    UPDATES_RECEIVED.with_label_values(&[update_type]).inc();
}

/// Record that an update was successfully sent.
pub fn record_update_sent(sink: &str, update_type: &str) {
    UPDATES_SENT.with_label_values(&[sink, update_type]).inc();
}

/// Record that an update was dropped.
pub fn record_update_dropped(sink: &str) {
    UPDATES_DROPPED.with_label_values(&[sink]).inc();
}

/// Update channel depth metric.
pub fn set_channel_depth(depth: usize) {
    CHANNEL_DEPTH.set(depth as f64);
}

/// Update sink health metric.
pub fn set_sink_health(sink: &str, healthy: bool) {
    SINK_HEALTH
        .with_label_values(&[sink])
        .set(if healthy { 1.0 } else { 0.0 });
}

/// Record sink latency.
pub fn record_sink_latency(sink: &str, latency_seconds: f64) {
    SINK_LATENCY
        .with_label_values(&[sink])
        .observe(latency_seconds);
}

/// Record bytes sent to sink.
pub fn record_bytes_sent(sink: &str, bytes: usize) {
    BYTES_SENT.with_label_values(&[sink]).inc_by(bytes as f64);
}

/// Update current slot metric.
pub fn set_current_slot(slot: u64) {
    CURRENT_SLOT.set(slot as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_can_be_recorded() {
        // Just verify metrics don't panic
        record_update_received("account");
        record_update_sent("grpc", "account");
        record_update_dropped("kafka");
        set_channel_depth(100);
        set_sink_health("grpc", true);
        record_sink_latency("grpc", 0.001);
        record_bytes_sent("kafka", 1024);
        set_current_slot(12345);
    }
}
