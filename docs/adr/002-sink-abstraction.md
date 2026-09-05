# ADR-002: Sink Abstraction

## Status

Accepted

## Context

Geyser-Tap needs to send validator updates to multiple downstream systems:
- gRPC streams for low-latency consumers
- Kafka for durable, replayable storage
- Future: Redis Streams, Kinesis, custom sinks

We need an abstraction that:
1. Allows adding new sink types without modifying core plugin code
2. Handles backpressure uniformly
3. Provides consistent metrics across all sinks
4. Supports concurrent sends to multiple sinks

## Decision

We define a `Sink` trait as the abstraction for all downstream outputs:

```rust
pub trait Sink: Send + Sync + 'static {
    /// Send an update to the sink (non-blocking)
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    
    /// Flush buffered data
    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    
    /// Graceful shutdown
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    
    /// Current health status
    fn health(&self) -> SinkHealth;
    
    /// Statistics
    fn stats(&self) -> SinkStats;
    
    /// Sink name for logging/metrics
    fn name(&self) -> &'static str;
}
```

### Design Principles

#### 1. Async Interface

Sinks use async methods because downstream I/O (network, disk) is inherently async:

```rust
// Async send allows non-blocking I/O
async fn send_to_grpc(&self, update: Update) -> SinkResult<()> {
    self.stream.send(update.into()).await
}
```

#### 2. Non-blocking Send

The `send` method must not block. If the sink cannot accept data:

```rust
fn send(&self, update: Update) -> SinkResult<()> {
    match self.buffer.try_send(update) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(SinkError::Backpressure { count: 1 }),
        Err(TrySendError::Closed(_)) => Err(SinkError::Shutdown),
    }
}
```

#### 3. Health Status

Sinks report their health for monitoring and routing decisions:

```rust
pub enum SinkHealth {
    Healthy,     // Accepting updates normally
    Degraded,    // Accepting but with issues (high latency, reconnecting)
    Unhealthy,   // Cannot accept updates
    Shutdown,    // Permanently stopped
}
```

#### 4. Statistics

Every sink provides consistent stats:

```rust
pub struct SinkStats {
    pub updates_sent: u64,
    pub updates_dropped: u64,
    pub bytes_sent: u64,
    pub buffer_utilization: f64,  // 0.0 - 1.0
    pub reconnect_count: u64,
    pub last_error: Option<String>,
}
```

### Sink Implementations

#### gRPC Sink

```rust
pub struct GrpcSink {
    broadcaster: Arc<Broadcaster>,  // Fan-out to clients
    // ...
}

impl Sink for GrpcSink {
    fn send(&self, update: Update) -> ... {
        // Broadcast to all connected clients
        self.broadcaster.broadcast(update).await
    }
}
```

#### Kafka Sink

```rust
pub struct KafkaSink {
    producer: KafkaProducer,
    // ...
}

impl Sink for KafkaSink {
    fn send(&self, update: Update) -> ... {
        let topic = self.topic_for_update(&update);
        let key = self.partitioner.key_for_update(&update);
        self.producer.send(topic, key, update).await
    }
}
```

### FanoutSink

For sending to multiple sinks concurrently:

```rust
pub struct FanoutSink {
    sinks: Vec<Box<dyn Sink>>,
}

impl Sink for FanoutSink {
    fn send(&self, update: Update) -> ... {
        // Send to all sinks concurrently
        let futures: Vec<_> = self.sinks.iter()
            .filter(|s| s.health().can_accept())
            .map(|s| s.send(update.clone()))
            .collect();
        
        // Collect results, log errors, continue
        let results = futures::future::join_all(futures).await;
        // ...
    }
}
```

## Consequences

### Positive

- **Extensibility**: Adding a Redis sink requires implementing one trait
- **Testability**: Mock sinks for testing plugin logic
- **Consistency**: All sinks have identical interfaces for metrics/monitoring
- **Isolation**: One sink's failure doesn't affect others

### Negative

- **Async complexity**: Trait with async methods requires boxing
- **Clone overhead**: Update must be cloned for fanout
- **Indirection**: Virtual dispatch for trait objects

### Trade-offs

We chose **boxed futures** (`Pin<Box<dyn Future>>`) over async-trait macro:

```rust
// Our approach: explicit boxing
fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;

// Alternative: async_trait macro
#[async_trait]
trait Sink {
    async fn send(&self, update: Update) -> SinkResult<()>;
}
```

Reasons:
1. Fewer dependencies (no async_trait macro)
2. Explicit about allocation
3. Compatible with stable Rust features

## Alternatives Considered

### Alternative 1: Enum Instead of Trait

```rust
enum SinkType {
    Grpc(GrpcSink),
    Kafka(KafkaSink),
}
```

**Rejected**: Requires modifying enum for each new sink type. Not extensible.

### Alternative 2: Channel-per-Sink

Each sink has its own channel from the plugin:

```rust
plugin -> channel_grpc -> grpc_sink
      \-> channel_kafka -> kafka_sink
```

**Rejected**: Duplicates backpressure logic. Harder to reason about.

### Alternative 3: Event-based (Observer Pattern)

```rust
sink_manager.subscribe(|update| { ... });
```

**Rejected**: Less control over async execution and error handling.

## References

- [Rust Async Book - Async Traits](https://rust-lang.github.io/async-book/)
- [Tokio Tower Service trait](https://docs.rs/tower-service) (similar pattern)
