# ADR-003: Sink Selection (gRPC, Kafka, WebSocket)

## Status
Accepted

## Context
geyser-tap needs to deliver updates to external consumers. Different consumers have different requirements:
- Real-time dashboards need low-latency streaming
- Data pipelines need durable, replayable event logs
- Simple scripts need easy-to-consume formats

We need to support multiple transport mechanisms to serve these use cases.

## Decision
We implement three sinks, each serving a different need:

**gRPC (tonic):**
- Server-streaming RPC for low-latency delivery
- Protocol buffers for efficient serialization
- Per-client subscription with server-side filtering
- Best for: real-time consumers, custom tooling, polyglot clients

**Kafka (rdkafka):**
- Durable message queue with replay capability
- Partitioned by slot for ordering guarantees
- Compressed with zstd for bandwidth efficiency
- Best for: data pipelines, offline analytics, guaranteed delivery

**WebSocket (tokio-tungstenite):**
- Simple JSON messages over WebSocket
- Easy to consume from browsers and scripts
- Subscription via JSON messages
- Best for: dashboards, debugging, quick prototyping

All three are optional and config-driven. At least one must be enabled.

## Consequences

**Positive:**
- Each transport serves its intended use case well
- Operators choose based on their requirements
- Multiple sinks can run simultaneously (fan-out)
- Standard protocols reduce integration friction

**Negative:**
- Three implementations to maintain
- Different serialization formats (protobuf, bincode, JSON)
- Configuration complexity

**Implementation details:**
- All sinks implement the common `Sink` trait
- The `FanoutSink` broadcasts to all enabled sinks
- Per-sink backpressure isolates failures
- Per-sink metrics enable targeted debugging

## Alternatives Considered

**Single transport (gRPC only):** Would simplify the codebase but excludes users who need Kafka durability or simple WebSocket access.

**Generic transport abstraction:** Would allow plugging in arbitrary transports, but adds complexity we don't need. Three specific transports cover known use cases.

**HTTP polling:** Would work but adds latency and complexity compared to streaming.

## References
- Tonic gRPC framework
- rdkafka Kafka client
- tokio-tungstenite WebSocket implementation
