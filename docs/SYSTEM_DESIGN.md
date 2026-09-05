# geyser-tap System Design

This document describes the architecture, design decisions, and operational model of geyser-tap.

## Overview

geyser-tap is a Solana Geyser plugin that streams validator data (accounts, transactions, slots, block metadata, entries) to external consumers via gRPC, Kafka, and WebSocket. It runs inside the validator process as a dynamically loaded shared library.

## Core Challenge

Geyser plugins operate under severe constraints:

1. **FFI boundary**: The plugin is loaded via dlopen and called through C-ABI function pointers. A panic crossing this boundary is undefined behavior and will crash the validator.

2. **Hot path**: The validator calls plugin callbacks from its critical path threads (banking, replay, entry-writing). Blocking these threads delays consensus.

3. **Unbounded throughput**: The validator can produce hundreds of thousands of account updates per second. The plugin cannot assume a maximum rate.

4. **No control over lifecycle**: The validator may unload the plugin at any time. Cleanup must be fast and correct.

## Architecture

```
                      ┌─────────────────────────────────────────┐
                      │            SOLANA VALIDATOR             │
                      │                                         │
                      │  Banking Threads ───┐                   │
                      │  Replay Thread ─────┼──> GeyserPlugin   │
                      │  Entry Writer ──────┘    Callbacks      │
                      └───────────────────────────┬─────────────┘
                                                  │
                                        ┌─────────▼─────────┐
                                        │   catch_unwind    │
                                        │   (panic guard)   │
                                        └─────────┬─────────┘
                                                  │
    ┌─────────────────────────────────────────────▼─────────────────────────────────────────────┐
    │                                       PLUGIN                                               │
    │                                                                                            │
    │  ┌────────────────┐    ┌────────────────┐    ┌──────────────────┐                         │
    │  │ Filter Engine  │    │   Serializer   │    │  Bounded Channel │                         │
    │  │                │    │                │    │  (100k capacity) │                         │
    │  │ - by owner     │───>│ - bincode tx   │───>│                  │                         │
    │  │ - by pubkey    │    │ - copy account │    │  try_send()      │                         │
    │  │ - by type      │    │   data         │    │  (never blocks)  │                         │
    │  └────────────────┘    └────────────────┘    └────────┬─────────┘                         │
    │                                                       │                                    │
    │                                              ┌────────▼────────┐                          │
    │                                              │  Tokio Runtime  │                          │
    │                                              │  (worker pool)  │                          │
    │                                              └────────┬────────┘                          │
    │                                                       │                                    │
    │                    ┌──────────────────────────────────┼──────────────────────────────┐    │
    │                    │                                  │                               │    │
    │              ┌─────▼─────┐                     ┌──────▼──────┐               ┌───────▼───┐│
    │              │ gRPC Sink │                     │ Kafka Sink  │               │  WS Sink  ││
    │              │           │                     │             │               │           ││
    │              │ Per-client│                     │  Producer   │               │ Per-client││
    │              │ channels  │                     │  queue      │               │ channels  ││
    │              └─────┬─────┘                     └──────┬──────┘               └─────┬─────┘│
    │                    │                                  │                            │      │
    └────────────────────┼──────────────────────────────────┼────────────────────────────┼──────┘
                         │                                  │                            │
                   ┌─────▼─────┐                     ┌──────▼──────┐               ┌─────▼─────┐
                   │  Clients  │                     │    Kafka    │               │    WS     │
                   │  (gRPC)   │                     │   Broker    │               │  Clients  │
                   └───────────┘                     └─────────────┘               └───────────┘
```

## FFI Safety Model

Every callback from the validator is wrapped:

```rust
fn update_account(&self, account: ...) -> PluginResult<()> {
    self.catch_panic("update_account", AssertUnwindSafe(|| {
        // actual implementation
    }))
}
```

The `catch_panic` wrapper:
1. Runs the closure inside `std::panic::catch_unwind`
2. If a panic occurs, catches it before it reaches the FFI boundary
3. Logs the panic with context
4. Returns `GeyserPluginError` instead of unwinding

Additionally, the release profile sets `panic = "abort"` as a backstop. This prevents any unwind from crossing FFI even if a panic escapes `catch_unwind` due to a bug.

All `unsafe` blocks in the plugin are documented with their invariants (currently only the `CStr::from_ptr` in config path handling).

## Backpressure Design

The plugin uses a **drop-on-full** policy:

1. Validator callbacks put updates into a bounded crossbeam channel (default 100k capacity).
2. `try_send()` is used, never `send()` (blocking).
3. If the channel is full, the update is dropped and a metric is incremented.
4. The async runtime consumes updates and fans them out to sinks.
5. Each sink has its own bounded buffer for per-consumer backpressure.

This guarantees the validator is never blocked. The tradeoff is data loss under sustained overload. Operators monitor the `updates_dropped` metric and tune `channel_capacity` accordingly.

Per-sink backpressure:
- gRPC: Each client has a bounded mpsc channel. Slow clients are disconnected when their buffer fills.
- Kafka: The rdkafka producer queue is bounded. Overload triggers message drops or producer errors.
- WebSocket: Each client has a bounded send buffer. Slow clients are disconnected.

## Serialization Strategy

Account data uses zero-copy where possible. The validator provides a borrowed slice; we copy it into a `bytes::Bytes` once, then share that reference across all consumers.

Transaction serialization uses bincode for the message payload. This is a blocking operation on the validator callback thread, but bincode is fast enough in practice (microseconds for typical transactions).

Entry data serializes the transaction signatures within the entry, not full transactions (which are available via notify_transaction).

## Filtering Architecture

Filtering happens at two levels:

1. **Plugin-level filter**: Configured via JSON. Determines which update types the plugin even processes. This reduces work before the bounded channel.

2. **Subscriber-level filter**: Each gRPC/WS client specifies its own filter at subscription time. The fan-out stage evaluates these filters server-side to minimize bandwidth.

Filter types:
- By update type (accounts, transactions, slots, entries, block_metadata)
- By account owner program
- By account pubkey
- By transaction involvement (account keys in the transaction)
- Include/exclude votes
- Include/exclude failed transactions

## Sink Abstraction

All sinks implement the `Sink` trait:

```rust
pub trait Sink: Send + Sync + 'static {
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    fn health(&self) -> SinkHealth;
    fn stats(&self) -> SinkStats;
    fn name(&self) -> &'static str;
}
```

This allows the runtime to treat all sinks uniformly. The `FanoutSink` wraps multiple sinks and broadcasts to all healthy ones.

## Observability

Prometheus metrics exposed at the configured bind address:
- `geyser_tap_updates_received_total{type}`: Updates received from validator by type
- `geyser_tap_updates_sent_total{sink,type}`: Updates sent to sinks
- `geyser_tap_updates_dropped_total{reason}`: Dropped updates (channel full, sink error)
- `geyser_tap_channel_depth`: Current channel utilization
- `geyser_tap_channel_capacity`: Configured capacity
- `geyser_tap_current_slot`: Latest slot processed
- `geyser_tap_sink_health{sink}`: Sink health (0=unhealthy, 1=degraded, 2=healthy)

Structured tracing via `tracing` crate with configurable log level.

## Configuration

Configuration is loaded from a JSON file specified in the validator's geyser plugin config. The plugin validates the config at load time and fails fast on invalid values.

Required: At least one sink must be enabled.

Tunable parameters:
- `channel_capacity`: Backpressure threshold (100k default)
- `worker_threads`: Async runtime threads (4 default)
- Per-sink: bind addresses, buffer sizes, credentials, filters

## Lifecycle

**on_load:**
1. Parse and validate config
2. Initialize tracing
3. Create async runtime
4. Create sinks
5. Start runtime worker
6. Store state

**During operation:**
- Validator callbacks route through catch_unwind to serialization to channel
- Runtime worker fans out to sinks
- Metrics updated throughout

**on_unload:**
1. Signal shutdown
2. Drop channel sender (closes channel)
3. Wait for runtime worker to finish (flushes remaining updates)
4. Shutdown sinks
5. Clear state

## Workspace Crates

- `geyser-tap-plugin`: The cdylib loaded by the validator. Contains FFI boundary, runtime, and fanout.
- `geyser-tap-common`: Shared types, Sink trait, configuration, error types, metrics.
- `geyser-tap-proto`: Protocol buffer definitions and conversion code.
- `geyser-tap-sink-grpc`: gRPC server using tonic.
- `geyser-tap-sink-kafka`: Kafka producer using rdkafka.
- `geyser-tap-sink-ws`: WebSocket server using tokio-tungstenite.
- `geyser-tap-sdk`: Rust client SDK for consuming the gRPC stream.

## Trade-offs

**Drop-on-full vs block-on-full**: We chose dropping to protect the validator. The alternative would provide delivery guarantees but could stall consensus. Operators who need guaranteed delivery should use Kafka (which has its own durability) or size buffers for their workload.

**Sync callbacks to async sinks**: The validator callbacks are synchronous. We bridge via a crossbeam channel (sync producer, async consumer via spawn_blocking in the runtime). This adds one channel hop but keeps the callback fast.

**Serialization on hot path**: Transaction serialization happens on the validator callback thread. We accept this because bincode is fast and copying is necessary regardless. If profiling shows this as a bottleneck, we could serialize lazily or use a different format.

**JSON config**: We use JSON for configuration because the validator's plugin interface already uses JSON. TOML or YAML would require additional dependencies.
