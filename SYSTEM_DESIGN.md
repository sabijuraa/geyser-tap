# Geyser-Tap System Design

## Overview

Geyser-Tap is a native Rust Solana Geyser plugin that streams validator updates to downstream sinks (gRPC, Kafka). It runs inside the validator process as a dynamically loaded shared library.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOLANA VALIDATOR PROCESS                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Banking    │  │   Banking    │  │   Replay     │  │   Replay     │    │
│  │   Thread 1   │  │   Thread N   │  │   Thread     │  │   Thread     │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│         │                 │                 │                 │             │
│         │ update_account  │ update_account  │ update_slot     │ notify_     │
│         │                 │                 │ _status         │ entry       │
│         └────────┬────────┴────────┬────────┴────────┬────────┘             │
│                  │                 │                 │                      │
│                  v                 v                 v                      │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                     GEYSER-TAP PLUGIN                            │      │
│  │  ┌────────────────────────────────────────────────────────────┐  │      │
│  │  │                   FFI BOUNDARY                             │  │      │
│  │  │  - catch_unwind() wrapper                                  │  │      │
│  │  │  - CStr handling                                           │  │      │
│  │  │  - No panics escape                                        │  │      │
│  │  └────────────────────────────────────────────────────────────┘  │      │
│  │                           │                                      │      │
│  │                           v                                      │      │
│  │  ┌────────────────────────────────────────────────────────────┐  │      │
│  │  │               BOUNDED CHANNEL (100k)                       │  │      │
│  │  │  - crossbeam::bounded for backpressure                     │  │      │
│  │  │  - try_send() never blocks validator                       │  │      │
│  │  │  - Dropped updates are metriced                            │  │      │
│  │  └────────────────────────────────────────────────────────────┘  │      │
│  │                           │                                      │      │
│  └───────────────────────────┼──────────────────────────────────────┘      │
│                              │                                             │
└──────────────────────────────┼─────────────────────────────────────────────┘
                               │
                               v
    ┌──────────────────────────────────────────────────────────────────┐
    │                     PLUGIN RUNTIME                               │
    │  ┌──────────────────────────────────────────────────────────┐    │
    │  │                 TOKIO RUNTIME                            │    │
    │  │  - Multi-threaded (4 workers default)                    │    │
    │  │  - Async sink operations                                 │    │
    │  └──────────────────────────────────────────────────────────┘    │
    │                           │                                      │
    │              ┌────────────┴────────────┐                         │
    │              │                         │                         │
    │              v                         v                         │
    │  ┌──────────────────────┐  ┌──────────────────────┐              │
    │  │     GRPC SINK        │  │    KAFKA SINK        │              │
    │  │  ┌───────────────┐   │  │  ┌───────────────┐   │              │
    │  │  │  Broadcaster  │   │  │  │   Producer    │   │              │
    │  │  │  (fan-out)    │   │  │  │   (batched)   │   │              │
    │  │  └───────┬───────┘   │  │  └───────┬───────┘   │              │
    │  │          │           │  │          │           │              │
    │  │    ┌─────┴─────┐     │  │          │           │              │
    │  │    v     v     v     │  │          v           │              │
    │  │  [C1]  [C2]  [C3]    │  │    [Kafka Broker]    │              │
    │  └──────────────────────┘  └──────────────────────┘              │
    └──────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Validator Callback

The validator calls plugin methods from its threads:

```
Banking Thread -> update_account(ReplicaAccountInfoVersions, slot)
Replay Thread  -> update_slot_status(slot, parent, status)
Replay Thread  -> notify_entry(ReplicaEntryInfoVersions)
Replay Thread  -> notify_block_metadata(ReplicaBlockInfoVersions)
```

### 2. FFI Safety Layer

Every callback is wrapped:

```rust
fn update_account(&self, ...) -> PluginResult<()> {
    self.catch_panic("update_account", AssertUnwindSafe(|| {
        // Actual logic here
        // If this panics, catch_unwind captures it
        // and returns a GeyserPluginError instead
    }))
}
```

### 3. Sync-to-Async Bridge

The bounded channel bridges validator threads to the async runtime:

```rust
// Validator thread (sync)
match channel.try_send(envelope) {
    Ok(()) => { /* sent */ }
    Err(TrySendError::Full(_)) => {
        // DROP update to protect validator
        metrics.dropped.inc();
    }
}
```

### 4. Sink Distribution

The runtime worker receives updates and distributes to all sinks:

```rust
loop {
    let update = receiver.recv()?;
    
    // Send to all sinks concurrently
    for sink in &sinks {
        sink.send(update.clone()).await?;
    }
}
```

## FFI Safety Model

### Problem

The plugin is a `.so` loaded by the validator via `dlopen()`. The validator is C++ code calling into Rust. Panics that unwind across the FFI boundary cause undefined behavior (usually a crash).

### Solution

1. **`catch_unwind` wrapper**: catches panics before the FFI boundary
2. **`panic = "unwind"` in release profile**: required for (1) to work at all
3. **Explicit `CStr` handling**: No assumptions about validator-provided strings
4. **No allocations visible to C**: All memory is Rust-managed

```rust
// Release profile MUST unwind, or catch_unwind below is a no-op
[profile.release]
panic = "unwind"

// Runtime safety net
fn catch_panic<F, R>(&self, context: &str, f: F) -> PluginResult<R>
where
    F: FnOnce() -> PluginResult<R> + UnwindSafe,
{
    match std::panic::catch_unwind(f) {
        Ok(result) => result,
        Err(panic) => {
            tracing::error!("Panic in {}: {:?}", context, panic);
            Err(GeyserPluginError::Custom(...))
        }
    }
}
```

## Toolchain and ABI Compatibility

The plugin's entry point is:

```rust
#[no_mangle]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin
```

Despite the `extern "C"`, the return type is a Rust trait object: a fat pointer
carrying a data pointer and a vtable pointer. Rust does not guarantee a stable
layout for vtables across compiler versions, so the validator and the plugin
must agree on that layout. In practice this means **the plugin must be compiled
with the same rustc version as the validator it is loaded into.**

A mismatch does not produce a link error or a helpful message. The validator
dereferences a vtable laid out differently from the one it expects and
segfaults during `on_load`, before any plugin code has a chance to log
anything.

`solana-test-validator 1.18.26` is built with rustc 1.75.0. The rustc commit
can be read out of any validator binary:

```sh
strings <validator-binary> | grep -o '/rustc/[a-f0-9]*'
```

`rust-toolchain.toml` pins the toolchain accordingly, and the Dockerfile pins
the same version. `Cargo.lock` is committed because building under 1.75
requires holding several transitive dependencies below their current releases,
and those pins live only in the lockfile.

Two related failure modes are worth knowing, because both present as a
segfault rather than an error:

- **Interface version drift.** The plugin targets
  `solana-geyser-plugin-interface` 1.18. Loading it into a validator from a
  different interface major version fails the same way.
- **Errors returned from `on_load`.** `GeyserPluginError::Custom` carries a
  `Box<dyn Error>` whose vtable lives inside the plugin. The validator's
  load-failure path may drop the library before formatting that error,
  dereferencing a vtable in an unloaded object. A configuration mistake can
  therefore surface as a crash rather than a message, so config is validated
  eagerly and kept simple.

## Zero-Copy Serialization Strategy

### Problem

High-throughput updates (1M+ accounts/sec on mainnet) make copying expensive.

### Solution

1. **`bytes::Bytes` for large payloads**: Reference-counted, clone is cheap
2. **Protobuf with `bytes` field type**: Maps directly to `Bytes`
3. **Single allocation per update**: Copy from validator -> owned `Bytes`

```rust
// Account data uses Bytes (reference counted)
pub struct AccountUpdate {
    pub data: Bytes,  // Not Vec<u8>
    // ...
}

// Protobuf field config in build.rs
tonic_build::configure()
    .bytes(["AccountUpdate.data", ...])
    .compile(...)?;
```

## Backpressure Handling

### Channel Backpressure (Validator -> Runtime)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Bounded Channel                             │
│  Capacity: 100,000 updates                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ [U] [U] [U] [U] [U] [U] [U] [U] [U] [U] ... [U] [U]     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  When full:                                                     │
│  - try_send() returns Err(Full)                                 │
│  - Update is DROPPED (not blocked)                              │
│  - Metric: geyser_tap_updates_dropped_total{sink="channel"}     │
│                                                                 │
│  Why drop?                                                      │
│  - Blocking would stall validator consensus                     │
│  - Validator stability > data completeness                      │
└─────────────────────────────────────────────────────────────────┘
```

### Sink Backpressure (Runtime -> External)

**gRPC Sink:**
```
┌─────────────────────────────────────────────────────────────────┐
│  Per-client buffer (1000 updates each)                         │
│                                                                 │
│  Client 1: [U][U][U][_][_]     Fast consumer                    │
│  Client 2: [U][U][U][U][U]     Slow consumer (buffer full)      │
│  Client 3: [U][_][_][_][_]     Medium consumer                  │
│                                                                 │
│  When Client 2 buffer full:                                     │
│  - Update dropped for Client 2 only                             │
│  - Other clients unaffected                                     │
│  - Metric: geyser_tap_updates_dropped_total{sink="grpc"}        │
└─────────────────────────────────────────────────────────────────┘
```

**Kafka Sink:**
```
┌─────────────────────────────────────────────────────────────────┐
│  librdkafka internal queue                                      │
│  - queue.buffering.max.messages: 100,000                        │
│  - queue.buffering.max.kbytes: 1GB                              │
│                                                                 │
│  When queue full:                                               │
│  - produce() returns QueueFull error                            │
│  - Update dropped                                               │
│  - Metric: geyser_tap_updates_dropped_total{sink="kafka"}       │
└─────────────────────────────────────────────────────────────────┘
```

## Thread Model

```
┌─────────────────────────────────────────────────────────────────┐
│                        THREAD OWNERSHIP                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  VALIDATOR THREADS (not ours):                                  │
│  - Banking threads (multiple): update_account, notify_txn      │
│  - Replay thread: update_slot_status, notify_entry             │
│  - These call into our plugin synchronously                    │
│                                                                 │
│  PLUGIN THREADS (ours):                                        │
│  - Main worker thread: receives from channel                   │
│  - Tokio runtime threads (4): async sink operations            │
│                                                                 │
│  SHARED STATE:                                                  │
│  - PluginState: RwLock<Arc<...>>                               │
│  - UpdateSender: Clone + Send (crossbeam channel)              │
│  - Config: immutable after on_load                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Shutdown Sequence

```
1. Validator calls on_unload()
   │
2. Drop UpdateSender (closes channel)
   │
3. Worker loop exits (recv returns Err)
   │
4. Flush all sinks
   │   - gRPC: drain client buffers
   │   - Kafka: producer.flush(timeout)
   │
5. Shutdown sinks
   │   - gRPC: close server, disconnect clients
   │   - Kafka: close producer
   │
6. Join worker thread
   │
7. Drop Tokio runtime
   │
8. on_unload() returns
```

## Performance Characteristics

| Metric | Target | Notes |
|--------|--------|-------|
| Callback latency | < 1ms | Must not block validator |
| Channel throughput | 1M msg/sec | crossbeam bounded channel |
| gRPC fan-out | 100 clients | Per-client buffering |
| Kafka throughput | 500k msg/sec | With batching + compression |
| Memory overhead | < 1GB | Bounded by channel capacity |

## Failure Modes

| Failure | Impact | Mitigation |
|---------|--------|------------|
| Kafka broker down | Kafka updates dropped | Metrics alert, reconnect |
| gRPC client slow | Client updates dropped | Per-client isolation |
| Plugin panic | Caught, logged | catch_unwind wrapper |
| Channel full | Updates dropped | Metrics alert |
| OOM | Validator crash | Bounded buffers |

## Metrics

All metrics use `geyser_tap_` prefix:

| Metric | Type | Description |
|--------|------|-------------|
| `updates_received_total` | Counter | Updates from validator |
| `updates_sent_total` | Counter | Updates to sinks |
| `updates_dropped_total` | Counter | Updates dropped |
| `channel_depth` | Gauge | Current channel utilization |
| `sink_latency_seconds` | Histogram | Sink send latency |
| `sink_health` | Gauge | Sink health (0/1) |
| `current_slot` | Gauge | Latest slot processed |
| `grpc_clients` | Gauge | Connected gRPC clients |

## Filtering

Filtering happens at two levels, so work is avoided as early as possible.

**Plugin-level.** Configured in JSON and applied inside the validator callback,
before anything is copied or enqueued. An update type that no sink wants is
never serialized and never occupies channel capacity. The plugin-level filter
is the union of every enabled sink's filter.

**Subscriber-level.** Each gRPC or WebSocket client supplies its own filter at
subscription time, evaluated server-side in the fan-out stage so a narrow
subscriber does not pay for traffic it will discard.

Available predicates: update type (accounts, transactions, slots, entries,
block metadata), account owner program, account pubkey, and include/exclude
toggles for vote and failed transactions.

The WebSocket sink currently applies the plugin-level filter only; it has no
per-client filtering, so every connected client receives the full configured
stream.

## Sink Abstraction

Every egress path implements one trait, so the runtime treats them uniformly:

```rust
pub trait Sink: Send + Sync + 'static {
    fn send(&self, update: Update) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    fn start(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>> { /* no-op default */ }
    fn flush(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = SinkResult<()>> + Send + '_>>;
    fn health(&self) -> SinkHealth;
    fn stats(&self) -> SinkStats;
    fn name(&self) -> &'static str;
}
```

`start()` exists because a sink that owns a listening socket cannot bind in its
constructor: `create_sinks()` runs on the validator's thread during `on_load`,
where there is no reactor to register a listener with. The worker calls
`start()` for each sink from inside the runtime instead. A sink that fails to
start is logged and left unstarted rather than failing plugin load, so losing
one egress path does not take down a validator.

`FanoutSink` wraps a set of sinks and broadcasts to the healthy ones.

## Workspace Layout

| Crate | Role |
|-------|------|
| `geyser-tap-plugin` | The cdylib the validator loads: FFI boundary, state, runtime bridge |
| `geyser-tap-common` | Shared update types, `Sink` trait, config, errors, metrics, exporter |
| `geyser-tap-proto` | Protobuf messages and the generated tonic service |
| `geyser-tap-sink-grpc` | tonic server and per-subscriber fan-out |
| `geyser-tap-sink-kafka` | rdkafka producer and partitioning |
| `geyser-tap-sink-ws` | tokio-tungstenite server |
| `geyser-tap-sdk` | Rust client for consuming the gRPC stream |

## Trade-offs

**Drop rather than block.** Back-pressure is resolved by discarding updates,
never by blocking a validator thread. The alternative would give delivery
guarantees at the risk of stalling consensus, which is not a trade worth
making inside someone else's validator. Operators needing durability should
use the Kafka sink, which has its own.

**Serialization on the callback thread.** Payloads are copied and serialized
where the validator hands them over, because the borrowed data is only valid
for the duration of the call. `Bytes` keeps subsequent clones cheap, so the
cost is paid once.

**Sync callbacks, async sinks.** The validator's callbacks are synchronous and
the sinks are async. A bounded crossbeam channel bridges them, costing one hop
and keeping the callback free of any await.

**JSON configuration.** The validator's plugin interface already hands the
plugin a JSON file path, so using anything else would mean carrying a second
config format for no benefit.

## Configuration Example

```json
{
  "libpath": "/opt/geyser-tap/libgeyser_tap_plugin.so",
  
  "grpc": {
    "enabled": true,
    "bind_address": "0.0.0.0:10000",
    "max_connections": 100,
    "send_buffer_size": 10000
  },
  
  "kafka": {
    "enabled": true,
    "brokers": "kafka-1:9092,kafka-2:9092,kafka-3:9092",
    "topic": "solana-updates",
    "producer": {
      "acks": "all",
      "compression": "zstd",
      "batch_size": 1000000,
      "linger_ms": 5
    }
  },
  
  "plugin": {
    "channel_capacity": 100000,
    "worker_threads": 4
  },
  
  "metrics": {
    "enabled": true,
    "bind_address": "127.0.0.1:9090"
  }
}
```
