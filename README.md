# Geyser-Tap

A Solana Geyser plugin for streaming validator updates to gRPC, WebSocket and Kafka.

## What Is This?

Geyser-Tap is a **native Rust plugin** that runs inside the Solana validator process. It receives real-time account updates, transactions, slot notifications, entries and block metadata, then streams them to downstream consumers.

## Status

Verified end-to-end against `solana-test-validator 1.18.26`: the plugin loads,
both the gRPC and WebSocket sinks bind, and a consumer receives real decoded
validator data (5496 updates across all five update types in 30s). The captured
evidence is in [VERIFICATION.md](VERIFICATION.md).

Not everything is proven. Current state, honestly:

| Area | State |
|------|-------|
| Plugin loads into validator | **Verified** - full session, no crash |
| gRPC sink | **Verified** - port binds, SDK consumer received real data |
| WebSocket sink | **Verified** - 3671 JSON frames received |
| Rust SDK client | **Verified** - produced the evidence above |
| FFI panic isolation | **Verified** - `panic="unwind"` confirmed on the artifact |
| Backpressure | Implemented; not stress-tested to forced drops |
| Kafka sink | **Implemented, never run against a broker** |
| Prometheus metrics | Counters recorded, but **no exporter exists** (see below) |
| Docker image | Rust pin corrected but **image never built** (no daemon here) |

This is a portfolio/reference implementation, not something that has carried
production traffic.

## Why Is This Hard?

Building a Geyser plugin isn't like building a normal application. It runs inside the validator process, so bugs have severe consequences:

| Challenge | Impact | How We Handle It |
|-----------|--------|------------------|
| **FFI/C ABI boundary** | Panics crash the validator | `catch_unwind` + `panic="unwind"` in release (see note) |
| **rustc vtable ABI** | Wrong compiler segfaults the validator on load | Toolchain pinned to the validator's exact rustc |
| **Validator thread blocking** | Consensus falls behind | Non-blocking `try_send`, drop on backpressure |
| **Memory exhaustion** | Validator OOM | Bounded channels, bounded buffers |
| **Slow downstream** | Updates pile up forever | Backpressure with drops + metrics |
| **Hot reload** | Plugin must handle restarts | Graceful shutdown, `on_unload` cleanup |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOLANA VALIDATOR                             │
│  Banking Threads ─┬── update_account() ──┐                      │
│  Replay Thread  ──┼── update_slot()   ───┼──> GEYSER-TAP PLUGIN │
│                   └── notify_entry()  ───┘                      │
└───────────────────────────────┬─────────────────────────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │   Bounded Channel (100k msgs)     │
              └─────────────────┬─────────────────┘
                                │
              ┌─────────────────┴─────────────────┐
              │          Plugin Runtime           │
              │  ┌─────────┐ ┌────────┐ ┌───────┐ │
              │  │gRPC Sink│ │WS Sink │ │ Kafka │ │
              │  └────┬────┘ └───┬────┘ └───┬───┘ │
              └───────┼──────────┼──────────┼─────┘
                      │          │          │
              ┌───────┴──┐ ┌─────┴────┐ ┌───┴─────────┐
              │ gRPC     │ │ WS       │ │ Kafka Broker│
              │ clients  │ │ clients  │ │ (untested)  │
              └──────────┘ └──────────┘ └─────────────┘
```

### A note on `panic`

The release profile uses `panic = "unwind"`, **not** `abort`. Under
`panic = "abort"` there is no unwinding at all, so the `catch_unwind` wrapping
every validator callback cannot catch anything and a plugin panic takes the
validator down -- which silently made the panic-isolation guarantee false.
Unwinding stays confined to the plugin: `catch_unwind` sits immediately inside
each `extern "C"` callback, so no panic reaches the FFI boundary.

## Features

- **gRPC streaming**: server-push updates to connected clients (verified)
- **WebSocket streaming**: JSON updates for consumers that cannot speak gRPC (verified)
- **Kafka publishing**: durable, replayable event log (implemented, not yet exercised against a broker)
- **Per-type filtering**: subscribe to accounts, transactions, slots, entries, block metadata
- **Account filters**: filter by owner program or pubkey
- **Backpressure**: drops updates rather than blocking the validator
- **Metrics counters**: recorded in-process; note there is currently **no
  Prometheus HTTP exporter** - see [Metrics](#metrics)

## Quick Start

### 1. Build

```bash
# Prerequisites: Rust 1.75.0 (exactly), protoc, cmake (for rdkafka)

git clone https://github.com/sabijuraa/geyser-tap.git
cd geyser-tap

# Build release binary. rust-toolchain.toml pins 1.75.0 automatically.
cargo build --release

# The plugin is at: target/release/libgeyser_tap_plugin.so
```

> **The Rust version is not a minimum, it is an exact requirement.**
> `_create_plugin` returns `*mut dyn GeyserPlugin`, a trait-object fat pointer
> handed across `dlopen`. Rust vtable layout is not ABI-stable between compiler
> versions, so a plugin built with a rustc other than the validator's
> **segfaults the validator on load**. Validator 1.18.26 is built with rustc
> 1.75.0. Building with 1.82 or 1.88 was measured to crash it; see
> [BLOCKERS.md](BLOCKERS.md).
>
> `Cargo.lock` is committed and load-bearing: building under 1.75 requires
> holding several transitive dependencies below their current releases.

### 2. Configure

Create `config.json`:

```json
{
  "libpath": "/path/to/libgeyser_tap_plugin.so",
  
  "grpc": {
    "enabled": true,
    "bind_address": "0.0.0.0:10000",
    "max_connections": 100
  },
  
  "kafka": {
    "enabled": true,
    "brokers": "localhost:9092",
    "topic": "solana-updates",
    "producer": {
      "compression": "zstd"
    }
  },
  
  "plugin": {
    "channel_capacity": 100000
  },
  
  "metrics": {
    "enabled": true,
    "bind_address": "127.0.0.1:9090"
  }
}
```

### 3. Run Validator with Plugin

```bash
solana-validator \
  --geyser-plugin-config /path/to/config.json \
  ... # other validator flags
```

The validator binary must match `solana-geyser-plugin-interface` in
`Cargo.toml` (currently 1.18). A validator from a different interface major
version -- Agave 4.x, for example -- will segfault on load.

### 4. Connect Clients

**gRPC (Rust, via the SDK):**
```rust
use geyser_tap_sdk::{GeyserClient, SubscriptionBuilder};

let mut client = GeyserClient::connect("http://localhost:10000").await?;

let subscription = SubscriptionBuilder::new()
    .accounts()
    .transactions()
    .include_votes(false)
    .slots()
    .build();

let mut stream = client.subscribe(subscription).await?;
while let Some(update) = stream.next().await {
    println!("Update: {:?}", update?);
}
```

A runnable version is in `crates/sdk/examples/consume.rs`; it is the consumer
used for the end-to-end verification:

```bash
cargo run --release -p geyser-tap-sdk --example consume -- http://127.0.0.1:10000 30
```

**WebSocket:** connect to `ws://localhost:10001` and read JSON frames, e.g.
`{"type":"entry","slot":57,"data":{...}}`. Note that the WebSocket sink has no
per-client filtering yet: every connected client receives the full configured
stream.

**Kafka:**
```bash
# Using kafkacat
kafkacat -b localhost:9092 -t solana-updates -C
```

## Project Structure

```
geyser-tap/
├── Cargo.toml              # Workspace root
├── SYSTEM_DESIGN.md        # Architecture documentation
├── crates/
│   ├── plugin/             # Main Geyser plugin (cdylib)
│   │   └── src/
│   │       ├── lib.rs      # Plugin entry point
│   │       ├── ffi.rs      # FFI-safe GeyserPlugin impl
│   │       ├── runtime.rs  # Async runtime bridge
│   │       └── state.rs    # Thread-safe state
│   ├── sink-grpc/          # gRPC sink
│   │   └── src/
│   │       ├── broadcaster.rs  # Fan-out to clients
│   │       ├── server.rs       # tonic gRPC server
│   │       └── service.rs      # Service implementation
│   ├── sink-kafka/         # Kafka sink
│   │   └── src/
│   │       ├── producer.rs     # rdkafka wrapper
│   │       ├── partitioner.rs  # Partitioning strategy
│   │       └── serializer.rs   # Protobuf encoding
│   ├── sink-ws/            # WebSocket sink
│   │   └── src/
│   │       ├── server.rs       # tokio-tungstenite server
│   │       └── client.rs       # Connected client tracking
│   ├── sdk/                # Rust client SDK
│   │   ├── src/
│   │   │   ├── client.rs       # GeyserClient
│   │   │   └── subscription.rs # SubscriptionBuilder
│   │   └── examples/
│   │       └── consume.rs      # End-to-end consumer
│   ├── common/             # Shared types and traits
│   │   ├── tests/
│   │   │   └── integration.rs  # Integration tests
│   │   └── src/
│   │       ├── sink.rs     # Sink trait
│   │       ├── types.rs    # Update types
│   │       ├── config.rs   # Configuration
│   │       ├── error.rs    # Error types
│   │       └── metrics.rs  # Prometheus metrics
│   └── proto/              # Protobuf definitions
│       ├── build.rs        # tonic-build: generates the service layer
│       └── src/
│           └── geyser.proto
└── docs/
    └── adr/                # Architecture Decision Records
        ├── 001-ffi-safety-model.md
        ├── 002-sink-abstraction.md
        ├── 003-backpressure-strategy.md
        ├── 004-serialization-format.md
        └── 005-thread-safety.md
```

## Configuration Reference

See [config.example.json](config.example.json) for full options.

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `grpc` | `enabled` | `false` | Enable gRPC streaming |
| `grpc` | `bind_address` | `0.0.0.0:10000` | gRPC server address |
| `grpc` | `max_connections` | `100` | Max concurrent clients |
| `kafka` | `enabled` | `false` | Enable Kafka publishing |
| `kafka` | `brokers` | - | Kafka broker list |
| `kafka` | `topic` | - | Default topic |
| `kafka.producer` | `acks` | `all` | Required by the idempotent producer |
| `kafka.producer` | `compression` | `zstd` | Compression codec |
| `plugin` | `channel_capacity` | `100000` | Internal buffer size |
| `metrics` | `enabled` | `true` | Enable Prometheus |
| `metrics` | `bind_address` | `127.0.0.1:9090` | Metrics endpoint |

## Metrics

> **There is currently no Prometheus HTTP exporter.** The counters below are
> defined and incremented in-process, but nothing gathers or serves them, so
> `metrics.bind_address` in the config is parsed and then unused. Scraping the
> plugin does not work yet. The counters are listed here because they exist in
> code and are maintained at runtime, not because they are reachable.

All metrics use the `geyser_tap_` prefix:

| Metric | Type | Description |
|--------|------|-------------|
| `updates_received_total` | Counter | Updates from validator |
| `updates_sent_total` | Counter | Updates to sinks |
| `updates_dropped_total` | Counter | Dropped due to backpressure |
| `channel_depth` | Gauge | Current channel utilization |
| `sink_latency_seconds` | Histogram | Sink send latency |
| `grpc_clients` | Gauge | Connected gRPC clients |
| `current_slot` | Gauge | Latest slot processed |

Once an exporter exists, drop rate would be:
```promql
rate(geyser_tap_updates_dropped_total[5m])
/ rate(geyser_tap_updates_received_total[5m])
```

## Development

### Prerequisites

- Rust 1.75.0 exactly (pinned by `rust-toolchain.toml`; see the warning above)
- `protoc` (Protocol Buffers compiler) - required, `crates/proto/build.rs`
  generates the tonic service layer
- `cmake` (for rdkafka)
- A `solana-test-validator` matching the plugin interface version, for
  end-to-end runs

### Build Commands

```bash
# Debug build
cargo build

# Release build (required for validator)
cargo build --release

# Run tests
cargo test

# Run clippy
cargo clippy --all-targets

# Format check
cargo fmt --check
```

### End-to-end run

`cargo test` covers the in-process tests only; it does not exercise the
validator FFI boundary. To check the real path, run a validator with the
plugin and attach the SDK consumer:

```bash
cargo build --release -p geyser-tap-plugin

<path-to-1.18.26>/solana-test-validator \
  --ledger /tmp/gt-ledger \
  --geyser-plugin-config test-config.json &

ss -ltn | grep -E ':10000|:10001'    # both sinks should be listening
cargo run --release -p geyser-tap-sdk --example consume -- http://127.0.0.1:10000 30
```

`docker-compose.yml` provides a Kafka service for exercising the Kafka sink.
That path has not been run yet, and `config/prometheus.yml` is gitignored, so
the compose stack needs it created locally first.

## Deployment Checklist

- [ ] Build with `--release` profile
- [ ] Build with the **exact** rustc the target validator was built with
- [ ] Verify `panic = "unwind"` in Cargo.toml (`abort` disables panic isolation)
- [ ] Test with validator on devnet first
- [ ] Note: metrics alerting is not possible yet - no exporter
- [ ] Set appropriate `channel_capacity` for your load
- [ ] Monitor memory usage (should stay bounded)
- [ ] Test graceful shutdown with `on_unload`

## Troubleshooting

### Validator segfaults on startup

```
Starting GeyserPluginService from config files: [...]
Segmentation fault (core dumped)
```

This is the most common failure and it is almost never a symbol problem.
Check, in order:

1. **rustc mismatch.** The plugin must be built with the same rustc as the
   validator. Find the validator's:
   `strings <validator-binary> | grep -o '/rustc/[a-f0-9]*'`
2. **Wrong validator.** Confirm the binary you are running matches
   `solana-geyser-plugin-interface`, not just whatever is on `PATH`.
3. **Invalid config.** Any error returned from `on_load` can surface as a
   segfault rather than a message, because the error is a boxed trait object
   whose vtable lives in the library the validator has just unloaded. A
   `kafka` block with `enabled: false` but no `brokers` field does this.

Full write-up in [BLOCKERS.md](BLOCKERS.md).

### Plugin appears loaded but produces no logs

Expected. `solana_logger` installs the global `log` logger during validator
startup, so the plugin's `tracing_subscriber` init is a no-op and all plugin
`tracing` output is discarded.

### Updates being dropped

There is no metrics endpoint to check yet (see [Metrics](#metrics)). If you
suspect drops:
1. Increase `channel_capacity` (uses more memory)
2. Add more Kafka brokers / gRPC clients
3. Check if downstream is healthy

### High memory usage

Memory is bounded by:
- Channel: `channel_capacity * ~1KB`
- Per-client gRPC buffers: `max_connections * send_buffer_size * ~1KB`
- Kafka queue: `queue.buffering.max.kbytes`

Reduce these if memory is constrained.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests
4. Ensure CI passes
5. Open a pull request

## License

MIT License. See [LICENSE](LICENSE).

## Acknowledgments

- [Solana Labs](https://github.com/solana-labs/solana) for the Geyser plugin interface
- [Jito Labs](https://github.com/jito-foundation) for geyser-grpc inspiration
- [Triton One](https://github.com/rpcpool) for yellowstone-grpc reference
