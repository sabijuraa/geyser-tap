# Geyser-Tap

A production-grade Solana Geyser plugin for streaming validator updates to gRPC and Kafka.

## What Is This?

Geyser-Tap is a **native Rust plugin** that runs inside the Solana validator process. It receives real-time account updates, transactions, slot notifications, and block metadata, then streams them to downstream consumers via gRPC and/or Kafka.

## Why Is This Hard?

Building a Geyser plugin isn't like building a normal application. It runs inside the validator process, so bugs have severe consequences:

| Challenge | Impact | How We Handle It |
|-----------|--------|------------------|
| **FFI/C ABI boundary** | Panics crash the validator | `catch_unwind` + `panic="abort"` in release |
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
              │  ┌───────────┐  ┌───────────┐     │
              │  │ gRPC Sink │  │ Kafka Sink│     │
              │  └─────┬─────┘  └─────┬─────┘     │
              └────────┼──────────────┼───────────┘
                       │              │
              ┌────────┴──┐    ┌──────┴──────┐
              │ Clients   │    │ Kafka Broker│
              └───────────┘    └─────────────┘
```

## Features

- **gRPC streaming**: Server-push updates to connected clients
- **Kafka publishing**: Durable, replayable event log
- **Per-type filtering**: Subscribe to accounts, transactions, slots, entries
- **Account filters**: Filter by owner program or pubkey
- **Backpressure**: Drops updates gracefully under load
- **Prometheus metrics**: Full observability

## Quick Start

### 1. Build

```bash
# Prerequisites: Rust 1.75+, protoc, cmake (for rdkafka)

git clone https://github.com/your-org/geyser-tap.git
cd geyser-tap

# Build release binary
cargo build --release

# The plugin is at: target/release/libgeyser_tap_plugin.so
```

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

### 4. Connect Clients

**gRPC (Rust):**
```rust
let mut client = GeyserStreamClient::connect("http://localhost:10000").await?;
let request = SubscribeRequest {
    accounts: Some(AccountFilter {
        owners: vec![token_program_id.to_bytes().into()],
        ..Default::default()
    }),
    transactions: Some(TransactionFilter {
        include_votes: false,
        ..Default::default()
    }),
    slots: true,
    ..Default::default()
};
let mut stream = client.subscribe(request).await?.into_inner();
while let Some(update) = stream.message().await? {
    println!("Update: {:?}", update);
}
```

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
│   ├── common/             # Shared types and traits
│   │   └── src/
│   │       ├── sink.rs     # Sink trait
│   │       ├── types.rs    # Update types
│   │       ├── config.rs   # Configuration
│   │       ├── error.rs    # Error types
│   │       └── metrics.rs  # Prometheus metrics
│   └── proto/              # Protobuf definitions
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
| `kafka.producer` | `compression` | `zstd` | Compression codec |
| `plugin` | `channel_capacity` | `100000` | Internal buffer size |
| `metrics` | `enabled` | `true` | Enable Prometheus |
| `metrics` | `bind_address` | `127.0.0.1:9090` | Metrics endpoint |

## Metrics

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

Example Prometheus query:
```promql
# Drop rate over 5 minutes
rate(geyser_tap_updates_dropped_total[5m]) 
/ rate(geyser_tap_updates_received_total[5m])
```

## Development

### Prerequisites

- Rust 1.75+
- `protoc` (Protocol Buffers compiler)
- `cmake` (for rdkafka)
- Docker (for integration tests)

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

### Local Testing with Docker

```bash
# Start Kafka
docker-compose up -d kafka

# Run integration tests
cargo test --features integration

# Stop
docker-compose down
```

## Deployment Checklist

- [ ] Build with `--release` profile
- [ ] Verify `panic = "abort"` in Cargo.toml
- [ ] Test with validator on devnet first
- [ ] Configure metrics alerts for `updates_dropped_total > 0`
- [ ] Set appropriate `channel_capacity` for your load
- [ ] Monitor memory usage (should stay bounded)
- [ ] Test graceful shutdown with `on_unload`

## Troubleshooting

### Plugin not loading

```
Error: Failed to load plugin: undefined symbol
```

Ensure Solana version matches the `solana-geyser-plugin-interface` version in Cargo.toml.

### Updates being dropped

Check metrics:
```bash
curl http://localhost:9090/metrics | grep dropped
```

If `geyser_tap_updates_dropped_total` is increasing:
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

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT License. See [LICENSE](LICENSE).

## Acknowledgments

- [Solana Labs](https://github.com/solana-labs/solana) for the Geyser plugin interface
- [Jito Labs](https://github.com/jito-foundation) for geyser-grpc inspiration
- [Triton One](https://github.com/rpcpool) for yellowstone-grpc reference
