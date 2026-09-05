# geyser-tap

A Solana Geyser plugin that streams live validator data to gRPC, WebSocket and Kafka consumers.

geyser-tap is a native Rust `cdylib` loaded into the validator process via `dlopen`. It receives account writes, transactions, slot transitions, entries and block metadata as the validator produces them, filters them, and fans them out to downstream consumers — without ever blocking a validator thread.

Running inside someone else's validator sets the engineering constraints. A panic, a blocked callback or an unbounded buffer in this code is a node outage, so the design consistently trades completeness for safety: bounded queues, drop-on-full, and panic isolation at every FFI callback.

---

## Verified

Every claim below was measured against live infrastructure, not inferred from the code.

| | |
|---|---|
| **Streaming end-to-end** | Loaded into `solana-test-validator 1.18.26`; a consumer received **5,496 updates in 30s** across all five update types — real base58 pubkeys, a 383-byte serialized vote transaction, real blockhashes |
| **gRPC sink** | tonic server binds and streams; consumed with the bundled Rust SDK |
| **WebSocket sink** | **3,671 JSON frames** received by a raw WebSocket client |
| **Kafka sink** | Produced to a real broker (Kafka 3.7.1, KRaft); messages read back off the topics and decoded as well-formed `StreamUpdate`s |
| **Metrics** | `/metrics` scraped with correct exposition content-type; `/health/live` and `/health/ready` behave independently |
| **FFI panic isolation** | `panic = "unwind"` confirmed present in the shipped artifact, so `catch_unwind` genuinely catches |
| **Container build** | Docker image builds; the `.so` it produces was extracted, loaded into a validator, and streamed data |
| **Tests** | 36 passing across the workspace |

**Not yet exercised:** Kafka SASL/SSL (disabled in the build) and multi-broker failover; sustained load heavy enough to force back-pressure drops; the slim `runtime` Docker stage. This is a reference implementation — it has not carried production traffic.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      SOLANA VALIDATOR                        │
│   Banking threads ─┬─ update_account()  ──┐                  │
│   Replay thread  ──┼─ update_slot_status()┼──▶ geyser-tap    │
│                    └─ notify_transaction()┘                  │
└───────────────────────────────┬──────────────────────────────┘
                                │  filter, serialize, try_send
                    ┌───────────▼───────────┐
                    │    Bounded channel    │  drop-on-full
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │    Plugin runtime     │  (tokio, own threads)
                    │  ┌──────┐┌────┐┌────┐ │
                    │  │ gRPC ││ WS ││Kafka│ │
                    │  └───┬──┘└─┬──┘└──┬─┘ │
                    └──────┼─────┼──────┼───┘
                           ▼     ▼      ▼
                       clients  clients  broker
```

The validator's callbacks are synchronous and must return fast. Everything expensive happens on the other side of a bounded channel, on threads geyser-tap owns. When that channel is full, updates are dropped and counted — never queued without limit, never blocking.

Full design notes: [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md). Decision records: [docs/adr](docs/adr).

---

## Key design decisions

**Panic isolation is real, and it depends on `panic = "unwind"`.**
Every validator callback is wrapped in `catch_unwind`. This only works under `panic = "unwind"`; with `panic = "abort"` there is no unwinding, the wrapper is inert, and a plugin panic aborts the validator outright. The two settings are mutually exclusive. Unwinding never crosses the C-ABI boundary because `catch_unwind` sits immediately inside each `extern "C"` callback.

**The Rust version is an exact requirement, not a minimum.**
`_create_plugin` returns `*mut dyn GeyserPlugin` — a trait-object fat pointer whose vtable layout Rust does not guarantee across compiler versions. A plugin built with a different rustc than the validator segfaults it on load, with no error message. `solana-test-validator 1.18.26` is built with rustc 1.75.0, so `rust-toolchain.toml` and the Dockerfile pin exactly that. `Cargo.lock` is committed and load-bearing: building under 1.75 requires holding several transitive dependencies below their current releases.

**Back-pressure drops rather than blocks.**
A slow consumer must never become a validator problem. The channel between callbacks and sinks is bounded and uses `try_send`; on failure the update is dropped and a counter incremented. Operators who need durability should use the Kafka sink, which has its own.

**Filtering happens as early as possible.**
Plugin-level filters are applied inside the callback, before anything is copied or enqueued. Per-subscriber filters are applied server-side during fan-out, so a narrow subscriber doesn't pay for traffic it would discard.

**Payloads are copied once, then shared.**
Validator-supplied buffers are only valid for the duration of the call, so payloads are serialized where they arrive. They are stored in `bytes::Bytes` so every subsequent clone through the fan-out is a refcount bump rather than a copy.

---

## Quick start

### Build

Requires **Rust 1.75.0** (pinned automatically by `rust-toolchain.toml`), `protoc`, and `cmake` for rdkafka.

```bash
git clone https://github.com/sabijuraa/geyser-tap.git
cd geyser-tap
cargo build --release
# → target/release/libgeyser_tap_plugin.so
```

### Configure

```json
{
  "libpath": "/opt/geyser-tap/libgeyser_tap_plugin.so",

  "grpc": {
    "enabled": true,
    "bind_address": "0.0.0.0:10000",
    "max_connections": 100,
    "filters": {
      "include_votes": false,
      "update_types": {
        "accounts": true,
        "transactions": true,
        "slots": true,
        "entries": false,
        "block_metadata": true
      }
    }
  },

  "plugin": { "channel_capacity": 100000, "worker_threads": 4 },
  "metrics": { "enabled": true, "bind_address": "127.0.0.1:9090" }
}
```

See [config.example.json](config.example.json) for every option, including Kafka and WebSocket.

### Run

```bash
solana-validator --geyser-plugin-config /path/to/config.json  # ...other flags
```

The validator must match `solana-geyser-plugin-interface` (currently 1.18) and have been built with the same rustc. A validator from a different interface major version — Agave 4.x, say — segfaults on load.

### Consume

**Rust, via the bundled SDK:**

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
    println!("{:?}", update?);
}
```

A runnable version:

```bash
cargo run --release -p geyser-tap-sdk --example consume -- http://127.0.0.1:10000 30
```

**WebSocket:** connect to `ws://localhost:10001` and read JSON frames such as `{"type":"entry","slot":57,"data":{…}}`. This sink has no per-client filtering — every client receives the full configured stream.

**Kafka:** consume the configured topics, or check them with the bundled verifier:

```bash
cargo run --release -p geyser-tap-sink-kafka --example verify_topic -- \
    localhost:9092 solana-transactions 5
```

---

## Configuration reference

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `grpc` | `enabled` | `false` | Enable gRPC streaming |
| `grpc` | `bind_address` | `0.0.0.0:10000` | gRPC listen address |
| `grpc` | `max_connections` | `100` | Concurrent subscriber cap |
| `websocket` | `enabled` | `false` | Enable WebSocket streaming |
| `websocket` | `bind_address` | `0.0.0.0:10001` | WebSocket listen address |
| `kafka` | `enabled` | `false` | Enable Kafka publishing |
| `kafka` | `brokers` | — | Broker list (required when enabled) |
| `kafka` | `topic` | — | Default topic |
| `kafka.producer` | `acks` | `all` | Required by the idempotent producer |
| `kafka.producer` | `compression` | `zstd` | Compression codec |
| `plugin` | `channel_capacity` | `100000` | Bounded channel size |
| `plugin` | `worker_threads` | `4` | Runtime worker threads |
| `metrics` | `enabled` | `true` | Serve metrics and health |
| `metrics` | `bind_address` | `127.0.0.1:9090` | Metrics listen address |

A disabled sink still has its config block validated, so `kafka` requires `brokers` even when `enabled` is `false`. Omit the block entirely instead.

---

## Metrics and health

Three endpoints are served on `metrics.bind_address`:

| Path | Purpose |
|------|---------|
| `/metrics` | Prometheus text exposition |
| `/health/live` | 200 once the server is accepting connections |
| `/health/ready` | 200 after sinks have started, 503 before |

Liveness and readiness are deliberately distinct: the exporter binds before the sinks come up, so `/health/live` answers during startup while `/health/ready` stays 503 until traffic can actually be served. If the metrics server itself fails to bind, that is logged and the plugin continues — losing observability should not take down a validator.

All metrics carry the `geyser_tap_` prefix:

| Metric | Type | Description |
|--------|------|-------------|
| `updates_received_total{type}` | Counter | Updates received from the validator |
| `updates_sent_total{sink,type}` | Counter | Updates delivered to a sink |
| `updates_dropped_total{sink}` | Counter | Updates dropped under back-pressure |
| `channel_depth` / `channel_capacity` | Gauge | Bounded channel utilisation |
| `sink_latency_seconds{sink}` | Histogram | Sink send latency |
| `sink_health{sink}` | Gauge | 1 healthy, 0 unhealthy |
| `current_slot` | Gauge | Most recent slot seen |
| `grpc_clients` | Gauge | Connected gRPC subscribers |

```promql
# Drop rate over 5 minutes
rate(geyser_tap_updates_dropped_total[5m])
  / rate(geyser_tap_updates_received_total[5m])
```

---

## Layout

```
crates/
  plugin/        cdylib loaded by the validator: FFI boundary, state, runtime bridge
  common/        update types, Sink trait, config, errors, metrics + exporter
  proto/         protobuf schema; build.rs generates the tonic service layer
  sink-grpc/     tonic server and per-subscriber fan-out
  sink-kafka/    rdkafka producer and partitioning
  sink-ws/       tokio-tungstenite server
  sdk/           Rust client for the gRPC stream
docs/adr/        architecture decision records
```

---

## Development

```bash
cargo build --release      # release build (required for the validator)
cargo test                 # 36 tests
cargo clippy --all-targets
cargo fmt --check
```

`cargo test` covers in-process behaviour only; it does not cross the validator FFI boundary. To exercise the real path, run a matching validator with the plugin and attach a consumer:

```bash
cargo build --release -p geyser-tap-plugin
solana-test-validator --ledger /tmp/gt-ledger --geyser-plugin-config test-config.json &

ss -ltn | grep -E ':10000|:10001'   # sinks listening
cargo run --release -p geyser-tap-sdk --example consume -- http://127.0.0.1:10000 30
```

`docker-compose.yml` brings up Kafka, Prometheus and Grafana for local work. `config/prometheus.yml` is gitignored, so create it before starting the stack.

---

## Troubleshooting

**The validator segfaults on startup.** Almost always one of three things, none of which produce a useful error message:

1. **rustc mismatch** — the plugin must be built with the validator's exact compiler. Read the validator's with `strings <validator-binary> | grep -o '/rustc/[a-f0-9]*'`.
2. **Wrong validator** — confirm the binary matches `solana-geyser-plugin-interface`, not just whatever is on `PATH`.
3. **Invalid config** — an error returned from `on_load` is a boxed trait object whose vtable lives in the library the validator has just unloaded, so a config mistake can surface as a crash. A `kafka` block with `enabled: false` but no `brokers` does exactly this.

**The plugin loads but logs nothing.** Expected. `solana_logger` installs the global `log` logger during validator startup, so the plugin's `tracing_subscriber` initialisation is a no-op and its output is discarded. Use the metrics endpoint to confirm the plugin is live.

**Updates are being dropped.** Check `geyser_tap_updates_dropped_total`. Either raise `channel_capacity` (costs memory), or find the slow sink via `sink_latency_seconds`.

---

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgments

Built against the [Solana](https://github.com/solana-labs/solana) Geyser plugin interface, with reference to [Jito](https://github.com/jito-foundation) and [Triton One's yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc).
