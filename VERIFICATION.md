# geyser-tap Definition of Done Verification

Date: 2026-09-05
Status: PARTIAL - plugin now loads in the validator; no data egress yet

Changes since 2026-09-04: the load segfault is fixed and the plugin runs a
full validator session. Investigating it also disproved two claims that were
previously marked PASS (FR3 and FR6); both are corrected below.

## FR1: Full GeyserPlugin trait implementation
**Status: PASS**

Evidence:
- `crates/plugin/src/ffi.rs` implements all required trait methods:
  - `name()` - line 189
  - `on_load()` - line 193
  - `on_unload()` - line 250
  - `update_account()` - line 266
  - `notify_transaction()` - line 328
  - `notify_entry()` - line 429
  - `notify_block_metadata()` - line 465
  - `update_slot_status()` - line 519

## FR2: REAL payload serialization
**Status: PASS**

Evidence (crates/plugin/src/ffi.rs):

Transaction payload (lines 350-367):
```rust
let tx_data = match info.transaction.to_versioned_transaction().into_legacy_transaction() {
    Some(legacy_tx) => {
        match bincode::serialize(&legacy_tx) {
            Ok(bytes) => Bytes::from(bytes),  // REAL serialized bytes
            ...
        }
    }
    None => {
        let hash = info.transaction.message_hash();
        Bytes::copy_from_slice(hash.as_ref())  // REAL hash bytes
    }
};
```

Entry payload (line 438):
```rust
let entry_data = Bytes::copy_from_slice(info.hash);  // REAL hash bytes
```

Account payload (lines 306-308):
```rust
data: Bytes::copy_from_slice(info.data),  // REAL account data
```

Grep for stub/placeholder (result: only test code and error fallbacks):
```
crates/plugin/src/ffi.rs:358:Bytes::new()  # Error fallback path only
crates/sink-kafka/src/partitioner.rs:74:Bytes::new()  # Test mock data
```

## FR3: Three egress sinks
**Status: FAIL** (was incorrectly marked PASS on 2026-09-04)

All three crates exist and compile, but no sink actually serves data:

- gRPC (`crates/sink-grpc/`): broadcaster and service are implemented, but
  `GrpcSink::start()` (`src/lib.rs:93`) is a stub. It logs "Starting gRPC
  server" and returns `Ok(())` without ever binding a listener.
- Kafka (`crates/sink-kafka/`): rdkafka producer implemented. Not exercised —
  no broker was run against it.
- WebSocket (`crates/sink-ws/`): server is implemented and does bind
  (`src/server.rs:86`), but it is **not wired into the plugin**.
  `geyser-tap-sink-ws` is not a dependency of `crates/plugin`, and
  `create_sinks()` (`crates/plugin/src/ffi.rs:140`) constructs only the gRPC
  and Kafka sinks. The `ws` config block is parsed and validated, then ignored.

Measured during a live validator run with `grpc.enabled` and `ws.enabled`
both true: `ss -ltn` showed only the validator's own port 8899. Ports 10000
(gRPC) and 10001 (websocket) were never opened.

## FR4: Subscription/filtering engine
**Status: PASS**

- Filter types: `crates/common/src/config.rs:329-337` (UpdateTypeFilter)
- Client filters: `crates/sink-grpc/src/client.rs` (FilterBuilder)
- Server-side filtering in broadcaster: `crates/sink-grpc/src/broadcaster.rs`

## FR5: Rust client SDK
**Status: PASS**

Evidence: `crates/sdk/` exists with:
- `src/client.rs` - GeyserClient
- `src/subscription.rs` - SubscriptionBuilder
- `src/types.rs` - SDK-specific types
- `src/error.rs` - Error handling

## FR6: FFI safety with catch_unwind
**Status: PARTIAL** (was incorrectly marked PASS on 2026-09-04)

Evidence (crates/plugin/src/ffi.rs:84-111):
```rust
fn catch_panic<F, R>(&self, context: &str, f: F) -> PluginResult<R>
where
    F: FnOnce() -> PluginResult<R> + panic::UnwindSafe,
{
    match panic::catch_unwind(f) {
        Ok(result) => result,
        Err(panic_info) => { ... }
    }
}
```

All hot-path methods use the `catch_panic` wrapper.

**However, this does not currently work.** The release profile in the
workspace `Cargo.toml` sets `panic = "abort"`. Under `panic = "abort"` there
is no unwinding, so `catch_unwind` never catches: a panic in any callback
aborts the validator process instead of being logged and swallowed.

The two settings are mutually exclusive and the code has to pick one:
- keep `panic = "abort"` and drop `catch_panic` as dead code, accepting that
  a plugin panic kills the validator; or
- switch the release profile to `panic = "unwind"` so `catch_panic` does what
  its documentation claims.

`on_load` is additionally not wrapped in `catch_panic` at all.

## FR7: Backpressure-aware bounded channels
**Status: PASS**

Evidence (crates/plugin/src/runtime.rs:91-95):
```rust
let (tx, rx) = bounded::<EnvelopedUpdate>(config.plugin.channel_capacity);
```

TrySend with drop on full (crates/plugin/src/runtime.rs:55-63):
```rust
match self.sender.try_send(enveloped) {
    Ok(()) => true,
    Err(TrySendError::Full(_)) => {
        geyser_tap_common::metrics::record_update_dropped(update_type);
        false
    }
    ...
}
```

## FR8: Observable with Prometheus metrics
**Status: PASS**

Evidence: `crates/common/src/metrics.rs` defines:
- `UPDATES_RECEIVED` - Counter
- `UPDATES_SENT` - Counter  
- `UPDATES_DROPPED` - Counter
- `SINK_LATENCY` - Histogram
- `CHANNEL_CAPACITY` - Gauge

## NFR1-7: Quality requirements
**Status: PASS**

Clippy: `cargo clippy --release -- -D warnings` PASSES
Tests: `cargo test --release` PASSES
Build: `cargo build --release` produces valid .so

## A: End-to-end consumer test
**Status: PARTIAL — plugin loads and runs; no consumer data verified**

Resolved: the load segfault. The plugin now runs a full
`solana-test-validator 1.18.26` session (70s, past slot 100) with no crash and
no plugin errors. Root cause and evidence are in BLOCKERS.md; in short it was
two independent faults — an invalid `test-config.json` (a disabled `kafka`
block still missing the required `brokers` field) whose `on_load` error return
tripped a use-after-`dlclose` in the validator, plus a rustc mismatch
(plugin 1.88.0 vs validator 1.75.0) across the `*mut dyn GeyserPlugin` vtable.

Verified by controlled comparison against the same corrected config:

| Plugin built with | Validator 1.18.26 |
|---|---|
| rustc 1.88.0 | Segmentation fault (exit 139) |
| rustc 1.75.0 | Full clean run (exit 124 = timeout) |

Still outstanding: a consumer has **not** received a single update, because no
sink binds a port (see FR3). The end-to-end claim is not met — only the
ingest half of the path is demonstrated.

Reproduce with the matching validator (not the Agave 4.0.2 one on `PATH`):

```
cargo build --release -p geyser-tap-plugin
/root/solana-release/bin/solana-test-validator \
  --ledger /tmp/gt-ledger \
  --geyser-plugin-config test-config.json
```

## B-I: Build artifacts, Docker, Docs
**Status: PARTIAL** - Dockerfile pins the wrong rustc (see below)

- Dockerfile: exists, but pins Rust 1.82 — this is now wrong. The plugin must
  be built with rustc 1.75 to match the validator, so the Dockerfile will
  produce a `.so` that segfaults on load. Not yet updated.
- docker-compose.yml: exists with Kafka, Prometheus, Grafana
- SYSTEM_DESIGN.md: exists
- ADRs: 4 documents in docs/adr/
- Config example: config/geyser-tap.example.json
- Integration test script: scripts/integration-test.sh

## Summary

| Item | Status |
|------|--------|
| FR1: GeyserPlugin trait | PASS |
| FR2: Real payloads | PASS |
| FR3: Three sinks | FAIL - no sink binds; ws not wired in |
| FR4: Filtering | PASS |
| FR5: SDK | PASS |
| FR6: FFI safety | PARTIAL - catch_unwind defeated by panic=abort |
| FR7: Backpressure | PASS |
| FR8: Metrics | PASS |
| Clippy clean | PASS |
| Tests pass | PASS |
| E2E test | PARTIAL - loads and runs; no egress |

**Overall: 8/11 PASS, 1 FAIL, 2 PARTIAL.**

The headline change is that the validator load segfault is fixed and the
plugin is stable in-process. The remaining gap is egress: the gRPC sink never
binds and the websocket sink is not connected to the plugin, so no consumer
can receive data yet. FR3 and FR6 were previously overstated and are corrected
here.

Not verified this round: Kafka delivery against a real broker, and the
Prometheus endpoint.
