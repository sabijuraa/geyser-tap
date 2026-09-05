# geyser-tap Definition of Done Verification

Date: 2026-09-05
Status: PASS with two exceptions (Kafka delivery and the Docker image are
unverified in this environment; nothing else is outstanding)

End-to-end now genuinely works: the plugin loads into
`solana-test-validator 1.18.26`, both the gRPC and websocket sinks bind, and
consumers receive real decoded validator data. Evidence is pasted below rather
than asserted.

## How to reproduce

```sh
cargo build --release -p geyser-tap-plugin          # rustc 1.75, see below
/root/solana-release/bin/solana-test-validator \
    --ledger /tmp/gt-ledger \
    --geyser-plugin-config test-config.json
cargo run --release -p geyser-tap-sdk --example consume -- http://127.0.0.1:10000 30
```

Two environment constraints are load-bearing:

- **Build with rustc 1.75.0.** `_create_plugin` returns `*mut dyn GeyserPlugin`,
  a trait-object fat pointer. Rust vtable layout is not ABI-stable across
  compiler versions, so a plugin built with a different rustc than the
  validator segfaults it on load. Validator 1.18.26 is built with rustc 1.75.0.
- **Use the validator at `/root/solana-release/bin/`.** The
  `solana-test-validator` on `PATH` is Agave 4.0.2, a different plugin
  interface major version.

## FR1: Full GeyserPlugin trait implementation
**Status: PASS**

`crates/plugin/src/ffi.rs` implements `name`, `on_load`, `on_unload`,
`update_account`, `notify_transaction`, `notify_entry`,
`notify_block_metadata` and `update_slot_status`.

## FR2: REAL payload serialization
**Status: PASS**

Verified from the consumer, not by reading the source: the values below are
decoded from bytes that travelled validator -> plugin -> gRPC -> SDK.

```
[account] pubkey=SysvarRecentB1ockHashes11111111111111111111 owner=Sysvar1111111111111111111111111111111111111 slot=77 lamports=42706560 data_len=6008 executable=false
[transaction] sig=3iGJmtbMBo9YJq1qzr3hB5Q5ubfcsDnLb1nx4SS7RYJmSLrBawHVB9BG7aytvULtzv5KSM4VB6WgddbS9zKQ2HfN slot=78 index=0 is_vote=true tx_bytes=383
[block_metadata] slot=77 blockhash=DVCzMXpHfKLLEYJxcZ9Hh79jXm8z3vkJA6L8YSg6HmBn block_height=Some(77) block_time=Some(1788593477)
[slot] slot=77 parent=Some(76) status=Processed
[entry] slot=77 index=38 num_hashes=1 executed_txs=0
```

Real base58 pubkeys, a real 383-byte serialized vote transaction, a real
blockhash and 6008 bytes of real sysvar account data.

Known gaps, unchanged: transaction `meta` is always `None` (the status-meta
types do not implement Serialize), and non-legacy versioned transactions fall
back to emitting the message hash.

## FR3: Three egress sinks
**Status: PASS for gRPC and websocket; Kafka UNVERIFIED**

Previously FAIL: `GrpcSink::start()` was a stub that never bound, and the
websocket sink was not wired into the plugin at all.

Three separate defects had to be fixed:

1. `GrpcSink::start()` returned `Ok(())` without binding, because there was no
   real tonic service to serve -- `service.rs` hand-rolled a `GeyserStream`
   trait that implemented none of tonic's routing. `build.rs` now generates
   the real service via tonic-build.
2. `Sink` had no `start()` method, so `create_sinks()` built sinks that were
   never started. Added `Sink::start()`, called by the plugin worker.
3. The websocket server accepted connections but never registered them, so
   `broadcast()` always iterated an empty client map.

Both ports now listen during a live run:

```
$ ss -ltn | grep -E ':10000|:10001|:8899'
LISTEN 0      128         127.0.0.1:10000      0.0.0.0:*
LISTEN 0      128         127.0.0.1:10001      0.0.0.0:*
LISTEN 0      1024          0.0.0.0:8899       0.0.0.0:*
```

Websocket delivery, from a raw websocket client (`101 Switching Protocols`,
then 3671 frames in ~20s):

```
{"type":"entry","slot":57,"data":{"executed_transaction_count":0,"hash":"51MxM2PBRQwEcqvUKv75nScYShrSneHdGEtehsivz7Cr","index":52,"num_hashes":1}}
FRAME COUNTS BY TYPE: {'type': 3671}
```

**Kafka is not verified.** The producer is implemented but no broker was run
against it, so no message was ever confirmed delivered. Do not read FR3 as
"all three sinks proven".

Also still missing: the websocket sink has no per-client server-side
filtering; every connected client gets the full configured stream.

## FR4: Subscription/filtering engine
**Status: PASS**

Verified behaviourally: subscribing without `.entries()` returned 0 entry
updates; adding `.entries()` to the same subscription returned 3093 in a
comparable window. Vote filtering likewise gates on `include_votes`.

## FR5: Rust client SDK
**Status: PASS** (was incorrectly marked PASS on 2026-09-04 while stubbed)

`GeyserClient` was a stub: `subscribe()` returned a receiver whose sender was
immediately dropped, so the stream was empty forever, and `ping()` echoed the
caller's own clock without contacting the server. Both are now real RPCs
against the generated client. The consumer output quoted throughout this
document is produced by this SDK.

```
ping ok: server_timestamp_ns=1788593478369986857 latency_ns=1222998
```

## FR6: FFI safety with catch_unwind
**Status: PASS** (was incorrectly marked PASS on 2026-09-04 while non-functional)

The release profile set `panic = "abort"`, under which `catch_unwind` cannot
catch anything -- so the panic-isolation claim was false in exactly the build
that ships. The profile is now `panic = "unwind"`.

Unit tests (`callback_panic_is_caught_and_returned_as_error`,
`catch_panic_passes_through_success`) pass. Those run under the test profile,
which unwinds regardless, so the release artifact was checked directly:

```
$ nm -D target/release/libgeyser_tap_plugin.so | grep -i unwind
                 U _Unwind_RaiseException@GCC_3.0
                 U _Unwind_Resume@GCC_3.0
$ readelf -S target/release/libgeyser_tap_plugin.so | grep gcc_except
  [17] .gcc_except_table PROGBITS
```

Both are absent from a `panic="abort"` build.

`on_load` is still not wrapped in `catch_panic`; a panic there aborts.

## FR7: Backpressure-aware bounded channels
**Status: PASS**

Bounded crossbeam channel with `try_send` and drop-on-full
(`crates/plugin/src/runtime.rs`), so a slow sink cannot stall the validator.
Not stress-tested to the point of actually forcing drops.

## FR8: Observable with Prometheus metrics
**Status: PARTIAL**

The counters exist and are recorded. The metrics HTTP endpoint was not
scraped during these runs, so the exposition side is unverified.

## NFR: Quality
**Status: PASS**

`cargo test --release`: 28 tests pass across all crates.

Note: `tests/integration.rs` sits at the workspace root, which is a virtual
manifest with no `[package]`, so it belongs to no crate and never compiles or
runs. Those five tests have never executed. They are not counted above.

## A: End-to-end consumer test
**Status: PASS**

A live validator, the plugin loaded into it, and the SDK consumer over gRPC:

```
connecting to http://127.0.0.1:10000 ...
connected
ping ok: server_timestamp_ns=1788593478369986857 latency_ns=1222998
subscribed; reading for 30s

[entry] slot=77 index=38 num_hashes=1 executed_txs=0
[account] pubkey=SysvarRecentB1ockHashes11111111111111111111 owner=Sysvar1111111111111111111111111111111111111 slot=77 lamports=42706560 data_len=6008 executable=false
[block_metadata] slot=77 blockhash=DVCzMXpHfKLLEYJxcZ9Hh79jXm8z3vkJA6L8YSg6HmBn block_height=Some(77) block_time=Some(1788593477)
[slot] slot=77 parent=Some(76) status=Processed
[transaction] sig=3iGJmtbMBo9YJq1qzr3hB5Q5ubfcsDnLb1nx4SS7RYJmSLrBawHVB9BG7aytvULtzv5KSM4VB6WgddbS9zKQ2HfN slot=78 index=0 is_vote=true tx_bytes=383

=== RESULT ===
total updates received: 5496
  account: 503
  block_metadata: 71
  entry: 4635
  slot: 213
  transaction: 74

PASS: consumer received real data over gRPC
```

All five update types, 5496 updates in 30s. The 74 transactions are the
validator's own votes plus three `solana airdrop` transactions issued during
the run.

## B-I: Build artifacts, Docker, Docs
**Status: PARTIAL**

- Dockerfile: repinned from Rust 1.82 to 1.75 so it stops producing a
  segfaulting `.so`. **The image build was not run** -- the Docker daemon is
  unreachable here (Docker Desktop WSL integration disabled), so this fix is
  reasoned, not verified.
- docker-compose.yml, ADRs, SYSTEM_DESIGN.md, example configs: present.
  `config/prometheus.yml` is gitignored, so the compose stack needs it created
  locally.

## Summary

| Item | Status |
|------|--------|
| FR1: GeyserPlugin trait | PASS |
| FR2: Real payloads | PASS - verified from consumer output |
| FR3: Three sinks | PASS for gRPC + ws; Kafka UNVERIFIED |
| FR4: Filtering | PASS |
| FR5: SDK | PASS - was a stub, now real |
| FR6: FFI safety | PASS - was defeated by panic=abort |
| FR7: Backpressure | PASS |
| FR8: Metrics | PARTIAL - endpoint not scraped |
| Clippy / tests | PASS - 28 tests |
| E2E consumer test | PASS - 5496 updates, all five types |
| Docker image | UNVERIFIED - no daemon |

Outstanding, in rough priority order:

1. Kafka delivery has never been exercised against a broker.
2. The Docker image build is unverified.
3. `tests/integration.rs` belongs to no crate and never runs.
4. Plugin `tracing` output never reaches the validator log: `solana_logger`
   installs the global `log` logger first, so the plugin's
   `tracing_subscriber` init is a no-op. All plugin-side diagnostics are
   currently invisible in `validator.log`.
5. Websocket sink has no per-client filtering.
6. `on_load` is not panic-wrapped.
7. Transaction `meta` is always `None`.
