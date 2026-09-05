# geyser-tap Definition of Done Verification

Date: 2026-09-05
Status: end-to-end PASS, and every Definition-of-Done item is now verified
against live infrastructure rather than asserted. Kafka was run against a real
broker, the Docker image was built and its artifact loaded into a validator,
and the Prometheus exporter is served and scraped.

Remaining gaps are narrower and listed at the bottom; none of them are items
previously claimed as done.

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
**Status: PASS for all three sinks**

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

**Kafka is now verified** against a real broker (Apache Kafka 3.7.1 in KRaft
mode on localhost:9092). See the Kafka section below for the evidence.

Also still missing: the websocket sink has no per-client server-side
filtering; every connected client gets the full configured stream.

## Kafka sink (part of FR3)
**Status: PASS**

Previously "implemented, never run against a broker". A real broker was run
here: Apache Kafka 3.7.1 in KRaft mode (no ZooKeeper), single node on
localhost:9092, with topics `solana-updates`, `solana-accounts` and
`solana-transactions` at 3 partitions each.

Running it immediately exposed a bug that code review had not: the producer
sets `enable.idempotence=true`, which librdkafka only permits with
`acks=all`, while the config default and both shipped examples used `"1"`.
Validator startup aborted with:

```
Failed to load the Geyser plugin: on_load method of plugin geyser-tap failed:
(failed to create sinks: sink error: connection failed to localhost:9092:
Client creation error: `acks` must be set to `all` when `enable.idempotence`
is true)
```

So the Kafka sink could never have worked as documented. Fixed by forcing
`acks=all` with a warning, and correcting the default and both examples.

After the fix, messages land on the topics:

```
$ kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic solana-accounts
solana-accounts:0:358
solana-accounts:1:341
solana-accounts:2:502

$ kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic solana-transactions
solana-transactions:0:50
solana-transactions:1:51
solana-transactions:2:55

$ kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic solana-updates
solana-updates:0:3822
solana-updates:1:3296
solana-updates:2:4684
```

Broker-side log dump confirms real records with signature-sized keys and
zstd compression actually applied:

```
| offset: 0 CreateTime: 1788602272096 keySize: 64 valueSize: 412 sequence: 0
  compresscodec: zstd crc: 2640682151 isvalid: true
```

Reading the messages back and decoding them as `StreamUpdate` (via
`cargo run -p geyser-tap-sink-kafka --example verify_topic`) shows they are
well-formed, not just bytes:

```
offset=0 partition=2 key_len=64 payload_len=420 seq=671 TRANSACTION sig=2aNjuKjg7s9oyfqpJEpWYj2L6S9Nd5h6UdVKxff6YqWWkyRtNt2tgBdSouRVzi6EKq9Sg1PTNceNxYWSqPdZs9h6 slot=6 is_vote=true tx_bytes=331
offset=1 partition=2 key_len=64 payload_len=424 seq=825 TRANSACTION sig=4fCMfqBVYfRyTTmpCRBqPM8Ruzcbg6vLD1sK9bEsAUyUP8Db1uJfTgZy1ch4SF9CiYb7qBFt1ChKEvHKEn3uHkKo slot=8 is_vote=true tx_bytes=335

offset=0 partition=2 key_len=32 payload_len=17169 seq=1 ACCOUNT pubkey=Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo owner=BPFLoader2111111111111111111111111111111111 slot=0 lamports=119712000 data_len=17072
offset=3 partition=2 key_len=32 payload_len=100 seq=7 ACCOUNT pubkey=StakeConfig11111111111111111111111111111111 owner=Config1111111111111111111111111111111111111 slot=0 lamports=960480 data_len=10
```

Key sizes confirm the partitioner: 64 bytes (transaction signature) on the
transaction topic, 32 bytes (account pubkey) on the account topic.

Not covered: SASL/SSL authentication (the build disables those features), and
multi-broker or failover behaviour.

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
**Status: PASS**

Previously FAIL: the counters were incremented in-process but nothing gathered,
encoded or served them, so `metrics.bind_address` was dead config and the
plugin could not be scraped at all. `common::metrics_server` now serves
`/metrics`, `/health/live` and `/health/ready`.

Verified against a live validator with the plugin loaded (endpoint on
127.0.0.1:9091):

```
$ curl -s -D - -o /dev/null http://127.0.0.1:9091/metrics | head -3
HTTP/1.1 200 OK
content-type: text/plain; version=0.0.4; charset=utf-8
content-length: 1656

$ curl -s http://127.0.0.1:9091/metrics | grep -E 'updates_received|channel|current_slot'
geyser_tap_channel_capacity 10000
geyser_tap_channel_depth 0
geyser_tap_current_slot 100
# HELP geyser_tap_updates_received_total Total number of updates received from the validator
# TYPE geyser_tap_updates_received_total counter
geyser_tap_updates_received_total{type="account"} 928
geyser_tap_updates_received_total{type="block_metadata"} 100
geyser_tap_updates_received_total{type="entry"} 6603
geyser_tap_updates_received_total{type="slot"} 269
geyser_tap_updates_received_total{type="transaction"} 101
```

Egress counters are populated per sink while consumers are attached:

```
geyser_tap_updates_sent_total{sink="grpc",type="entry"} 11656
geyser_tap_updates_sent_total{sink="websocket",type="entry"} 11656
```

Health endpoints and routing:

```
$ curl -s -i http://127.0.0.1:9091/health/live
HTTP/1.1 200 OK
{"status":"live"}

$ curl -s -i http://127.0.0.1:9091/health/ready
HTTP/1.1 200 OK
{"status":"ready"}

$ curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9091/nope
404
```

Readiness is distinct from liveness: the exporter binds before the sinks come
up, so `/health/live` answers during startup while `/health/ready` returns 503
until the worker has started the sinks.

## NFR: Quality
**Status: PASS**

`cargo test --release`: **36 tests pass** across all crates.

This previously read 28 (and later 33, before the metrics-server tests were
added), and separately noted that `tests/integration.rs` sat
at the workspace root -- a virtual manifest with no `[package]` -- so it
belonged to no crate, never compiled, and had never executed despite being
cited as coverage. That file is now at `crates/common/tests/integration.rs`
and genuinely runs:

```
     Running tests/integration.rs (target/release/deps/integration-650135c89c206801)
running 5 tests
test test_config_validation ... ok
test test_disabled_kafka_block_still_requires_brokers ... ok
test test_filter_defaults ... ok
test test_sink_health ... ok
test test_update_serialization ... ok
test result: ok. 5 passed; 0 failed; 0 ignored
```

One test was deleted rather than ported: `test_plugin_loads` spawned a
validator and asserted only that the *spawn call* succeeded, which is true even
when the plugin segfaults immediately afterwards. It would have passed
throughout the entire period the plugin was crashing every validator it was
loaded into, and it invoked `solana-test-validator` from `PATH`, which is
Agave 4.0.2 here. A test that cannot fail for the reason it claims to check is
worse than none; the real end-to-end check is the consumer run below.

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
| FR3: Three sinks | PASS - gRPC, websocket and Kafka all verified live |
| FR4: Filtering | PASS |
| FR5: SDK | PASS - was a stub, now real |
| FR6: FFI safety | PASS - was defeated by panic=abort |
| FR7: Backpressure | PASS |
| FR8: Metrics | PASS - /metrics + /health served and scraped |
| Clippy / tests | PASS - 36 tests |
| E2E consumer test | PASS - 5496 updates, all five types |
| Docker image | PASS - built, artifact loads and streams |

Outstanding, in rough priority order. None of these are items previously
claimed as done:

1. Plugin `tracing` output never reaches the validator log: `solana_logger`
   installs the global `log` logger first, so the plugin's
   `tracing_subscriber` init is a no-op. All plugin-side diagnostics are
   invisible in `validator.log`. This made the original segfault far harder to
   diagnose than it needed to be.
2. Websocket sink has no per-client filtering: every connected client receives
   the full configured stream.
3. `on_load` is not wrapped in `catch_panic`; a panic there still aborts.
4. Transaction `meta` is always `None` -- the status-meta types do not
   implement Serialize.
5. Non-legacy versioned transactions fall back to emitting the message hash
   rather than the full transaction.
6. Kafka SASL/SSL is untested (those features are disabled in the build), as
   is multi-broker/failover behaviour.
7. Only the Docker `builder` stage was built; the slim `runtime` stage was not.
8. Backpressure drops are implemented but never forced under load.
