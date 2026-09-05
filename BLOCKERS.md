# Blockers for geyser-tap End-to-End Testing

## RESOLVED: Segfault when loading plugin into solana-test-validator

**Status: RESOLVED (2026-09-05).** The plugin now loads and runs for the full
duration of a `solana-test-validator` session without crashing.

### Original symptom

```
solana-test-validator --geyser-plugin-config test-config.json
# Segmentation fault (core dumped)
```

The validator died immediately after:

```
[INFO solana_geyser_plugin_manager::geyser_plugin_service] Starting GeyserPluginService from config files: [...]
```

### How it was diagnosed

The earlier notes guessed at the cause. It was located instead by adding
allocation-free `libc::write(2, ...)` markers to `_create_plugin` and each
step of `on_load`, then reading them back from the ledger's `validator.log`
(the validator redirects its own stderr there, which is why the markers were
invisible in the terminal at first).

The markers showed the crash landed between "config file read" and
"config parsed", i.e. inside `serde_json::from_str::<PluginConfig>()`.

Loading the same `.so` from a small C `dlopen` harness outside the validator
showed that call returning a clean `Err`, not crashing:

```
failed to parse config: missing field `brokers` at line 24 column 3
```

### Root cause

**Two independent faults, both required to be fixed.**

**1. Invalid test config → validator crashes on the error-return path.**

`test-config.json` carried `"kafka": { "enabled": false }`, but `brokers` is a
required field of `KafkaConfig`, so deserialization failed even though the
sink was disabled. `on_load` then returned
`GeyserPluginError::Custom(Box<dyn Error + Send + Sync>)` — a trait object
whose vtable lives inside the plugin's `.so`. The validator's load-failure
path drops the `Library` and then touches that error, dereferencing a vtable
in an unloaded object. Any `on_load` error surfaces as a segfault rather than
a readable message, which is what made this so hard to see.

Fix: `kafka` is `Option<KafkaConfig>`, so the block was removed from
`test-config.json` rather than half-populated.

**2. rustc version mismatch.**

`_create_plugin` returns `*mut dyn GeyserPlugin` — a Rust trait-object fat
pointer. Rust vtable layout is not ABI-stable across compiler versions, so the
plugin must be built with the same rustc as the validator.

`solana-test-validator 1.18.26` (commit `d9f20e95`) is built with
**rustc 1.75.0** (rustc commit `82e1608dfa6e0b5569232559e3d385fea5a93112`,
readable via `strings` on the validator binary). The plugin was being built
with 1.88.0.

Fix: `rust-toolchain.toml` pins `channel = "1.75"`.

### Both fixes verified necessary

Controlled runs against the *same corrected* `test-config.json`:

| Plugin built with | Validator 1.18.26 result |
|---|---|
| rustc 1.88.0 | Segmentation fault (exit 139) |
| rustc 1.75.0 | Ran the full session, clean (exit 124 = timeout) |

So the config fix alone is not sufficient; the toolchain pin is load-bearing.

### Secondary finding: wrong validator on PATH

`solana-test-validator` on `PATH` resolves to **Agave 4.0.2**
(`~/.local/share/solana/install/`), whose plugin interface is a different
major version from the pinned `solana-geyser-plugin-interface 1.18`. The
matching 1.18.26 validator is at `/root/solana-release/bin/`. Use the
absolute path; the earlier report of "Solana 1.18.26" was reading
`version.yml`, not the binary actually being run.

### Build note: MSRV pinning

Building under rustc 1.75 required holding several transitive dependencies
below their current releases (they need edition2024 or a newer MSRV):
`cpufeatures`, `blake3`, `async-compression`, `enum-iterator-derive` and
others resolved via `CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback`.

Those pins live only in `Cargo.lock`, which is therefore **committed**.
Deleting it reintroduces the build failure.

### Build note: toolchain switches do not relink

`cargo build` keeps a separate fingerprint per toolchain but writes to the
same `target/release/` path. After building with a different toolchain, a
rebuild can report `Finished` in under a second while leaving the *other*
toolchain's `.so` in place. Remove the artifact (or use a separate
`CARGO_TARGET_DIR`) when switching, or you will test the wrong binary — this
produced one false result during this investigation.

## RESOLVED: No data egress from the sinks

**Status: RESOLVED (2026-09-05).** Consumers now receive data over both the
gRPC and websocket sinks. Three separate defects were involved:

1. **No real gRPC service existed.** `crates/proto` hand-wrote the prost
   message types and `sink-grpc/src/service.rs` hand-wrote a `GeyserStream`
   trait plus a `GeyserStreamServer` struct that implemented none of tonic's
   routing. They looked like a service but could not be mounted on a tonic
   server, which is why `GrpcSink::start()` was a stub -- there was nothing to
   serve. `crates/proto/build.rs` now runs tonic-build to generate the real
   client and server, mapping every message onto the existing hand-written
   type via `extern_path` so no message is defined twice and the wire format
   is unchanged. This adds a build-time dependency on `protoc`.

2. **`Sink` had no lifecycle hook.** `create_sinks()` returned
   `Vec<Box<dyn Sink>>` and the worker went straight to `send()`.
   `GrpcSink::start` and `WsSink::start` existed only as inherent methods that
   nothing called, so even a correct `start()` would never have run. Added
   `Sink::start()` with a no-op default, called for every sink at the top of
   the plugin worker -- inside the runtime, since `create_sinks()` runs on the
   validator thread during `on_load` where there is no reactor.

3. **The websocket server never registered its clients.** It completed the
   handshake and then only read, so `broadcast()` always iterated an empty map
   and a connected client sat silent forever.

Verified against a live validator:

```
$ ss -ltn | grep -E ':10000|:10001'
LISTEN 0      128         127.0.0.1:10000      0.0.0.0:*
LISTEN 0      128         127.0.0.1:10001      0.0.0.0:*
```

gRPC consumer: 5496 updates in 30s across all five update types.
Websocket consumer: 3671 JSON frames. Full output in VERIFICATION.md.

## RESOLVED: catch_unwind was defeated by panic="abort"

**Status: RESOLVED (2026-09-05).** The release profile is now
`panic = "unwind"`, so the `catch_unwind` at the FFI boundary actually
catches. Confirmed on the release artifact itself
(`_Unwind_RaiseException`, `_Unwind_Resume` and `.gcc_except_table` are
present; all absent under `panic="abort"`).

`on_load` is still not wrapped in `catch_panic`.

## RESOLVED: Docker image build

**Status: RESOLVED (2026-09-05).** The Dockerfile pinned `rust:1.82`, which
builds a `.so` that segfaults the validator on load; it is now `rust:1.75`.
That fix was previously reasoned but unverified because Docker Desktop's WSL
integration is disabled. A native daemon works instead.

Two environmental workarounds were needed on this WSL2 kernel
(`7.1.3-microsoft-standard-WSL2`), neither of which is a project problem:

- No `xt_addrtype` module and no bridge netlink support, so `dockerd` cannot
  create `docker0`:
  `Failed to create bridge docker0 via netlink: operation not supported`.
  Run `dockerd --bridge=none --iptables=false --ip6tables=false` and build
  with `docker build --network=host`.
- The build context was the 18GB `target/` directory, which the daemon has to
  receive before the build starts. Added `.dockerignore`; context is now 748KB.

The image builds and its artifact was extracted and loaded into a real
validator: no segfault, both sink ports and the metrics port bound, and a
consumer received 3698 updates across all five update types. The container
artifact is a distinct binary from the host build (md5 `e4027d87...` vs
`d1e3a616...`), so this tested the container toolchain rather than re-testing
the host one.

Only the `builder` stage was built; the slim `runtime` stage was not.

## RESOLVED: Kafka sink verified against a live broker

**Status: RESOLVED (2026-09-05).** Previously "implemented, never run against
a broker". Apache Kafka 3.7.1 was run in KRaft mode (single node, no
ZooKeeper) on localhost:9092.

Running it immediately exposed a bug that code review had not: the producer
sets `enable.idempotence=true`, which librdkafka only permits with
`acks=all`, while the config default and both shipped examples used `"1"`.
Validator startup aborted with `Client creation error: acks must be set to
all when enable.idempotence is true`, so the sink could never have worked as
documented. Fixed by forcing `acks=all` with a warning.

After the fix, messages land on all three topics and decode as
`StreamUpdate`s with real signatures and account pubkeys. Evidence is in
VERIFICATION.md.

Not covered: SASL/SSL (disabled in the build) and multi-broker failover.

## OPEN: Plugin logs never reach the validator log

`solana_logger` installs the global `log` logger during validator startup, so
the plugin's `tracing_subscriber::fmt().try_init()` in `on_load` is a no-op
and every `tracing::info!`/`error!` in the plugin and sinks is discarded. This
made the original segfault far harder to diagnose than it needed to be -- the
only way to get output from inside the plugin was raw `libc::write` to fd 2,
which does land in the ledger's `validator.log`.

Bridging tracing onto the `log` facade, or writing to a plugin-owned file,
would make the plugin observable in production.

## RESOLVED: no Prometheus exporter existed

`crates/common/src/metrics.rs` defines the counters and they are incremented
at runtime, but nothing ever gathers, encodes or serves them. There is no
`TextEncoder`, no `prometheus::gather()` and no HTTP listener anywhere in the
workspace:

```
$ grep -rn 'TextEncoder\|prometheus::gather\|default_registry' --include=*.rs crates/
(no matches)
```

`MetricsConfig::bind_address` was parsed from config and then never read, so a
config that enabled metrics was silently inert and the plugin could not be
scraped.

This had previously been described as "endpoint present, not scraped", which
overstated it: the counters were real, but the exposition side did not exist.

**Status: RESOLVED (2026-09-05).** `common::metrics_server` now serves
`/metrics` (prometheus text exposition), `/health/live` and `/health/ready`,
and has been scraped against a live validator. Evidence in VERIFICATION.md
(FR8).

## RESOLVED: tests/integration.rs never ran

**Status: RESOLVED (2026-09-05).** The file sat at the workspace root, whose
manifest is a virtual workspace with no `[package]`, so it belonged to no
crate, was never compiled, and none of its tests had ever executed despite
being cited as coverage.

Moved to `crates/common/tests/integration.rs`, where cargo builds and runs it;
the suite now reports `Running tests/integration.rs`, 5 passing, and the total
goes from 28 to 33.

`test_plugin_loads` was deleted rather than moved. It spawned a validator and
asserted only that the spawn call returned `Ok`, which holds even when the
plugin segfaults a moment later -- so it would have passed unchanged
throughout the whole period the plugin was crashing every validator that
loaded it. It also ran `solana-test-validator` from `PATH`, which is Agave
4.0.2 here, the wrong interface major version.

### Date
2026-09-05
