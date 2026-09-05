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

## OPEN: No data egress from the sinks

The plugin loads and ingests, but nothing is served to consumers yet.

- `GrpcSink::start()` (`crates/sink-grpc/src/lib.rs:93`) is a stub — it logs
  "Starting gRPC server" and returns `Ok(())` without binding. Port 10000
  never listens.
- The websocket sink is implemented and does bind
  (`crates/sink-ws/src/server.rs:86`), but it is not wired into the plugin:
  `geyser-tap-sink-ws` is not a dependency of `crates/plugin`, and
  `create_sinks()` (`crates/plugin/src/ffi.rs:140`) only ever constructs the
  gRPC and Kafka sinks. The `ws` config block is parsed and validated, then
  ignored.

Confirmed by `ss -ltn` during a live validator run: only the validator's own
8899 was listening; 10000 and 10001 were absent.

### Date
2026-09-05
