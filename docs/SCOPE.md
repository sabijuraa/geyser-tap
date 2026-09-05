# geyser-tap — Scope & Definition of Done

The authoritative scope for geyser-tap. This is the contract. A build is "complete" only when every requirement below is verifiably met against the actual code and a running system — not against a summary. Drop this at `docs/SCOPE.md`.

## What it is

A production Solana Geyser plugin plus its egress infrastructure. It runs inside a validator process, receives account/transaction/slot/block/entry notifications, filters them per subscriber, and streams them out over gRPC, Kafka, and websocket to downstream consumers, without ever destabilizing the validator.

## Goals

- Stream real, complete Solana ledger data (accounts, transactions, slots, block metadata, entries) out of a validator to external consumers with low added latency.
- Never crash, stall, or slow the validator, regardless of consumer behavior or plugin errors.
- Let consumers subscribe to only the data they need (by account, owner program, commitment) rather than the full firehose.
- Be operable in production: observable, configurable, restartable, documented.

## Non-goals (explicitly out of scope — do not build these)

- Not an indexer or database. It streams; persisting/indexing is slot-stream's job (a downstream consumer).
- Not a query/analytics layer. No historical queries, no aggregation (that's chain-lens).
- Not a validator or RPC node. It plugs into an existing validator.
- No historical backfill. It streams live data from the point it's loaded.
- No auth/multi-tenant billing system. TLS and basic access control only.

## Functional requirements (what it must DO)

FR1. Implement the full GeyserPlugin trait: on_load, on_unload, update_account, notify_transaction, update_slot_status, notify_block_metadata, notify_entry, notify_transaction (with all commitment levels).
FR2. Serialize and emit REAL payloads for every notification type. Transaction and entry data must be actual, complete, correctly-encoded bytes — not empty, not placeholder. (This is a known current gap: notify_transaction/notify_entry currently emit empty Bytes.)
FR3. Egress sinks: a gRPC streaming server (tonic, with a defined .proto), a Kafka producer, and a websocket/TCP sink. All three functional and selectable via config.
FR4. Subscription/filtering engine: consumers subscribe with filters (account pubkey, owner program, commitment level, notification type); the plugin evaluates filters server-side and only sends matching data.
FR5. Client SDK (Rust) that lets a consumer subscribe and receive a typed stream ergonomically.
FR6. Config-driven: sinks, filters, endpoints, credentials, and tuning all come from a config file; documented format.
FR7. Backpressure: a slow or stalled consumer must not block the validator hot path. Define and implement the policy (bounded buffers, drop-oldest or disconnect-slow-consumer) and make it explicit.
FR8. Graceful lifecycle: clean startup on on_load, clean shutdown/flush on on_unload, correct behaviour across validator/slot restarts.

## Non-functional requirements (how well it must do it)

NFR1. Safety: the plugin runs across the C ABI (FFI). No panic may propagate into the validator — all callback boundaries wrapped in catch_unwind; all unsafe blocks documented with their invariants. A plugin bug must degrade the plugin, never the node.
NFR2. Performance: minimal added latency on the validator hot path; hot-path serialization avoids unnecessary allocation/copying (zero-copy where it matters). Provide a benchmark of hot-path cost.
NFR3. Scalability: handle high slot/transaction throughput and multiple concurrent consumers with server-side fan-out; per-consumer backpressure isolates a slow consumer from others. State the tested throughput and consumer count.
NFR4. Reliability: bounded resource use (no unbounded queues/memory growth); defined behaviour under overload; recovers cleanly from consumer disconnects and sink failures.
NFR5. Observability: Prometheus metrics (slots/txns processed, per-sink throughput, dropped/lagged messages, consumer count), structured tracing, health/readiness endpoints.
NFR6. Code quality: idiomatic Rust, rustfmt + clippy clean with warnings denied, no dead code, real error handling (no unwrap/expect on runtime paths), errors typed and handled.
NFR7. Operability: Dockerfile + docker-compose that runs a test-validator, the plugin, and a sample consumer; documented deployment into a real validator.

## System design (the shape it must have)

Workspace crates:
- proto — the gRPC/protobuf schema (streaming API, subscription messages).
- common — shared types (AccountUpdate, TransactionUpdate, SlotUpdate, BlockMetadataUpdate, EntryUpdate), config, error types, metrics.
- plugin — the GeyserPlugin trait impl; the FFI boundary; the filter evaluation; the fan-out to sinks; backpressure enforcement.
- sink-grpc — tonic streaming server, subscription management, per-consumer send with backpressure.
- sink-kafka — Kafka producer.
- sink-ws — websocket/TCP sink.
- sdk — Rust client SDK over the gRPC API.

Data flow: validator callback → plugin (wrap in catch_unwind) → serialize real payload → evaluate subscriber filters → fan out to matching consumers across sinks → per-consumer bounded channel enforces backpressure. Config selects active sinks and default filters. Metrics/tracing throughout.

## Testing requirements

- Unit tests per crate (filters, serialization correctness, backpressure policy, config parsing).
- Integration test against a real solana-test-validator: load the plugin, run a sample consumer, assert it receives REAL non-empty transaction/account/slot data matching what the validator produced.
- Hard-case tests: slow consumer triggers backpressure without stalling others; consumer disconnect handled cleanly; validator restart handled; malformed subscription rejected; sink failure isolated.
- FFI-safety test: a panic inside a callback is caught and does not propagate.
- A load/soak test and a hot-path benchmark (criterion).
- Report real coverage numbers on the critical paths (serialization, filtering, backpressure, FFI boundary).

## Documentation requirements

Written in plain, senior-engineer prose (no AI tells — no overused em-dashes, no "not just X but Y", no over-hedging, no bullet-everything, no marketing tone):
- README: what it is, the problem it solves, how to build/run, config, deploy.
- SYSTEM_DESIGN.md: plugin lifecycle, the FFI safety model, the fan-out + backpressure design, the filtering architecture, the sink abstraction, trade-offs.
- ADRs: gRPC + Kafka + ws choice, the backpressure policy, the zero-copy/serialization decision, the panic-isolation strategy.
- Deployment guide: loading into a real validator.

## DEFINITION OF DONE — all must be TRUE and VERIFIED

A. Every FR (1–8) implemented and demonstrated, with FR2 proven: a consumer receives real, non-empty, correctly-decoded transaction/account/slot/entry data end-to-end.
B. Every NFR (1–7) met: catch_unwind at all FFI boundaries; documented unsafe; clippy clean (warnings denied); metrics/tracing/health present; bounded resources; benchmark exists.
C. All three sinks (gRPC, Kafka, ws) functional and config-selectable.
D. Subscription/filtering works (a filtered subscription receives only matching data — demonstrated).
E. Client SDK works (a consumer using the SDK receives a real stream — demonstrated).
F. Test suite passes, includes the integration test vs test-validator and all hard-case tests, with real coverage numbers reported.
G. Docs (README, SYSTEM_DESIGN, ADRs, deployment guide) exist, are accurate to the code, and read as human-written.
H. Dockerfile + compose run the full demo (test-validator + plugin + consumer) and it works.
I. cargo build and cargo clippy are clean; no stubbed/placeholder/"for now" comments remain in the shipped paths.
