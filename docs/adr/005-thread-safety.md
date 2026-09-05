# ADR-005: Thread Safety Model

## Status

Accepted

## Context

The Solana validator calls Geyser plugin methods from multiple threads:

- **Banking threads** (many): Process transactions, call `update_account`, `notify_transaction`
- **Replay thread** (one): Replays ledger, calls `update_slot_status`, `notify_entry`, `notify_block_metadata`
- **Startup thread** (one): Initial account loading, calls `update_account` with `is_startup=true`

The plugin must handle concurrent calls safely without:
1. Data races
2. Deadlocks
3. Excessive contention (blocking validator threads)

## Decision

We use Rust's type system to enforce thread safety at compile time, combined with careful synchronization primitive selection for runtime efficiency.

### Principle: Minimize Shared Mutable State

Most plugin state is immutable after `on_load`:
- Configuration
- Sink instances
- Channel endpoints

Only runtime statistics require synchronization.

### State Categories

#### 1. Immutable After Init

```rust
pub struct PluginConfig {
    // All fields are immutable after on_load
    pub grpc: Option<GrpcSinkConfig>,
    pub kafka: Option<KafkaSinkConfig>,
    pub plugin: PluginSettings,
}
```

No synchronization needed - Rust's borrowing rules handle it.

#### 2. Atomically Updated

Counters and gauges use atomic types:

```rust
pub struct PluginStats {
    pub updates_sent: AtomicU64,
    pub updates_dropped: AtomicU64,
}

// In callback (any thread)
stats.updates_sent.fetch_add(1, Ordering::Relaxed);
```

**Why `Relaxed` ordering?**
- These are statistics, not synchronization primitives
- No happens-before relationship needed
- Maximum performance

#### 3. Rarely Changed, Read-Heavy

Plugin state that changes rarely (e.g., during reconfiguration):

```rust
pub struct GeyserTapPlugin {
    // RwLock allows concurrent reads
    state: RwLock<Option<Arc<PluginState>>>,
}

// In callback (hot path)
let state = self.state.read();  // Non-exclusive, fast
if let Some(ref s) = *state {
    s.process(update);
}
```

We use `parking_lot::RwLock` instead of `std::sync::RwLock`:
- No poisoning on panic
- Faster (no syscall for uncontended case)
- Fair scheduling

#### 4. Message Passing

Communication between validator threads and plugin runtime:

```rust
// crossbeam bounded channel
let (tx, rx) = crossbeam::bounded::<EnvelopedUpdate>(100_000);

// Validator thread (producer)
tx.try_send(update)?;

// Plugin worker (consumer)
let update = rx.recv()?;
```

Channel properties:
- **Lock-free**: Uses CAS operations internally
- **Bounded**: Backpressure when full
- **MPSC**: Multiple validator threads -> single worker

### Synchronization Primitive Selection

| Use Case | Primitive | Reason |
|----------|-----------|--------|
| Counters | `AtomicU64` | No contention, cache-friendly |
| Plugin state | `parking_lot::RwLock` | Read-heavy, rare writes |
| Sink state | `parking_lot::Mutex` | Write-heavy (gRPC client list) |
| Thread comms | `crossbeam::channel` | Lock-free, bounded |
| One-time init | `once_cell::Lazy` | Safe lazy initialization |

### Thread Model Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATOR PROCESS                            │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Banking 1   │  │  Banking N   │  │   Replay     │          │
│  │    Thread    │  │    Thread    │  │   Thread     │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         │                 │                 │                   │
│         │ update_account  │ update_account  │ update_slot       │
│         │                 │                 │ notify_entry      │
│         v                 v                 v                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  GEYSER-TAP PLUGIN                      │    │
│  │                                                         │    │
│  │   ┌─────────────────────────────────────────────────┐   │    │
│  │   │  state: RwLock<Option<Arc<PluginState>>>        │   │    │
│  │   │         ▲                                       │   │    │
│  │   │         │ read() - concurrent, non-blocking    │   │    │
│  │   │         │                                       │   │    │
│  │   └─────────┼───────────────────────────────────────┘   │    │
│  │             │                                           │    │
│  │   ┌─────────┴─────────────────────────────────────────┐ │    │
│  │   │  PluginState                                      │ │    │
│  │   │  - filter: UpdateTypeFilter (immutable)           │ │    │
│  │   │  - sender: UpdateSender (Clone, thread-safe)      │ │    │
│  │   │  - stats: AtomicU64 counters                      │ │    │
│  │   └───────────────────────────┬───────────────────────┘ │    │
│  │                               │                         │    │
│  │                               │ try_send() - lock-free  │    │
│  │                               v                         │    │
│  │   ┌─────────────────────────────────────────────────┐   │    │
│  │   │  crossbeam::bounded channel                     │   │    │
│  │   │  Capacity: 100,000                              │   │    │
│  │   └───────────────────────────┬─────────────────────┘   │    │
│  │                               │                         │    │
│  └───────────────────────────────┼─────────────────────────┘    │
│                                  │                              │
└──────────────────────────────────┼──────────────────────────────┘
                                   │
                                   │ recv() - blocks worker
                                   v
┌─────────────────────────────────────────────────────────────────┐
│                    PLUGIN RUNTIME                               │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Worker Thread                                           │   │
│  │  - Receives from channel                                 │   │
│  │  - Dispatches to sinks                                   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Tokio Runtime (4 threads)                               │   │
│  │  - gRPC server                                           │   │
│  │  - Kafka producer                                        │   │
│  │  - Async I/O                                             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Deadlock Prevention

Rules to prevent deadlocks:

1. **Single lock ordering**: Always acquire locks in the same order
2. **No nested locks**: Never hold RwLock while acquiring another
3. **Non-blocking channel ops**: Use `try_send`, never `send` from validator threads
4. **Timeout on runtime shutdown**: Don't wait forever for worker thread

```rust
// GOOD: Non-blocking
match tx.try_send(update) {
    Ok(()) => { /* sent */ }
    Err(TrySendError::Full(_)) => { /* drop */ }
}

// BAD: Could block validator
tx.send(update).unwrap();  // NEVER in callback
```

### Send + Sync Requirements

All plugin types implement `Send + Sync`:

```rust
// Compile-time enforcement
fn assert_thread_safe<T: Send + Sync>() {}

#[test]
fn plugin_is_thread_safe() {
    assert_thread_safe::<GeyserTapPlugin>();
    assert_thread_safe::<PluginState>();
    assert_thread_safe::<GrpcSink>();
    assert_thread_safe::<KafkaSink>();
}
```

## Consequences

### Positive

- **Compile-time safety**: Rust prevents data races
- **Low contention**: Lock-free where possible
- **No deadlocks**: Simple, documented lock ordering
- **Predictable performance**: No blocking in hot path

### Negative

- **Complexity**: Multiple synchronization primitives
- **Learning curve**: Understanding ownership for thread safety
- **Testing difficulty**: Race conditions hard to test

### Monitoring

Thread contention is observable via metrics:

```rust
// Track time spent waiting for locks (if needed)
let start = Instant::now();
let guard = self.state.read();
metrics::lock_wait_time.observe(start.elapsed());
```

## Alternatives Considered

### Alternative 1: Single-Threaded Plugin

Queue all callbacks to a single plugin thread.

**Rejected**: Would bottleneck validator throughput.

### Alternative 2: Lock-Free Everything

Use only atomics and lock-free data structures.

**Rejected**: Over-engineered for actual contention level. RwLock with read-heavy pattern is simpler and fast enough.

### Alternative 3: Actor Model (Actix)

Use actor framework for message passing.

**Rejected**: Adds dependency and complexity. Simple channels suffice.

## References

- [Rustonomicon: Send and Sync](https://doc.rust-lang.org/nomicon/send-and-sync.html)
- [parking_lot Crate](https://docs.rs/parking_lot)
- [crossbeam-channel](https://docs.rs/crossbeam-channel)
- [Rust Atomics and Locks](https://marabos.nl/atomics/) (Mara Bos)
