# ADR-003: Backpressure Strategy

## Status

Accepted

## Context

Geyser-Tap must handle situations where downstream sinks cannot keep up with the validator's update rate:

- **Mainnet load**: 1M+ account updates per second during high activity
- **Kafka broker issues**: Network partitions, broker restarts
- **gRPC client issues**: Slow consumers, client disconnects
- **Resource exhaustion**: Memory limits on the plugin

The validator cannot be blocked waiting for sinks. Blocking validator threads would impact consensus and could cause the node to fall behind the cluster.

## Decision

We implement a **drop-on-backpressure** strategy with comprehensive metrics for observability.

### Principle: Validator Stability Over Data Completeness

When faced with the choice between:
1. **Block the validator** waiting for sinks (bad: consensus impact)
2. **Drop updates** and continue (acceptable: data gaps can be filled)

We always choose (2). A Geyser plugin that stalls the validator is worse than one that drops data.

### Implementation Layers

#### Layer 1: Plugin-to-Runtime Channel

```rust
// Bounded channel with 100k capacity
let (tx, rx) = crossbeam::bounded::<EnvelopedUpdate>(100_000);

// In validator callback (sync context)
match tx.try_send(update) {
    Ok(()) => {
        metrics::channel_depth.set(tx.len());
    }
    Err(TrySendError::Full(_)) => {
        // DROP - never block the validator
        metrics::dropped.inc_by(1);
        tracing::warn!("Channel full, dropping update");
    }
    Err(TrySendError::Disconnected(_)) => {
        // Runtime shut down - silently drop
    }
}
```

Key properties:
- **Non-blocking**: `try_send` returns immediately
- **Bounded**: Fixed memory footprint (100k * ~1KB = ~100MB)
- **Observable**: Channel depth metric for alerting

#### Layer 2: gRPC Client Buffers

Each gRPC client gets its own bounded buffer:

```rust
struct ConnectedClient {
    sender: mpsc::Sender<Arc<Update>>,  // Capacity: 10,000
    // ...
}

// In broadcaster
match client.sender.try_send(update.clone()) {
    Ok(()) => sent_count += 1,
    Err(TrySendError::Full(_)) => {
        // DROP for this client only
        client.dropped.inc();
        metrics::grpc_dropped.inc();
    }
    Err(TrySendError::Closed(_)) => {
        // Client disconnected - remove from roster
        disconnected.push(client.id);
    }
}
```

Key properties:
- **Isolation**: Slow client doesn't affect fast clients
- **Per-client metrics**: Know which clients are falling behind
- **Auto-cleanup**: Disconnected clients removed automatically

#### Layer 3: Kafka Producer Queue

librdkafka has internal buffering:

```rust
// Producer config
client_config.set("queue.buffering.max.messages", "100000");
client_config.set("queue.buffering.max.kbytes", "1048576");  // 1GB

// On send
match producer.send(record, Timeout::After(Duration::from_secs(5))) {
    Ok(_) => { /* success */ }
    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
        // DROP - Kafka queue full
        metrics::kafka_dropped.inc();
    }
    // ... other errors
}
```

Key properties:
- **Batching**: Groups messages for efficiency
- **Timeout**: Don't wait forever for queue space
- **Compression**: zstd reduces memory footprint

### Metrics for Observability

```
# High-level
geyser_tap_updates_received_total{type="account|transaction|slot|entry"}
geyser_tap_updates_sent_total{sink="grpc|kafka", type="..."}
geyser_tap_updates_dropped_total{sink="channel|grpc|kafka"}

# Buffer utilization
geyser_tap_channel_depth         # 0-100000
geyser_tap_grpc_client_buffer    # Per-client depth

# Alert when:
# - dropped > 0 sustained
# - channel_depth > 80000 (80% full)
# - client buffer consistently full
```

### Configuration

```json
{
  "plugin": {
    "channel_capacity": 100000,      // Main channel
    "drop_on_backpressure": true     // Explicit opt-in (always true)
  },
  "grpc": {
    "send_buffer_size": 10000        // Per-client buffer
  },
  "kafka": {
    "producer": {
      "queue_buffering_max_messages": 100000
    }
  }
}
```

## Consequences

### Positive

- **Validator stability**: Plugin never blocks consensus
- **Predictable memory**: All buffers are bounded
- **Observability**: Metrics show exactly where drops occur
- **Graceful degradation**: Partial data better than crashed validator

### Negative

- **Data gaps**: Downstream consumers see missing data
- **No replay**: Dropped updates are gone forever
- **Complexity**: Multiple backpressure points to monitor

### Mitigations for Data Gaps

1. **RPC backfill**: Consumers can request missing data via RPC
2. **Bigtable storage**: Cross-reference with validator's Bigtable writes
3. **Slot awareness**: Consumers detect gaps via slot sequence
4. **Alerting**: Dropped metrics trigger immediate investigation

## Alternatives Considered

### Alternative 1: Unbounded Queues

No backpressure - just keep queueing.

**Rejected**: Memory exhaustion would crash the validator, worse outcome.

### Alternative 2: Block on Full

Block validator threads until space available.

**Rejected**: Consensus impact. Validator would fall behind cluster.

### Alternative 3: Spill to Disk

When memory full, write to disk queue.

**Rejected**: Disk I/O latency too high for validator callback path.

### Alternative 4: Sampling

Under pressure, sample 1-in-N updates instead of dropping random ones.

**Considered for future**: Could provide better data quality than random drops. Not implemented in v1.

## References

- [Back Pressure - Reactive Streams](https://www.reactive-streams.org/)
- [Kafka Producer Buffering](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
- [Solana Validator Performance Tuning](https://docs.solana.com/running-validator/validator-start#performance-tuning)
