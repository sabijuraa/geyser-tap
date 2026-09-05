# ADR-004: Serialization Format

## Status

Accepted

## Context

Geyser-Tap must serialize validator updates for transmission to downstream sinks. Key requirements:

1. **Performance**: 1M+ messages/second serialization
2. **Size**: Minimize bandwidth for large account data
3. **Compatibility**: Consumers in multiple languages (Rust, Go, Python, TypeScript)
4. **Evolution**: Schema must evolve without breaking consumers
5. **Zero-copy potential**: Avoid unnecessary copying of large byte arrays

## Decision

We use **Protocol Buffers 3** as the wire format, with specific optimizations for zero-copy and performance.

### Wire Format: Protobuf 3

```protobuf
syntax = "proto3";
package geyser;

message StreamUpdate {
  uint64 sequence = 1;
  uint64 timestamp_ns = 2;
  
  oneof payload {
    AccountUpdate account = 10;
    TransactionUpdate transaction = 11;
    SlotUpdate slot = 12;
    EntryUpdate entry = 13;
    BlockMetadataUpdate block_metadata = 14;
  }
}

message AccountUpdate {
  bytes pubkey = 1;      // 32 bytes
  bytes data = 2;        // Variable, up to 10MB
  uint64 slot = 3;
  bytes owner = 4;       // 32 bytes
  uint64 lamports = 5;
  // ...
}
```

### Why Protobuf

| Criterion | Protobuf | JSON | MessagePack | FlatBuffers |
|-----------|----------|------|-------------|-------------|
| Size | Small | Large | Medium | Smallest |
| Speed | Fast | Slow | Fast | Fastest |
| Schema | Yes | No | No | Yes |
| Polyglot | Excellent | Excellent | Good | Good |
| Evolution | Excellent | Poor | Poor | Good |
| Complexity | Medium | Low | Low | High |

Protobuf wins on the combination of **schema evolution** + **polyglot support** + **good performance**.

### Zero-Copy Optimization

Large fields (account data, transactions) use `bytes::Bytes` to avoid copying:

```rust
// In proto crate
tonic_build::configure()
    .bytes([
        "AccountUpdate.data",
        "AccountUpdate.pubkey",
        "AccountUpdate.owner",
        "TransactionUpdate.transaction",
        "TransactionUpdate.signature",
        // ...
    ])
    .compile(&["geyser.proto"], &["."])?;
```

This generates code using `bytes::Bytes` instead of `Vec<u8>`:

```rust
// Generated code
pub struct AccountUpdate {
    pub pubkey: Bytes,  // Not Vec<u8>
    pub data: Bytes,    // Reference-counted, cheap clone
    // ...
}
```

### Memory Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    MEMORY LIFECYCLE                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Validator Callback:                                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  info.data: &[u8]  (validator-owned, temporary)         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                      │
│                          │ Bytes::copy_from_slice()             │
│                          v                                      │
│  Plugin Owned:                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  update.data: Bytes  (plugin-owned, ref-counted)        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                      │
│                          │ Clone (cheap, Arc::clone)            │
│                          v                                      │
│  Sink 1 (gRPC):         Sink 2 (Kafka):                        │
│  ┌──────────────────┐   ┌──────────────────┐                   │
│  │  update.data     │   │  update.data     │                   │
│  │  (same Bytes)    │   │  (same Bytes)    │                   │
│  └──────────────────┘   └──────────────────┘                   │
│                          │                                      │
│                          │ Protobuf encode (copies to output)   │
│                          v                                      │
│  Wire:                                                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  [varint][bytes...]                                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

Copy count:
1. Validator -> Plugin: **1 copy** (unavoidable, validator owns original)
2. Plugin -> Sinks: **0 copies** (Bytes is Arc-based)
3. Sink -> Wire: **1 copy** (encoding)

Total: **2 copies** vs naive approach of 4+ copies.

### Envelope Structure

Every message includes metadata for ordering and debugging:

```protobuf
message StreamUpdate {
  // Monotonically increasing, for detecting gaps
  uint64 sequence = 1;
  
  // When plugin received from validator (ns since epoch)
  uint64 timestamp_ns = 2;
  
  // Discriminated union of update types
  oneof payload { ... }
}
```

### Encoding Performance

Benchmarks on typical account update (1KB data):

| Operation | Time | Throughput |
|-----------|------|------------|
| Encode | 200ns | 5M msg/sec |
| Decode | 300ns | 3.3M msg/sec |

For 10MB account (max size):

| Operation | Time | Throughput |
|-----------|------|------------|
| Encode | 2ms | 500 msg/sec |
| Decode | 3ms | 333 msg/sec |

### Compression

For Kafka, we use zstd compression at the producer level:

```rust
client_config.set("compression.type", "zstd");
```

Compression is **not** done at the protobuf level because:
1. Kafka compresses batches, better compression ratio
2. gRPC streaming doesn't benefit from per-message compression
3. Avoids CPU overhead for gRPC clients

## Consequences

### Positive

- **Polyglot**: Generated clients for Rust, Go, Python, TypeScript, Java
- **Schema evolution**: Add fields without breaking consumers
- **Performance**: Sub-microsecond encoding for typical messages
- **Type safety**: Protobuf enforces structure

### Negative

- **Build complexity**: Requires protoc + build.rs
- **One copy unavoidable**: Must copy from validator-owned memory
- **Not smallest format**: FlatBuffers would be smaller

### Trade-offs

**Why not FlatBuffers?**

FlatBuffers is smaller and faster, but:
1. Worse polyglot support (Rust, C++ are good; others are second-class)
2. More complex API (builders, offsets)
3. Schema evolution is more fragile

For a Solana plugin ecosystem, language support matters more than the last 20% of performance.

## Schema Evolution Rules

1. **Never remove fields**: Mark deprecated, keep tag numbers
2. **Never change field types**: Add new field instead
3. **Reserve tag numbers**: Prevent accidental reuse
4. **Use `optional` for nullable**: Explicit about presence

Example evolution:

```protobuf
// v1
message AccountUpdate {
  bytes pubkey = 1;
  bytes data = 2;
}

// v2 (compatible)
message AccountUpdate {
  bytes pubkey = 1;
  bytes data = 2;
  uint64 write_version = 3;  // NEW: added field
  optional bytes txn_signature = 4;  // NEW: optional field
}

// v3 (compatible)
message AccountUpdate {
  bytes pubkey = 1;
  bytes data = 2;
  reserved 3;  // DEPRECATED: write_version
  optional bytes txn_signature = 4;
  uint64 write_version_v2 = 5;  // RENAMED
}
```

## References

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [bytes::Bytes](https://docs.rs/bytes/latest/bytes/struct.Bytes.html)
- [prost Crate](https://docs.rs/prost)
- [tonic-build Configuration](https://docs.rs/tonic-build)
