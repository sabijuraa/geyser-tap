# ADR-004: Serialization Strategy

## Status
Accepted

## Context
The plugin receives data from the validator and must serialize it for transmission to consumers. Key considerations:
1. Performance: serialization is on the hot path (validator callback thread)
2. Size: network bandwidth is finite
3. Compatibility: consumers may use different languages

We need a serialization strategy that balances these concerns.

## Decision

**Transaction serialization:**
Use bincode to serialize the transaction message. Bincode is fast and produces compact output. The transaction metadata is serialized separately when available.

**Account serialization:**
Copy the account data into a bytes::Bytes. This is a single copy from the validator's borrowed slice. We then share this Bytes reference across all consumers (clone is cheap - just a reference count).

**Entry serialization:**
Serialize transaction signatures within the entry using bincode. Full transactions are available via notify_transaction, so entries only carry the signature list to indicate which transactions are in each entry.

**Wire format by sink:**
- gRPC: Protocol buffers (defined in proto crate)
- Kafka: Protocol buffers (same messages)
- WebSocket: JSON (for simplicity)

**Zero-copy considerations:**
We use bytes::Bytes throughout the data path. This allows:
- Single copy from validator into owned buffer
- Cheap clones for fan-out to multiple consumers
- No additional copies during protobuf encoding (Bytes maps to proto bytes)

## Consequences

**Positive:**
- Single copy for account data (the unavoidable one)
- Compact binary format for gRPC/Kafka
- Human-readable format for WebSocket
- Fast encoding (bincode for internal, prost for wire)

**Negative:**
- Multiple serialization formats increase code complexity
- JSON is larger than protobuf (acceptable for WS use case)
- bincode is Rust-specific (but we control both ends for internal serialization)

**Trade-off accepted:**
The mixed format approach serves each sink's needs. gRPC/Kafka consumers care about efficiency and use protobuf. WebSocket consumers care about simplicity and use JSON. The additional code is worth the flexibility.

## Alternatives Considered

**JSON everywhere:** Simple but inefficient. Protobuf is 3-5x smaller for binary data like signatures and pubkeys.

**Protobuf everywhere:** Would require WebSocket consumers to handle protobuf, adding friction for simple use cases.

**Cap'n Proto:** Zero-copy on the wire, but more complex to use and less ecosystem support than protobuf.

**Flatbuffers:** Similar to Cap'n Proto - good performance but adds complexity.

## References
- bincode documentation
- prost (protobuf) documentation
- bytes crate
