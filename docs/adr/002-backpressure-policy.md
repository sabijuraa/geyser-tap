# ADR-002: Backpressure Policy

## Status
Accepted

## Context
The validator produces updates at a rate we cannot control. During high-throughput periods (slot transitions, program execution spikes), the update rate can spike to hundreds of thousands per second. Downstream consumers (gRPC clients, Kafka brokers) may not be able to keep up.

We need a policy that:
1. Never blocks the validator callback threads (would affect consensus)
2. Provides bounded memory usage (no unbounded queues)
3. Degrades gracefully under overload
4. Is observable (operators need to know when drops occur)

## Decision
We use a **drop-on-full** policy at multiple levels:

**Plugin-to-runtime channel:**
- Bounded crossbeam channel with configurable capacity (default 100k)
- Validator callbacks use try_send(), never send()
- If the channel is full, the update is dropped immediately
- A counter metric tracks dropped updates
- The callback returns success (the validator doesn't care about our internal backpressure)

**Runtime-to-sink fan-out:**
- Each sink has its own bounded buffer
- The runtime sends to all healthy sinks concurrently
- If a sink's buffer is full, we log and continue with other sinks
- A sink that stays full too long transitions to Degraded, then Unhealthy

**Per-client backpressure (gRPC/WS):**
- Each client has a bounded send channel (default 64k entries)
- If a client's channel fills, new updates are dropped for that client
- If a client stays slow (drops > threshold), they are disconnected
- This isolates slow clients from fast ones

**Kafka:**
- The rdkafka producer has its own bounded queue
- We configure queue.buffering.max.messages and queue.buffering.max.kbytes
- If the producer queue is full, we drop the update
- Producer delivery reports track successful/failed sends

## Consequences

**Positive:**
- Validator is never blocked
- Memory usage is bounded and predictable
- Overload is observable via metrics
- Fast consumers are not affected by slow ones
- Simple mental model: if you're too slow, you miss updates

**Negative:**
- Data loss under sustained overload
- No delivery guarantees within the plugin itself
- Operators must size buffers for their workload

**Trade-off accepted:**
We prioritize validator stability over delivery guarantees. Consumers who need guaranteed delivery should:
1. Use Kafka (which provides its own durability once messages reach the broker)
2. Size their consumers to keep up with the validator's throughput
3. Monitor the dropped metrics and scale up if needed

## Alternatives Considered

**Block on full:** Would guarantee delivery but could stall the validator. Rejected because validator stability is paramount.

**Exponential backoff:** Would smooth out temporary spikes but adds complexity and latency. The drop-on-full policy handles spikes (later updates still get through) and sustained overload (consistent drops are observable).

**Circuit breaker:** Would disable the plugin entirely under overload. Too aggressive - we prefer degraded operation to complete outage.

## References
- Solana validator threading model
- Crossbeam channel documentation
- rdkafka producer configuration
