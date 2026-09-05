# ADR-001: FFI Safety Model

> **Superseded 2026-09-05 on the `panic` setting.** The release profile now
> uses `panic = "unwind"`. `panic = "abort"` is not a backstop or
> defense-in-depth for `catch_unwind` -- it *disables* it. With unwinding off,
> `catch_unwind` can never catch a panic, so every callback wrapper was inert
> and a plugin panic aborted the validator outright. The two settings are
> mutually exclusive; panic isolation requires `unwind`. Unwinding stays
> confined to the plugin because `catch_unwind` sits immediately inside each
> `extern "C"` callback. The rest of this ADR still stands. See
> VERIFICATION.md (FR6).

## Status
Accepted

## Context
geyser-tap is loaded as a shared library (.so) into the Solana validator process via dlopen. The validator calls into the plugin through the GeyserPlugin trait, which crosses the C-ABI boundary. Rust panics that unwind across this boundary cause undefined behavior - in practice, this means the validator crashes.

The validator is a critical piece of infrastructure. A plugin bug that crashes the validator takes the node offline, affects consensus, and may cause stake slashing. We cannot accept this risk.

## Decision
Every callback from the validator is wrapped in std::panic::catch_unwind. The wrapping function:

1. Accepts a closure containing the actual callback logic
2. Runs it inside catch_unwind
3. If a panic occurs, catches it before it reaches the FFI boundary
4. Logs the panic with full context (callback name, panic message if available)
5. Returns a GeyserPluginError instead of unwinding

Additionally, the release profile sets panic = "abort" as a defense-in-depth measure. This means any panic that somehow escapes catch_unwind will abort the process rather than causing undefined behavior. In practice, catch_unwind should catch everything, but abort provides a defined failure mode.

All unsafe blocks in the codebase are documented with their invariants. Currently, the only unsafe block is the CStr::from_ptr call when converting the config path from C string, and it documents the invariant that the validator provides a valid null-terminated string.

## Consequences

**Positive:**
- A bug in the plugin (panic) degrades the plugin, not the validator
- The validator continues operating (possibly with degraded plugin functionality)
- Operators get logs about what went wrong
- Defined failure modes instead of undefined behavior

**Negative:**
- catch_unwind has a small runtime cost (though minimal in practice)
- Some types cannot be safely moved across unwind boundaries (we use AssertUnwindSafe where necessary, after verifying the safety)
- panic = "abort" means we lose the ability to use panic for non-fatal assertions during development

**Trade-off accepted:**
The small performance and ergonomic costs are worth the safety guarantee. A production validator cannot have a plugin that might crash it.

## References
- Rust Nomicon: FFI and Panics
- Solana Geyser Plugin Interface documentation
- GeyserPlugin trait requirements
