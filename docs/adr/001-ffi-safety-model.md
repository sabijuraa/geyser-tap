# ADR-001: FFI Safety Model

## Status

Accepted

## Context

Geyser-Tap is a Solana Geyser plugin loaded as a shared library (`.so`) by the validator process. The validator is a Rust program, but the plugin interface uses C ABI for dynamic loading compatibility. This creates several FFI-related risks:

1. **Panics across FFI boundary**: Rust panics that unwind across the FFI boundary cause undefined behavior
2. **CStr handling**: The validator passes file paths as C strings (`*const c_char`)
3. **Memory ownership**: Unclear who owns memory passed across the boundary
4. **Thread safety**: Validator callbacks come from multiple threads

A bug in FFI handling will crash the validator, causing consensus failures and potential slashing.

## Decision

We implement a multi-layered FFI safety model:

### Layer 1: Compile-Time Prevention

```toml
[profile.release]
panic = "abort"
```

This prevents stack unwinding entirely in release builds. Any panic immediately aborts, which is safer than undefined behavior from cross-FFI unwinding.

### Layer 2: Runtime Panic Catching

For defense in depth (and debug builds), we wrap all FFI entry points:

```rust
fn catch_panic<F, R>(&self, context: &str, f: F) -> PluginResult<R>
where
    F: FnOnce() -> PluginResult<R> + UnwindSafe,
{
    match std::panic::catch_unwind(f) {
        Ok(result) => result,
        Err(panic_info) => {
            tracing::error!(context, "Panic caught: {:?}", panic_info);
            Err(GeyserPluginError::Custom(/* ... */))
        }
    }
}
```

Every public trait method uses this wrapper:

```rust
fn update_account(&self, ...) -> PluginResult<()> {
    self.catch_panic("update_account", AssertUnwindSafe(|| {
        // Actual implementation
    }))
}
```

### Layer 3: Safe CStr Handling

Configuration paths from the validator are C strings:

```rust
unsafe fn cstr_to_path(ptr: *const i8) -> PluginResult<PathBuf> {
    // Null check
    if ptr.is_null() {
        return Err(GeyserPluginError::Custom(/* null pointer */));
    }
    
    // Safe CStr creation
    let c_str = CStr::from_ptr(ptr);
    
    // UTF-8 validation
    let path_str = c_str.to_str().map_err(|e| {
        GeyserPluginError::Custom(/* invalid UTF-8 */)
    })?;
    
    Ok(PathBuf::from(path_str))
}
```

### Layer 4: Memory Ownership Rules

1. **Validator-owned data**: Copied immediately in callbacks, never stored
2. **Plugin-owned data**: Managed by Rust, never exposed to validator
3. **No raw pointers stored**: Everything converted to owned types

```rust
fn update_account(&self, account: ReplicaAccountInfoVersions, ...) {
    // COPY from validator memory into owned Bytes
    let data = Bytes::copy_from_slice(info.data);
    
    // Now safe to use asynchronously after callback returns
    let update = AccountUpdate { data, ... };
}
```

### Layer 5: Thread Safety

All shared state uses appropriate synchronization:

```rust
pub struct GeyserTapPlugin {
    // RwLock for read-heavy access pattern
    state: RwLock<Option<Arc<PluginState>>>,
    
    // Atomic for simple counters
    sequence: AtomicU64,
}
```

## Consequences

### Positive

- **Validator stability**: Plugin bugs cannot crash the validator through panics
- **Debuggability**: Panics are logged with context before being converted to errors
- **No memory issues**: Clear ownership prevents use-after-free and leaks
- **Thread-safe by construction**: Rust's type system enforces safety

### Negative

- **`AssertUnwindSafe` proliferation**: Every callback needs the wrapper
- **Performance overhead**: `catch_unwind` has measurable but small overhead
- **Memory copying**: Cannot use zero-copy from validator (safety > performance)

### Risks

- **`panic = "abort"` in release**: Makes debugging harder (no stack trace)
- **Silent drops**: Panics become errors that may be silently logged

## Alternatives Considered

### Alternative 1: Trust the Implementation

Don't wrap in `catch_unwind`, rely on code correctness.

**Rejected**: Too risky. A single overlooked edge case crashes the validator.

### Alternative 2: Process Isolation

Run plugin logic in a separate process, communicate via IPC.

**Rejected**: Too much latency. Geyser callbacks must be fast (<1ms).

### Alternative 3: Wasm Sandbox

Compile plugin to Wasm, run in sandbox.

**Rejected**: Wasm doesn't support the geyser plugin interface, and FFI overhead would be prohibitive.

## References

- [Rustonomicon: FFI](https://doc.rust-lang.org/nomicon/ffi.html)
- [Rust panic handling](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html)
- [Solana Geyser Plugin Interface](https://docs.rs/solana-geyser-plugin-interface)
