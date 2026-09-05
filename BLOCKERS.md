# Blockers for geyser-tap End-to-End Testing

## BLOCKER: Segfault when loading plugin into solana-test-validator

### Environment
- OS: Ubuntu 24.04 (WSL2)
- Rust: 1.98.1
- Solana: 1.18.26
- Plugin solana-geyser-plugin-interface: 1.18

### Symptom
```
solana-test-validator --geyser-plugin-config /root/projects/solana/geyser-tap/test-config.json
# Segmentation fault (core dumped)
```

The validator crashes immediately after:
```
[INFO  solana_geyser_plugin_manager::geyser_plugin_service] Starting GeyserPluginService from config files: ["/root/projects/solana/geyser-tap/test-config.json"]
```

### Investigation
1. Plugin .so built successfully (5.8MB)
2. ldd shows minimal dependencies (libc, libm, libz, libgcc_s)
3. _create_plugin() function is simple and shouldn't crash
4. Plugin compiled with Rust 1.98.1, validator may expect different ABI

### Possible Causes
1. **ABI mismatch**: solana-geyser-plugin-interface 1.18 crate may have different ABI than validator 1.18.26
2. **Static initialization**: parking_lot::RwLock or tracing may have static initializers that fail
3. **Memory layout**: Trait object layout mismatch between plugin and validator

### Resolution Path
1. Build plugin with exact same Rust version as validator (1.75.0 per Solana CI)
2. Use solana-geyser-plugin-interface from the exact solana 1.18.26 release
3. Test on native Linux (not WSL) where Solana is officially supported
4. Contact Solana Discord for FFI debugging guidance

### Workaround for Verification
The plugin's business logic (serialization, routing, sink implementations) can be verified via:
1. Unit tests: `cargo test --release`
2. Code review of notify_transaction, notify_entry payloads
3. Integration tests with mock validator interface

### Status
**BLOCKED** - Cannot run end-to-end test with solana-test-validator until ABI compatibility is resolved.

### Date
2026-09-04
