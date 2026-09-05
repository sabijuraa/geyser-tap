//! Build script for geyser-tap-proto.
//!
//! The protobuf *message* types are hand-written in `lib.rs` with prost derive
//! macros. This script generates only the tonic *service* layer (client and
//! server routing for `geyser.GeyserStream`), mapping every message in the
//! schema onto the existing hand-written type via `extern_path` so there is a
//! single definition of each message and no duplicate/divergent copies.
//!
//! Requires `protoc` on PATH at build time.

#![allow(clippy::disallowed_methods)]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/geyser.proto");

    // Every message and enum in geyser.proto maps to the hand-written type of
    // the same name in `crate::geyser`, so prost-build emits no message code.
    const TYPES: &[&str] = &[
        "SubscribeRequest",
        "AccountFilter",
        "TransactionFilter",
        "PingRequest",
        "PingResponse",
        "StreamUpdate",
        "AccountUpdate",
        "TransactionUpdate",
        "SlotUpdate",
        "EntryUpdate",
        "BlockMetadataUpdate",
        "SlotStatus",
    ];

    let mut builder = tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Only the service module is emitted into OUT_DIR.
        .out_dir(std::env::var("OUT_DIR")?);

    for ty in TYPES {
        builder = builder.extern_path(format!(".geyser.{ty}"), format!("crate::geyser::{ty}"));
    }

    builder.compile(&["src/geyser.proto"], &["src"])?;

    Ok(())
}
