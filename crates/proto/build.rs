//! Build script for geyser-tap-proto.
//!
//! The proto types are defined manually in lib.rs using prost derive macros
//! to avoid protoc dependency during cross-platform builds. The .proto file
//! is kept for documentation and potential future codegen.

#![allow(clippy::disallowed_methods)]

fn main() {
    // Rerun if proto changes (for future codegen)
    println!("cargo:rerun-if-changed=src/geyser.proto");
}
