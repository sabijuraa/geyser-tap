//! # geyser-tap-common
//!
//! Shared types, traits, and utilities for the geyser-tap Solana Geyser plugin.
//!
//! This crate provides:
//! - [`Sink`] trait for implementing downstream data sinks
//! - [`Update`] enum representing all validator update types
//! - [`SinkConfig`] for sink configuration
//! - Metrics utilities for observability
//! - Error types for the entire plugin ecosystem
//!
//! ## Design Principles
//!
//! 1. **Zero-copy where possible**: Updates reference data without copying
//! 2. **Backpressure-aware**: Sinks must handle bounded channel semantics
//! 3. **Thread-safe**: All types are `Send + Sync` for validator callback threads

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms, clippy::all)]

pub mod config;
pub mod error;
pub mod metrics;
pub mod metrics_server;
pub mod sink;
pub mod types;

pub use config::*;
pub use error::*;
pub use sink::*;
pub use types::*;
