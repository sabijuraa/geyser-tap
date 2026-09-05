//! # geyser-tap-sdk
//!
//! Rust client SDK for consuming geyser-tap streams.
//!
//! This SDK provides an ergonomic interface for subscribing to Solana
//! validator updates via the geyser-tap gRPC service.
//!
//! ## Example
//!
//! ```ignore
//! use geyser_tap_sdk::{GeyserClient, SubscriptionBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to geyser-tap server
//!     let mut client = GeyserClient::connect("http://localhost:10000").await?;
//!
//!     // Subscribe to account updates for a specific program
//!     let subscription = SubscriptionBuilder::new()
//!         .accounts()
//!         .with_owner("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
//!         .slots()
//!         .build();
//!
//!     let mut stream = client.subscribe(subscription).await?;
//!
//!     while let Some(update) = stream.next().await {
//!         match update? {
//!             Update::Account(acc) => println!("Account: {}", acc.pubkey()),
//!             Update::Slot(slot) => println!("Slot: {}", slot.slot),
//!             _ => {}
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs, rust_2018_idioms)]

mod client;
mod error;
mod subscription;
mod types;

pub use client::GeyserClient;
pub use error::{SdkError, SdkResult};
pub use subscription::SubscriptionBuilder;
pub use types::*;

/// Re-export proto types for direct access.
pub mod proto {
    pub use geyser_tap_proto::geyser::*;
}
