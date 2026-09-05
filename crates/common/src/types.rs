//! Core types representing validator updates.
//!
//! These types are designed for:
//! - Minimal copying from validator data structures
//! - Efficient serialization to protobuf
//! - Safe transmission across thread boundaries
//!
//! ## Memory Model
//!
//! The validator callbacks provide borrowed data that must be copied
//! before the callback returns. These types own their data to allow
//! asynchronous transmission to sinks.

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature};

mod option_signature {
    use super::*;

    pub fn serialize<S>(val: &Option<[u8; 64]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(bytes) => serializer.serialize_some(&serde_bytes::Bytes::new(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 64]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<serde_bytes::ByteBuf> = Option::deserialize(deserializer)?;
        match opt {
            Some(buf) => {
                let slice: &[u8] = &buf;
                if slice.len() == 64 {
                    let mut arr = [0u8; 64];
                    arr.copy_from_slice(slice);
                    Ok(Some(arr))
                } else {
                    Err(serde::de::Error::custom("expected 64 bytes"))
                }
            }
            None => Ok(None),
        }
    }
}

mod signature_bytes {
    use super::*;

    pub fn serialize<S>(val: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::Bytes::new(val).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf: serde_bytes::ByteBuf = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let slice: &[u8] = &buf;
        if slice.len() == 64 {
            let mut arr = [0u8; 64];
            arr.copy_from_slice(slice);
            Ok(arr)
        } else {
            Err(serde::de::Error::custom("expected 64 bytes"))
        }
    }
}

/// Represents all types of updates that can flow through the plugin.
///
/// Each variant contains owned data suitable for async transmission.
/// The validator callback must convert borrowed data to these owned types
/// before enqueueing.
#[derive(Debug, Clone)]
pub enum Update {
    /// Account data has changed
    Account(AccountUpdate),
    /// Transaction has been processed
    Transaction(TransactionUpdate),
    /// New slot has started
    Slot(SlotUpdate),
    /// New entry (set of transactions) in a slot
    Entry(EntryUpdate),
    /// Block metadata is complete
    BlockMetadata(BlockMetadataUpdate),
}

impl Update {
    /// Returns the slot associated with this update, if applicable.
    pub fn slot(&self) -> Option<Slot> {
        match self {
            Update::Account(u) => Some(u.slot),
            Update::Transaction(u) => Some(u.slot),
            Update::Slot(u) => Some(u.slot),
            Update::Entry(u) => Some(u.slot),
            Update::BlockMetadata(u) => Some(u.slot),
        }
    }

    /// Returns a string identifier for the update type (for metrics).
    pub fn type_name(&self) -> &'static str {
        match self {
            Update::Account(_) => "account",
            Update::Transaction(_) => "transaction",
            Update::Slot(_) => "slot",
            Update::Entry(_) => "entry",
            Update::BlockMetadata(_) => "block_metadata",
        }
    }

    /// Approximate size in bytes for backpressure calculations.
    pub fn size_bytes(&self) -> usize {
        match self {
            Update::Account(u) => u.data.len() + 64, // pubkey + overhead
            Update::Transaction(u) => u.transaction_data.len() + 128,
            Update::Slot(_) => 32,
            Update::Entry(u) => u.entry_data.len() + 64,
            Update::BlockMetadata(_) => 256,
        }
    }
}

/// Account update from the validator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    /// Account public key (32 bytes).
    #[serde(with = "serde_bytes")]
    pub pubkey: [u8; 32],
    /// Account data.
    pub data: Bytes,
    /// Slot when account was updated.
    pub slot: Slot,
    /// Owner program public key (32 bytes).
    #[serde(with = "serde_bytes")]
    pub owner: [u8; 32],
    /// Lamport balance.
    pub lamports: u64,
    /// Rent epoch.
    pub rent_epoch: u64,
    /// Whether account is executable.
    pub executable: bool,
    /// Write version for ordering.
    pub write_version: u64,
    /// Transaction signature that modified this account.
    #[serde(with = "option_signature")]
    pub txn_signature: Option<[u8; 64]>,
}

impl AccountUpdate {
    /// Returns the pubkey as a Solana Pubkey type.
    pub fn pubkey(&self) -> Pubkey {
        Pubkey::from(self.pubkey)
    }

    /// Returns the owner as a Solana Pubkey type.
    pub fn owner(&self) -> Pubkey {
        Pubkey::from(self.owner)
    }
}

/// Transaction update from the validator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionUpdate {
    /// Transaction signature (64 bytes).
    #[serde(with = "signature_bytes")]
    pub signature: [u8; 64],
    /// Serialized transaction data.
    pub transaction_data: Bytes,
    /// Slot containing the transaction.
    pub slot: Slot,
    /// Index within the slot.
    pub index: u64,
    /// Whether this is a vote transaction.
    pub is_vote: bool,
    /// Transaction metadata (optional).
    pub meta: Option<Bytes>,
}

impl TransactionUpdate {
    /// Returns the signature as a Solana Signature type.
    pub fn signature(&self) -> Signature {
        Signature::from(self.signature)
    }
}

/// Slot status update.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SlotUpdate {
    /// Slot number
    pub slot: Slot,
    /// Parent slot number
    pub parent: Option<Slot>,
    /// Slot status
    pub status: SlotStatus,
}

/// Status of a slot in the validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotStatus {
    /// Slot is being processed
    Processed,
    /// Slot is rooted (finalized)
    Rooted,
    /// Slot has been confirmed
    Confirmed,
}

impl SlotStatus {
    /// Returns a string representation for serialization/logging.
    pub fn as_str(&self) -> &'static str {
        match self {
            SlotStatus::Processed => "processed",
            SlotStatus::Rooted => "rooted",
            SlotStatus::Confirmed => "confirmed",
        }
    }
}

/// Entry update (batch of transactions within a slot).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryUpdate {
    /// Slot containing this entry.
    pub slot: Slot,
    /// Index of entry within the slot.
    pub index: u64,
    /// Number of hashes in proof-of-history.
    pub num_hashes: u64,
    /// Entry hash (32 bytes).
    #[serde(with = "serde_bytes")]
    pub hash: [u8; 32],
    /// Serialized entry data (hash bytes).
    pub entry_data: Bytes,
    /// Number of transactions executed in this entry.
    pub executed_transaction_count: u64,
}

/// Block metadata update (emitted when block is complete).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMetadataUpdate {
    /// Slot number.
    pub slot: Slot,
    /// Block hash (32 bytes).
    #[serde(with = "serde_bytes")]
    pub blockhash: [u8; 32],
    /// Unix timestamp when block was produced.
    pub block_time: Option<i64>,
    /// Block height (distinct from slot).
    pub block_height: Option<u64>,
    /// Serialized rewards data.
    pub rewards: Option<Bytes>,
}

/// Wrapper for sending updates through channels with metadata.
#[derive(Debug)]
pub struct EnvelopedUpdate {
    /// The actual update
    pub update: Update,
    /// Timestamp when the update was received from validator
    pub received_at_ns: u64,
    /// Sequence number for ordering
    pub sequence: u64,
}

impl EnvelopedUpdate {
    /// Create a new enveloped update with current timestamp.
    pub fn new(update: Update, sequence: u64) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let received_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Self {
            update,
            received_at_ns,
            sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Update>();
        assert_send_sync::<AccountUpdate>();
        assert_send_sync::<TransactionUpdate>();
        assert_send_sync::<SlotUpdate>();
        assert_send_sync::<EntryUpdate>();
        assert_send_sync::<EnvelopedUpdate>();
    }

    #[test]
    fn slot_status_string_conversion() {
        assert_eq!(SlotStatus::Processed.as_str(), "processed");
        assert_eq!(SlotStatus::Rooted.as_str(), "rooted");
        assert_eq!(SlotStatus::Confirmed.as_str(), "confirmed");
    }
}
