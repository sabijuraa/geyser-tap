//! Kafka partitioning strategy.
//!
//! The partitioner determines how updates are distributed across Kafka
//! partitions. The strategy is designed to:
//!
//! 1. Ensure ordering for related updates (same account, same transaction)
//! 2. Distribute load evenly across partitions
//! 3. Enable parallel consumption by key

use geyser_tap_common::Update;

/// Partitioner for Kafka messages.
///
/// Generates partition keys based on update content to ensure:
/// - All updates for the same account go to the same partition
/// - All updates for the same transaction go to the same partition
/// - Slots and block metadata use slot number for ordering
pub struct Partitioner;

impl Partitioner {
    /// Create a new partitioner.
    pub fn new() -> Self {
        Self
    }

    /// Get the partition key for an update.
    ///
    /// The key is a byte array that Kafka will hash to determine
    /// the partition. Using the same key guarantees ordering.
    pub fn key_for_update(&self, update: &Update) -> Vec<u8> {
        match update {
            Update::Account(u) => {
                // Key by account pubkey for account ordering
                u.pubkey.to_vec()
            }
            Update::Transaction(u) => {
                // Key by signature for transaction ordering
                u.signature.to_vec()
            }
            Update::Slot(u) => {
                // Key by slot for slot ordering
                u.slot.to_le_bytes().to_vec()
            }
            Update::Entry(u) => {
                // Key by slot for entry ordering within slot
                u.slot.to_le_bytes().to_vec()
            }
            Update::BlockMetadata(u) => {
                // Key by slot for block metadata ordering
                u.slot.to_le_bytes().to_vec()
            }
        }
    }
}

impl Default for Partitioner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use geyser_tap_common::{AccountUpdate, SlotStatus, SlotUpdate};

    #[test]
    fn account_key_is_pubkey() {
        let partitioner = Partitioner::new();
        let pubkey = [1u8; 32];
        let update = Update::Account(AccountUpdate {
            pubkey,
            data: Bytes::new(),
            slot: 100,
            owner: [0u8; 32],
            lamports: 1000,
            rent_epoch: 0,
            executable: false,
            write_version: 1,
            txn_signature: None,
        });

        let key = partitioner.key_for_update(&update);
        assert_eq!(key, pubkey.to_vec());
    }

    #[test]
    fn slot_key_is_slot_number() {
        let partitioner = Partitioner::new();
        let update = Update::Slot(SlotUpdate {
            slot: 12345,
            parent: Some(12344),
            status: SlotStatus::Processed,
        });

        let key = partitioner.key_for_update(&update);
        assert_eq!(key, 12345u64.to_le_bytes().to_vec());
    }
}
