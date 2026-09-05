//! Update serialization for Kafka.
//!
//! This module handles serializing updates to bytes for Kafka messages.
//! The format is protobuf for:
//! - Efficient encoding
//! - Schema evolution support
//! - Polyglot consumer compatibility

use geyser_tap_common::{SerializationError, Update};
use geyser_tap_proto::convert::to_stream_update;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Serializer for converting updates to Kafka message payloads.
pub struct UpdateSerializer {
    /// Sequence number for ordering
    sequence: AtomicU64,
}

impl UpdateSerializer {
    /// Create a new serializer.
    pub fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
        }
    }

    /// Serialize an update to bytes.
    ///
    /// The output is a protobuf-encoded `StreamUpdate` message.
    pub fn serialize(&self, update: &Update) -> Result<Vec<u8>, SerializationError> {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let proto = to_stream_update(update, seq, timestamp_ns);

        let mut buf = Vec::with_capacity(proto.encoded_len());
        proto
            .encode(&mut buf)
            .map_err(|e| SerializationError::Encode(e.to_string()))?;

        Ok(buf)
    }

    /// Estimate the serialized size of an update without encoding.
    ///
    /// Useful for backpressure calculations.
    pub fn estimate_size(&self, update: &Update) -> usize {
        // Rough estimate: actual update size + ~50 bytes overhead
        update.size_bytes() + 50
    }
}

impl Default for UpdateSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use geyser_tap_common::AccountUpdate;

    #[test]
    fn serialization_produces_bytes() {
        let serializer = UpdateSerializer::new();
        let update = Update::Account(AccountUpdate {
            pubkey: [1u8; 32],
            data: Bytes::from(vec![0u8; 100]),
            slot: 100,
            owner: [0u8; 32],
            lamports: 1000,
            rent_epoch: 0,
            executable: false,
            write_version: 1,
            txn_signature: None,
        });

        let bytes = serializer.serialize(&update).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn sequence_numbers_increment() {
        let serializer = UpdateSerializer::new();
        let update = Update::Slot(geyser_tap_common::SlotUpdate {
            slot: 100,
            parent: None,
            status: geyser_tap_common::SlotStatus::Processed,
        });

        let _ = serializer.serialize(&update).unwrap();
        let _ = serializer.serialize(&update).unwrap();

        // Sequence should now be 2
        assert_eq!(serializer.sequence.load(Ordering::Relaxed), 2);
    }
}
