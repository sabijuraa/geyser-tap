//! SDK-specific types for consuming updates.

use bytes::Bytes;
use geyser_tap_proto::geyser;

/// An update received from the server.
#[derive(Debug, Clone)]
pub enum Update {
    /// Account data update
    Account(AccountUpdate),
    /// Transaction update
    Transaction(TransactionUpdate),
    /// Slot status update
    Slot(SlotUpdate),
    /// Entry update
    Entry(EntryUpdate),
    /// Block metadata update
    BlockMetadata(BlockMetadataUpdate),
}

impl Update {
    /// Get the slot associated with this update.
    pub fn slot(&self) -> Option<u64> {
        match self {
            Update::Account(u) => Some(u.slot),
            Update::Transaction(u) => Some(u.slot),
            Update::Slot(u) => Some(u.slot),
            Update::Entry(u) => Some(u.slot),
            Update::BlockMetadata(u) => Some(u.slot),
        }
    }
}

impl TryFrom<geyser::StreamUpdate> for Update {
    type Error = crate::SdkError;

    fn try_from(proto: geyser::StreamUpdate) -> Result<Self, Self::Error> {
        match proto.payload {
            Some(geyser::UpdatePayload::Account(a)) => Ok(Update::Account(a.into())),
            Some(geyser::UpdatePayload::Transaction(t)) => Ok(Update::Transaction(t.into())),
            Some(geyser::UpdatePayload::Slot(s)) => Ok(Update::Slot(s.into())),
            Some(geyser::UpdatePayload::Entry(e)) => Ok(Update::Entry(e.into())),
            Some(geyser::UpdatePayload::BlockMetadata(b)) => Ok(Update::BlockMetadata(b.into())),
            None => Err(crate::SdkError::Decode("empty payload".to_string())),
        }
    }
}

/// Account data update.
#[derive(Debug, Clone)]
pub struct AccountUpdate {
    /// Account public key (base58)
    pub pubkey: String,
    /// Account data
    pub data: Bytes,
    /// Slot of the update
    pub slot: u64,
    /// Owner program (base58)
    pub owner: String,
    /// Lamport balance
    pub lamports: u64,
    /// Whether executable
    pub executable: bool,
    /// Write version
    pub write_version: u64,
}

impl AccountUpdate {
    /// Get the pubkey as bytes.
    pub fn pubkey_bytes(&self) -> Option<[u8; 32]> {
        bs58::decode(&self.pubkey)
            .into_vec()
            .ok()
            .and_then(|v: Vec<u8>| v.try_into().ok())
    }
}

impl From<geyser::AccountUpdate> for AccountUpdate {
    fn from(proto: geyser::AccountUpdate) -> Self {
        Self {
            pubkey: bs58::encode(&proto.pubkey).into_string(),
            data: proto.data,
            slot: proto.slot,
            owner: bs58::encode(&proto.owner).into_string(),
            lamports: proto.lamports,
            executable: proto.executable,
            write_version: proto.write_version,
        }
    }
}

/// Transaction update.
#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    /// Transaction signature (base58)
    pub signature: String,
    /// Serialized transaction
    pub transaction: Bytes,
    /// Slot containing the transaction
    pub slot: u64,
    /// Index within the slot
    pub index: u64,
    /// Whether this is a vote transaction
    pub is_vote: bool,
    /// Transaction metadata
    pub meta: Option<Bytes>,
}

impl TransactionUpdate {
    /// Get the signature as bytes.
    pub fn signature_bytes(&self) -> Option<[u8; 64]> {
        bs58::decode(&self.signature)
            .into_vec()
            .ok()
            .and_then(|v: Vec<u8>| v.try_into().ok())
    }
}

impl From<geyser::TransactionUpdate> for TransactionUpdate {
    fn from(proto: geyser::TransactionUpdate) -> Self {
        Self {
            signature: bs58::encode(&proto.signature).into_string(),
            transaction: proto.transaction,
            slot: proto.slot,
            index: proto.index,
            is_vote: proto.is_vote,
            meta: proto.meta,
        }
    }
}

/// Slot status update.
#[derive(Debug, Clone, Copy)]
pub struct SlotUpdate {
    /// Slot number
    pub slot: u64,
    /// Parent slot number
    pub parent: Option<u64>,
    /// Slot status
    pub status: SlotStatus,
}

impl From<geyser::SlotUpdate> for SlotUpdate {
    fn from(proto: geyser::SlotUpdate) -> Self {
        Self {
            slot: proto.slot,
            parent: proto.parent,
            status: SlotStatus::from(proto.status),
        }
    }
}

/// Slot status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    /// Unknown
    Unknown,
    /// Being processed
    Processed,
    /// Finalized
    Rooted,
    /// Confirmed
    Confirmed,
}

impl From<i32> for SlotStatus {
    fn from(v: i32) -> Self {
        match v {
            1 => SlotStatus::Processed,
            2 => SlotStatus::Rooted,
            3 => SlotStatus::Confirmed,
            _ => SlotStatus::Unknown,
        }
    }
}

/// Entry update.
#[derive(Debug, Clone)]
pub struct EntryUpdate {
    /// Slot containing the entry
    pub slot: u64,
    /// Entry index within the slot
    pub index: u64,
    /// Number of hashes since previous entry
    pub num_hashes: u64,
    /// Entry hash (base58)
    pub hash: String,
    /// Entry data
    pub entry: Bytes,
    /// Number of executed transactions
    pub executed_transaction_count: u64,
}

impl From<geyser::EntryUpdate> for EntryUpdate {
    fn from(proto: geyser::EntryUpdate) -> Self {
        Self {
            slot: proto.slot,
            index: proto.index,
            num_hashes: proto.num_hashes,
            hash: bs58::encode(&proto.hash).into_string(),
            entry: proto.entry,
            executed_transaction_count: proto.executed_transaction_count,
        }
    }
}

/// Block metadata update.
#[derive(Debug, Clone)]
pub struct BlockMetadataUpdate {
    /// Slot number
    pub slot: u64,
    /// Block hash (base58)
    pub blockhash: String,
    /// Block time (Unix timestamp)
    pub block_time: Option<i64>,
    /// Block height
    pub block_height: Option<u64>,
    /// Rewards data
    pub rewards: Option<Bytes>,
}

impl From<geyser::BlockMetadataUpdate> for BlockMetadataUpdate {
    fn from(proto: geyser::BlockMetadataUpdate) -> Self {
        Self {
            slot: proto.slot,
            blockhash: bs58::encode(&proto.blockhash).into_string(),
            block_time: proto.block_time,
            block_height: proto.block_height,
            rewards: proto.rewards,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_status_conversion() {
        assert_eq!(SlotStatus::from(1), SlotStatus::Processed);
        assert_eq!(SlotStatus::from(2), SlotStatus::Rooted);
        assert_eq!(SlotStatus::from(3), SlotStatus::Confirmed);
        assert_eq!(SlotStatus::from(99), SlotStatus::Unknown);
    }
}
