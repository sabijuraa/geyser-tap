//! Update broadcaster for fan-out to multiple clients.
//!
//! The broadcaster maintains a set of connected clients and distributes
//! updates to all of them concurrently. It handles:
//!
//! - Client registration and deregistration
//! - Per-client subscription filters
//! - Slow client detection and backpressure
//! - Client cleanup on disconnect

use dashmap::DashMap;
use geyser_tap_common::{SinkResult, Update};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Unique identifier for a connected client.
pub type ClientId = u64;

/// A registered client with its update channel.
pub struct RegisteredClient {
    /// Unique client identifier
    pub id: ClientId,
    /// Channel sender for updates to this client
    pub sender: mpsc::Sender<Arc<Update>>,
    /// Subscription filter for this client
    pub filter: ClientFilter,
    /// Number of updates sent to this client
    pub updates_sent: AtomicU64,
    /// Number of updates dropped for this client (slow consumer)
    pub updates_dropped: AtomicU64,
}

/// Filter determining which updates a client receives.
#[derive(Debug, Clone, Default)]
pub struct ClientFilter {
    /// Account owners to include (empty = all)
    pub account_owners: Vec<[u8; 32]>,
    /// Account pubkeys to include (empty = all)
    pub account_pubkeys: Vec<[u8; 32]>,
    /// Whether to include account updates
    pub accounts: bool,
    /// Whether to include transaction updates
    pub transactions: bool,
    /// Whether to include vote transactions
    pub include_votes: bool,
    /// Whether to include slot updates
    pub slots: bool,
    /// Whether to include entry updates
    pub entries: bool,
    /// Whether to include block metadata updates
    pub block_metadata: bool,
}

impl ClientFilter {
    /// Check if an update matches this filter.
    pub fn matches(&self, update: &Update) -> bool {
        match update {
            Update::Account(u) => {
                if !self.accounts {
                    return false;
                }
                // Check owner filter
                if !self.account_owners.is_empty()
                    && !self.account_owners.contains(&u.owner)
                {
                    return false;
                }
                // Check pubkey filter
                if !self.account_pubkeys.is_empty()
                    && !self.account_pubkeys.contains(&u.pubkey)
                {
                    return false;
                }
                true
            }
            Update::Transaction(u) => {
                if !self.transactions {
                    return false;
                }
                if u.is_vote && !self.include_votes {
                    return false;
                }
                true
            }
            Update::Slot(_) => self.slots,
            Update::Entry(_) => self.entries,
            Update::BlockMetadata(_) => self.block_metadata,
        }
    }
}

/// Broadcasts updates to all connected clients.
pub struct Broadcaster {
    /// Map of client ID to client state
    clients: DashMap<ClientId, Arc<RegisteredClient>>,
    /// Counter for generating unique client IDs
    next_client_id: AtomicU64,
    /// Per-client channel capacity
    channel_capacity: usize,
}

impl Broadcaster {
    /// Create a new broadcaster.
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            clients: DashMap::new(),
            next_client_id: AtomicU64::new(1),
            channel_capacity,
        }
    }

    /// Register a new client and return its ID and receiver channel.
    pub fn register(&self, filter: ClientFilter) -> (ClientId, mpsc::Receiver<Arc<Update>>) {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(self.channel_capacity);

        let client = Arc::new(RegisteredClient {
            id,
            sender,
            filter,
            updates_sent: AtomicU64::new(0),
            updates_dropped: AtomicU64::new(0),
        });

        self.clients.insert(id, client);
        tracing::info!(client_id = id, "Client registered");

        (id, receiver)
    }

    /// Unregister a client.
    pub fn unregister(&self, client_id: ClientId) {
        if let Some((_, client)) = self.clients.remove(&client_id) {
            tracing::info!(
                client_id,
                updates_sent = client.updates_sent.load(Ordering::Relaxed),
                updates_dropped = client.updates_dropped.load(Ordering::Relaxed),
                "Client unregistered"
            );
        }
    }

    /// Broadcast an update to all clients.
    ///
    /// Returns the number of clients that received the update.
    /// Clients whose buffers are full will have the update dropped.
    pub async fn broadcast(&self, update: Update) -> SinkResult<usize> {
        let update = Arc::new(update);
        let mut sent_count = 0;
        let mut dropped_clients = Vec::new();

        for entry in self.clients.iter() {
            let client = entry.value();

            // Check filter
            if !client.filter.matches(&update) {
                continue;
            }

            // Try to send without blocking
            match client.sender.try_send(Arc::clone(&update)) {
                Ok(()) => {
                    client.updates_sent.fetch_add(1, Ordering::Relaxed);
                    sent_count += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Client is slow, drop the update
                    client.updates_dropped.fetch_add(1, Ordering::Relaxed);
                    geyser_tap_common::metrics::record_update_dropped("grpc");
                    tracing::warn!(
                        client_id = client.id,
                        dropped = client.updates_dropped.load(Ordering::Relaxed),
                        "Client buffer full, dropping update"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Client disconnected
                    dropped_clients.push(client.id);
                }
            }
        }

        // Clean up disconnected clients
        for client_id in dropped_clients {
            self.unregister(client_id);
        }

        Ok(sent_count)
    }

    /// Get the number of connected clients.
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Get statistics for a specific client.
    pub fn client_stats(&self, client_id: ClientId) -> Option<(u64, u64)> {
        self.clients.get(&client_id).map(|c| {
            (
                c.updates_sent.load(Ordering::Relaxed),
                c.updates_dropped.load(Ordering::Relaxed),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_defaults_match_nothing() {
        let filter = ClientFilter::default();
        // Default filter has all types disabled
        assert!(!filter.accounts);
        assert!(!filter.transactions);
    }
}
