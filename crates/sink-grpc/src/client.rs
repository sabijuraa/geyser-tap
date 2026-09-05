//! Client session management.
//!
//! Each connected gRPC client is represented by a `ClientSession` that
//! manages its subscription state, buffering, and lifecycle.

use crate::broadcaster::ClientFilter;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Unique identifier for a client session.
pub type SessionId = u64;

/// Represents a connected client session.
pub struct ClientSession {
    /// Unique session identifier
    pub id: SessionId,
    /// Client's remote address (for logging)
    pub remote_addr: SocketAddr,
    /// Subscription filter
    pub filter: ClientFilter,
    /// When the session was created
    pub connected_at: Instant,
    /// Number of updates sent
    pub updates_sent: AtomicU64,
    /// Number of updates dropped (slow consumer)
    pub updates_dropped: AtomicU64,
    /// Bytes sent
    pub bytes_sent: AtomicU64,
}

impl ClientSession {
    /// Create a new client session.
    pub fn new(id: SessionId, remote_addr: SocketAddr, filter: ClientFilter) -> Self {
        Self {
            id,
            remote_addr,
            filter,
            connected_at: Instant::now(),
            updates_sent: AtomicU64::new(0),
            updates_dropped: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        }
    }

    /// Record that an update was sent.
    pub fn record_sent(&self, bytes: usize) {
        self.updates_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Record that an update was dropped.
    pub fn record_dropped(&self) {
        self.updates_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the session duration.
    pub fn duration(&self) -> std::time::Duration {
        self.connected_at.elapsed()
    }

    /// Get session statistics.
    pub fn stats(&self) -> SessionStats {
        SessionStats {
            id: self.id,
            remote_addr: self.remote_addr,
            duration_secs: self.duration().as_secs_f64(),
            updates_sent: self.updates_sent.load(Ordering::Relaxed),
            updates_dropped: self.updates_dropped.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }
}

/// Statistics for a client session.
#[derive(Debug, Clone)]
pub struct SessionStats {
    /// Session ID
    pub id: SessionId,
    /// Remote address
    pub remote_addr: SocketAddr,
    /// Session duration in seconds
    pub duration_secs: f64,
    /// Updates sent
    pub updates_sent: u64,
    /// Updates dropped
    pub updates_dropped: u64,
    /// Bytes sent
    pub bytes_sent: u64,
}

impl SessionStats {
    /// Calculate the update rate (updates per second).
    pub fn update_rate(&self) -> f64 {
        if self.duration_secs > 0.0 {
            self.updates_sent as f64 / self.duration_secs
        } else {
            0.0
        }
    }

    /// Calculate the drop rate (fraction of updates dropped).
    pub fn drop_rate(&self) -> f64 {
        let total = self.updates_sent + self.updates_dropped;
        if total > 0 {
            self.updates_dropped as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Builder for client filters from subscription requests.
pub struct FilterBuilder {
    filter: ClientFilter,
}

impl FilterBuilder {
    /// Create a new filter builder with defaults.
    pub fn new() -> Self {
        Self {
            filter: ClientFilter::default(),
        }
    }

    /// Enable account updates.
    pub fn with_accounts(mut self, enabled: bool) -> Self {
        self.filter.accounts = enabled;
        self
    }

    /// Set account owner filter.
    pub fn with_account_owners(mut self, owners: Vec<[u8; 32]>) -> Self {
        self.filter.account_owners = owners;
        self
    }

    /// Set account pubkey filter.
    pub fn with_account_pubkeys(mut self, pubkeys: Vec<[u8; 32]>) -> Self {
        self.filter.account_pubkeys = pubkeys;
        self
    }

    /// Enable transaction updates.
    pub fn with_transactions(mut self, enabled: bool) -> Self {
        self.filter.transactions = enabled;
        self
    }

    /// Include vote transactions.
    pub fn with_votes(mut self, include: bool) -> Self {
        self.filter.include_votes = include;
        self
    }

    /// Enable slot updates.
    pub fn with_slots(mut self, enabled: bool) -> Self {
        self.filter.slots = enabled;
        self
    }

    /// Enable entry updates.
    pub fn with_entries(mut self, enabled: bool) -> Self {
        self.filter.entries = enabled;
        self
    }

    /// Enable block metadata updates.
    pub fn with_block_metadata(mut self, enabled: bool) -> Self {
        self.filter.block_metadata = enabled;
        self
    }

    /// Build the filter.
    pub fn build(self) -> ClientFilter {
        self.filter
    }
}

impl Default for FilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_stats_calculations() {
        let session = ClientSession::new(
            1,
            "127.0.0.1:12345".parse().unwrap(),
            ClientFilter::default(),
        );

        session.updates_sent.store(100, Ordering::Relaxed);
        session.updates_dropped.store(10, Ordering::Relaxed);

        let stats = session.stats();
        assert_eq!(stats.updates_sent, 100);
        assert_eq!(stats.updates_dropped, 10);

        // Drop rate should be 10 / 110 ~ 0.09
        let drop_rate = stats.drop_rate();
        assert!(drop_rate > 0.08 && drop_rate < 0.10);
    }
}
