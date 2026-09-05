//! Plugin state management.
//!
//! This module contains the shared state accessed by validator callback
//! threads. All state must be thread-safe since callbacks come from
//! multiple validator threads concurrently.

use crate::runtime::UpdateSender;
use geyser_tap_common::{GeyserTapError, PluginConfig, Update, UpdateTypeFilter};
use std::sync::atomic::{AtomicU64, Ordering};

/// Combined filter configuration for the plugin.
#[derive(Debug, Clone)]
pub struct PluginFilter {
    /// Update type filter
    pub update_types: UpdateTypeFilter,
    /// Whether to include vote transactions
    pub include_votes: bool,
}

/// Shared plugin state.
///
/// This struct is accessed from multiple validator threads and must be
/// entirely thread-safe. We use `parking_lot::RwLock` for better
/// performance than std RwLock.
pub struct PluginState {
    /// Filter configuration for which updates to process
    filter: PluginFilter,
    /// Channel sender for updates
    sender: UpdateSender,
    /// Statistics
    stats: PluginStats,
}

impl PluginState {
    /// Create new plugin state.
    pub fn new(config: &PluginConfig, sender: UpdateSender) -> Result<Self, GeyserTapError> {
        // Determine filter from config
        // Merge filters from all enabled sinks
        let filter = Self::build_filter(config);

        Ok(Self {
            filter,
            sender,
            stats: PluginStats::default(),
        })
    }

    /// Build the combined filter from configuration.
    fn build_filter(config: &PluginConfig) -> PluginFilter {
        let mut update_types = UpdateTypeFilter {
            accounts: false,
            transactions: false,
            slots: false,
            entries: false,
            block_metadata: false,
        };
        let mut include_votes = false;

        // Enable types based on sink configurations
        if let Some(ref grpc) = config.grpc {
            if grpc.enabled {
                merge_filter(&mut update_types, &grpc.filters.update_types);
                include_votes |= grpc.filters.include_votes;
            }
        }

        if let Some(ref kafka) = config.kafka {
            if kafka.enabled {
                merge_filter(&mut update_types, &kafka.filters.update_types);
                include_votes |= kafka.filters.include_votes;
            }
        }

        // The websocket sink was previously not wired into the plugin, so its
        // filters were never merged here. Now that create_sinks() builds it,
        // an update type enabled only on the ws sink must still be ingested.
        if let Some(ref ws) = config.ws {
            if ws.enabled {
                merge_filter(&mut update_types, &ws.filters.update_types);
                include_votes |= ws.filters.include_votes;
            }
        }

        PluginFilter {
            update_types,
            include_votes,
        }
    }

    /// Get the current filter configuration.
    pub fn filter(&self) -> &PluginFilter {
        &self.filter
    }

    /// Send an update to the runtime.
    ///
    /// Returns `true` if the update was sent, `false` if dropped.
    pub fn send(&self, update: Update) {
        let sent = self.sender.send(update);
        if sent {
            self.stats.updates_sent.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.updates_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get current statistics.
    #[allow(dead_code)]
    pub fn stats(&self) -> (u64, u64) {
        (
            self.stats.updates_sent.load(Ordering::Relaxed),
            self.stats.updates_dropped.load(Ordering::Relaxed),
        )
    }
}

/// Statistics counters.
#[derive(Default)]
struct PluginStats {
    /// Updates successfully sent to runtime
    updates_sent: AtomicU64,
    /// Updates dropped due to backpressure
    updates_dropped: AtomicU64,
}

/// Merge two filter configurations (OR logic).
fn merge_filter(target: &mut UpdateTypeFilter, source: &UpdateTypeFilter) {
    target.accounts |= source.accounts;
    target.transactions |= source.transactions;
    target.slots |= source.slots;
    target.entries |= source.entries;
    target.block_metadata |= source.block_metadata;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_merge_is_or() {
        let mut target = UpdateTypeFilter {
            accounts: true,
            transactions: false,
            slots: false,
            entries: false,
            block_metadata: false,
        };

        let source = UpdateTypeFilter {
            accounts: false,
            transactions: true,
            slots: false,
            entries: false,
            block_metadata: false,
        };

        merge_filter(&mut target, &source);

        assert!(target.accounts);
        assert!(target.transactions);
        assert!(!target.slots);
    }
}
