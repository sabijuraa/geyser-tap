//! Subscription builder for filtering updates.

use bytes::Bytes;
use geyser_tap_proto::geyser;

/// Builder for creating subscriptions with filters.
///
/// # Example
///
/// ```ignore
/// let subscription = SubscriptionBuilder::new()
///     .accounts()
///     .with_owner("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
///     .transactions()
///     .include_votes(false)
///     .slots()
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct SubscriptionBuilder {
    accounts: Option<AccountFilter>,
    transactions: Option<TransactionFilter>,
    slots: bool,
    entries: bool,
    block_metadata: bool,
}

impl SubscriptionBuilder {
    /// Create a new subscription builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to account updates.
    pub fn accounts(mut self) -> Self {
        self.accounts = Some(AccountFilter::default());
        self
    }

    /// Filter accounts by owner program.
    pub fn with_owner(mut self, owner: &str) -> Self {
        let pubkey = bs58::decode(owner).into_vec().unwrap_or_default();
        if let Some(ref mut filter) = self.accounts {
            filter.owners.push(pubkey);
        } else {
            let mut filter = AccountFilter::default();
            filter.owners.push(pubkey);
            self.accounts = Some(filter);
        }
        self
    }

    /// Filter accounts by pubkey.
    pub fn with_pubkey(mut self, pubkey: &str) -> Self {
        let key = bs58::decode(pubkey).into_vec().unwrap_or_default();
        if let Some(ref mut filter) = self.accounts {
            filter.pubkeys.push(key);
        } else {
            let mut filter = AccountFilter::default();
            filter.pubkeys.push(key);
            self.accounts = Some(filter);
        }
        self
    }

    /// Subscribe to transaction updates.
    pub fn transactions(mut self) -> Self {
        self.transactions = Some(TransactionFilter::default());
        self
    }

    /// Include or exclude vote transactions.
    pub fn include_votes(mut self, include: bool) -> Self {
        if let Some(ref mut filter) = self.transactions {
            filter.include_votes = include;
        } else {
            self.transactions = Some(TransactionFilter {
                include_votes: include,
                ..Default::default()
            });
        }
        self
    }

    /// Include or exclude failed transactions.
    pub fn include_failed(mut self, include: bool) -> Self {
        if let Some(ref mut filter) = self.transactions {
            filter.include_failed = include;
        } else {
            self.transactions = Some(TransactionFilter {
                include_failed: include,
                ..Default::default()
            });
        }
        self
    }

    /// Subscribe to slot updates.
    pub fn slots(mut self) -> Self {
        self.slots = true;
        self
    }

    /// Subscribe to entry updates.
    pub fn entries(mut self) -> Self {
        self.entries = true;
        self
    }

    /// Subscribe to block metadata updates.
    pub fn block_metadata(mut self) -> Self {
        self.block_metadata = true;
        self
    }

    /// Build the subscription.
    pub fn build(self) -> Subscription {
        Subscription {
            accounts: self.accounts,
            transactions: self.transactions,
            slots: self.slots,
            entries: self.entries,
            block_metadata: self.block_metadata,
        }
    }
}

/// A subscription configuration.
#[derive(Debug, Clone, Default)]
pub struct Subscription {
    /// Account filter
    pub accounts: Option<AccountFilter>,
    /// Transaction filter
    pub transactions: Option<TransactionFilter>,
    /// Whether to receive slot updates
    pub slots: bool,
    /// Whether to receive entry updates
    pub entries: bool,
    /// Whether to receive block metadata updates
    pub block_metadata: bool,
}

impl Subscription {
    /// Convert to a gRPC subscribe request.
    pub fn into_request(self) -> geyser::SubscribeRequest {
        geyser::SubscribeRequest {
            accounts: self.accounts.map(|f| geyser::AccountFilter {
                owners: f.owners.into_iter().map(Bytes::from).collect(),
                pubkeys: f.pubkeys.into_iter().map(Bytes::from).collect(),
            }),
            transactions: self.transactions.map(|f| geyser::TransactionFilter {
                include_votes: f.include_votes,
                include_failed: f.include_failed,
                account_keys: f.account_keys.into_iter().map(Bytes::from).collect(),
            }),
            slots: self.slots,
            entries: self.entries,
            block_metadata: self.block_metadata,
        }
    }
}

/// Filter for account updates.
#[derive(Debug, Clone, Default)]
pub struct AccountFilter {
    /// Owner programs to filter by
    pub owners: Vec<Vec<u8>>,
    /// Account pubkeys to filter by
    pub pubkeys: Vec<Vec<u8>>,
}

/// Filter for transaction updates.
#[derive(Debug, Clone, Default)]
pub struct TransactionFilter {
    /// Include vote transactions
    pub include_votes: bool,
    /// Include failed transactions
    pub include_failed: bool,
    /// Account keys that must be involved
    pub account_keys: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_chain() {
        let sub = SubscriptionBuilder::new()
            .accounts()
            .with_owner("11111111111111111111111111111111")
            .transactions()
            .include_votes(false)
            .slots()
            .build();

        assert!(sub.accounts.is_some());
        assert!(sub.transactions.is_some());
        assert!(sub.slots);
        assert!(!sub.entries);
    }

    #[test]
    fn into_request() {
        let sub = SubscriptionBuilder::new().slots().block_metadata().build();

        let request = sub.into_request();
        assert!(request.slots);
        assert!(request.block_metadata);
        assert!(request.accounts.is_none());
    }
}
