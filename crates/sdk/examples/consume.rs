//! Minimal consumer: connects to a running geyser-tap gRPC sink and prints
//! the updates it receives.
//!
//! Used as the end-to-end acceptance check against a live validator:
//!
//! ```sh
//! cargo run --release -p geyser-tap-sdk --example consume -- \
//!     http://127.0.0.1:10000 20
//! ```
//!
//! Args: <endpoint> [seconds to run]

use geyser_tap_sdk::{GeyserClient, SubscriptionBuilder, Update};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().unwrap_or_else(|| "http://127.0.0.1:10000".to_string());
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);

    println!("connecting to {endpoint} ...");
    let mut client = GeyserClient::connect(&endpoint).await?;
    println!("connected");

    let pong = client.ping().await?;
    println!(
        "ping ok: server_timestamp_ns={} latency_ns={}",
        pong.server_timestamp_ns, pong.latency_ns
    );

    let subscription = SubscriptionBuilder::new()
        .accounts()
        .transactions()
        .include_votes(true)
        .include_failed(true)
        .slots()
        .entries()
        .block_metadata()
        .build();

    let mut stream = client.subscribe(subscription).await?;
    println!("subscribed; reading for {secs}s\n");

    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut samples_shown = 0usize;
    let mut total = 0u64;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        let next = match tokio::time::timeout(remaining, stream.next()).await {
            Err(_) => break,          // ran out the clock
            Ok(None) => {
                println!("stream closed by server");
                break;
            }
            Ok(Some(item)) => item,
        };

        let update = match next {
            Ok(u) => u,
            Err(e) => {
                eprintln!("stream error: {e}");
                break;
            }
        };

        total += 1;

        let kind = match &update {
            Update::Account(_) => "account",
            Update::Transaction(_) => "transaction",
            Update::Slot(_) => "slot",
            Update::Entry(_) => "entry",
            Update::BlockMetadata(_) => "block_metadata",
        };
        *counts.entry(kind).or_default() += 1;

        // Print the first few of each kind with real decoded field values, so
        // the output shows actual validator data rather than just a count.
        let shown_of_kind = counts[kind];
        if shown_of_kind <= 2 && samples_shown < 12 {
            samples_shown += 1;
            match &update {
                Update::Account(a) => println!(
                    "[account] pubkey={} owner={} slot={} lamports={} data_len={} executable={}",
                    a.pubkey, a.owner, a.slot, a.lamports, a.data.len(), a.executable
                ),
                Update::Transaction(t) => println!(
                    "[transaction] sig={} slot={} index={} is_vote={} tx_bytes={}",
                    t.signature, t.slot, t.index, t.is_vote, t.transaction.len()
                ),
                Update::Slot(s) => println!(
                    "[slot] slot={} parent={:?} status={:?}",
                    s.slot, s.parent, s.status
                ),
                Update::Entry(e) => println!(
                    "[entry] slot={} index={} num_hashes={} executed_txs={}",
                    e.slot, e.index, e.num_hashes, e.executed_transaction_count
                ),
                Update::BlockMetadata(b) => println!(
                    "[block_metadata] slot={} blockhash={} block_height={:?} block_time={:?}",
                    b.slot, b.blockhash, b.block_height, b.block_time
                ),
            }
        }
    }

    println!("\n=== RESULT ===");
    println!("total updates received: {total}");
    for (kind, n) in &counts {
        println!("  {kind}: {n}");
    }

    if total == 0 {
        eprintln!("\nFAIL: no updates received");
        std::process::exit(1);
    }
    println!("\nPASS: consumer received real data over gRPC");
    Ok(())
}
