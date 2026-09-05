//! Reads messages back off a geyser-tap Kafka topic and decodes them, to
//! confirm the producer is publishing well-formed updates rather than just
//! bytes.
//!
//! ```sh
//! cargo run --release -p geyser-tap-sink-kafka --example verify_topic -- \
//!     localhost:9092 solana-transactions 5
//! ```

use prost::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message as _};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let brokers = args.next().unwrap_or_else(|| "localhost:9092".into());
    let topic = args.next().unwrap_or_else(|| "solana-transactions".into());
    let want: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(5);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "geyser-tap-verify")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&topic])?;
    println!("reading up to {want} messages from {topic} on {brokers}\n");

    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut seen = 0usize;

    while seen < want && std::time::Instant::now() < deadline {
        let Some(result) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        let msg = result?;
        let payload = msg.payload().unwrap_or_default();
        let key_len = msg.key().map(|k| k.len()).unwrap_or(0);

        let update = geyser_tap_proto::geyser::StreamUpdate::decode(payload)?;
        seen += 1;

        print!(
            "offset={} partition={} key_len={} payload_len={} seq={} ",
            msg.offset(),
            msg.partition(),
            key_len,
            payload.len(),
            update.sequence
        );

        use geyser_tap_proto::geyser::UpdatePayload as P;
        match update.payload {
            Some(P::Transaction(t)) => println!(
                "TRANSACTION sig={} slot={} is_vote={} tx_bytes={}",
                bs58::encode(&t.signature).into_string(),
                t.slot,
                t.is_vote,
                t.transaction.len()
            ),
            Some(P::Account(a)) => println!(
                "ACCOUNT pubkey={} owner={} slot={} lamports={} data_len={}",
                bs58::encode(&a.pubkey).into_string(),
                bs58::encode(&a.owner).into_string(),
                a.slot,
                a.lamports,
                a.data.len()
            ),
            Some(P::Slot(s)) => println!("SLOT slot={} parent={:?}", s.slot, s.parent),
            Some(P::BlockMetadata(b)) => {
                println!(
                    "BLOCK_METADATA slot={} blockhash={}",
                    b.slot,
                    bs58::encode(&b.blockhash).into_string()
                )
            }
            Some(P::Entry(e)) => println!("ENTRY slot={} index={}", e.slot, e.index),
            None => println!("EMPTY PAYLOAD"),
        }
    }

    println!("\ndecoded {seen} message(s) from {topic}");
    if seen == 0 {
        eprintln!("FAIL: no messages decoded");
        std::process::exit(1);
    }
    println!("PASS: messages on the topic decode as geyser-tap StreamUpdates");
    Ok(())
}
