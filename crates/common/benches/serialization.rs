//! Serialization benchmarks for geyser-tap.
//!
//! Run with: cargo bench -p geyser-tap-common

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use geyser_tap_common::{AccountUpdate, SlotStatus, SlotUpdate, TransactionUpdate, Update};

fn create_account_update(data_size: usize) -> Update {
    Update::Account(AccountUpdate {
        pubkey: [1u8; 32],
        data: Bytes::from(vec![0u8; data_size]),
        slot: 12345678,
        owner: [2u8; 32],
        lamports: 1_000_000_000,
        rent_epoch: 123,
        executable: false,
        write_version: 999,
        txn_signature: Some([3u8; 64]),
    })
}

fn create_transaction_update(data_size: usize) -> Update {
    Update::Transaction(TransactionUpdate {
        signature: [1u8; 64],
        transaction_data: Bytes::from(vec![0u8; data_size]),
        slot: 12345678,
        index: 42,
        is_vote: false,
        meta: Some(Bytes::from(vec![0u8; 100])),
    })
}

fn create_slot_update() -> Update {
    Update::Slot(SlotUpdate {
        slot: 12345678,
        parent: Some(12345677),
        status: SlotStatus::Processed,
    })
}

fn bench_update_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_clone");

    // Small account (1KB)
    let small_account = create_account_update(1024);
    group.throughput(Throughput::Bytes(1024));
    group.bench_function("account_1kb", |b| {
        b.iter(|| black_box(small_account.clone()))
    });

    // Large account (1MB)
    let large_account = create_account_update(1024 * 1024);
    group.throughput(Throughput::Bytes(1024 * 1024));
    group.bench_function("account_1mb", |b| {
        b.iter(|| black_box(large_account.clone()))
    });

    // Transaction (2KB payload) - the hot path in practice
    let transaction = create_transaction_update(2048);
    group.throughput(Throughput::Bytes(2048));
    group.bench_function("transaction_2kb", |b| {
        b.iter(|| black_box(transaction.clone()))
    });

    // Slot update (minimal)
    let slot = create_slot_update();
    group.throughput(Throughput::Elements(1));
    group.bench_function("slot", |b| b.iter(|| black_box(slot.clone())));

    group.finish();
}

fn bench_update_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_size");

    let small_account = create_account_update(1024);
    group.bench_function("account_1kb", |b| {
        b.iter(|| black_box(small_account.size_bytes()))
    });

    let large_account = create_account_update(1024 * 1024);
    group.bench_function("account_1mb", |b| {
        b.iter(|| black_box(large_account.size_bytes()))
    });

    group.finish();
}

criterion_group!(benches, bench_update_clone, bench_update_size);
criterion_main!(benches);
