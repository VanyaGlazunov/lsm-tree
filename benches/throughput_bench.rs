//! LSM-tree throughput benchmarks
//!
//! Measures write and read throughput (MB/s) for single-threaded performance
//! comparing BTreeMap and SkipList LSM memtable implementations with standalone B+Tree

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lsm_tree::{memtable::BtreeMapMemtable, options::LSMStorageOptions};
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const CHUNK_SIZE: usize = 32 * 1024;
const TOTAL_OPS: usize = 100_000;

fn generate_key(index: usize) -> Vec<u8> {
    format!("key_{:016x}", index).into_bytes()
}

fn generate_value(seed: usize) -> Bytes {
    let mut data = vec![0u8; CHUNK_SIZE];
    for i in 0..CHUNK_SIZE {
        data[i] = ((seed + i) % 256) as u8;
    }
    Bytes::from(data)
}

fn get_options() -> LSMStorageOptions {
    LSMStorageOptions::default()
        .memtable_size(1 << 32)
        .max_l0_ssts(8)
}

fn bench_lsm_btree_write_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lsm_btree_write_throughput");

    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    group.sample_size(20);

    let total_bytes = TOTAL_OPS * CHUNK_SIZE;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function("single_thread", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let storage = rt.block_on(async {
                    get_options()
                        .open::<BtreeMapMemtable>(temp_dir.path())
                        .await
                        .unwrap()
                });
                let mut keys = Vec::with_capacity(TOTAL_OPS);
                for op_id in 0..TOTAL_OPS {
                    keys.push(generate_key(op_id));
                }
                (temp_dir, storage, keys)
            },
            |(temp_dir, storage, keys)| {
                rt.block_on(async {
                    let _ = temp_dir;
                    for (op_id, key) in keys.into_iter().enumerate() {
                        let value = generate_value(op_id);
                        storage.insert(&key, value).await.unwrap();
                    }
                })
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_lsm_btree_read_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lsm_btree_read_throughput");

    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    group.sample_size(20);

    let total_bytes = TOTAL_OPS * CHUNK_SIZE;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function("single_thread", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let storage = rt.block_on(async {
                    let storage = get_options()
                        .open::<BtreeMapMemtable>(temp_dir.path())
                        .await
                        .unwrap();

                    for key_id in 0..TOTAL_OPS {
                        let key = generate_key(key_id);
                        let value = generate_value(key_id);
                        storage.insert(&key, value).await.unwrap();
                    }
                    storage
                });
                let mut keys = Vec::with_capacity(TOTAL_OPS);
                for op_id in 0..TOTAL_OPS {
                    keys.push(generate_key(op_id));
                }
                (temp_dir, storage, keys)
            },
            |(temp_dir, storage, keys)| {
                rt.block_on(async {
                    let _ = temp_dir;
                    for key in keys {
                        let _ = storage.get(&key).await.unwrap();
                    }
                })
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_lsm_btree_write_throughput,
    bench_lsm_btree_read_throughput,
);
criterion_main!(benches);
