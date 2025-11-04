//! LSM-tree concurrency scalability benchmarks
//!
//! Measures write and read throughput (MB/s) at different concurrency levels
//! comparing BTreeMap and SkipList memtable implementations

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lsm_tree::{
    memtable::{BtreeMapMemtable, SkipListMemtable},
    options::LSMStorageOptions,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const CHUNK_SIZE: usize = 32 * 1024;
const OPS_PER_THREAD: usize = 10_000;
const THREAD_COUNTS: [usize; 5] = [1, 2, 4, 8, 16];

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

fn bench_lsm_btree_write_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lsm_btree_write");

    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for concurrency in THREAD_COUNTS {
        let total_ops = concurrency * OPS_PER_THREAD;
        let total_bytes = total_ops * CHUNK_SIZE;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_function(BenchmarkId::from_parameter(concurrency), |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let storage = rt.block_on(async {
                        LSMStorageOptions::default()
                            .memtable_size(8 * 1024 * 1024)
                            .max_l0_ssts(4)
                            .open::<BtreeMapMemtable>(temp_dir.path())
                            .await
                            .unwrap()
                    });
                    (temp_dir, Arc::new(storage))
                },
                |(temp_dir, storage)| {
                    rt.block_on(async {
                        let _ = temp_dir;
                        let mut handles = vec![];
                        for thread_id in 0..concurrency {
                            let storage = storage.clone();
                            let handle = tokio::spawn(async move {
                                for op_id in 0..OPS_PER_THREAD {
                                    let key_id = thread_id * OPS_PER_THREAD + op_id;
                                    let key = generate_key(key_id);
                                    let value = generate_value(key_id);
                                    storage.insert(&key, value).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_lsm_skiplist_write_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lsm_skiplist_write");

    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for concurrency in THREAD_COUNTS {
        let total_ops = concurrency * OPS_PER_THREAD;
        let total_bytes = total_ops * CHUNK_SIZE;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_function(BenchmarkId::from_parameter(concurrency), |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let storage = rt.block_on(async {
                        LSMStorageOptions::default()
                            .memtable_size(8 * 1024 * 1024) // 8MB memtable
                            .max_l0_ssts(4)
                            .open::<SkipListMemtable>(temp_dir.path())
                            .await
                            .unwrap()
                    });
                    (temp_dir, Arc::new(storage))
                },
                |(temp_dir, storage)| {
                    rt.block_on(async {
                        let _ = temp_dir;
                        let mut handles = vec![];
                        for thread_id in 0..concurrency {
                            let storage = storage.clone();
                            let handle = tokio::spawn(async move {
                                for op_id in 0..OPS_PER_THREAD {
                                    let key_id = thread_id * OPS_PER_THREAD + op_id;
                                    let key = generate_key(key_id);
                                    let value = generate_value(key_id);
                                    storage.insert(&key, value).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_lsm_btree_read_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lsm_btree_read");

    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for concurrency in THREAD_COUNTS {
        let total_ops = concurrency * OPS_PER_THREAD;
        let total_bytes = total_ops * CHUNK_SIZE;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_function(BenchmarkId::from_parameter(concurrency), |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let storage = rt.block_on(async {
                        let storage = LSMStorageOptions::default()
                            .memtable_size(8 * 1024 * 1024)
                            .max_l0_ssts(4)
                            .open::<BtreeMapMemtable>(temp_dir.path())
                            .await
                            .unwrap();

                        for key_id in 0..total_ops {
                            let key = generate_key(key_id);
                            let value = generate_value(key_id);
                            storage.insert(&key, value).await.unwrap();
                        }
                        storage
                    });
                    (temp_dir, Arc::new(storage))
                },
                |(temp_dir, storage)| {
                    rt.block_on(async {
                        let _ = temp_dir;
                        let mut handles = vec![];
                        for thread_id in 0..concurrency {
                            let storage = storage.clone();
                            let handle = tokio::spawn(async move {
                                for op_id in 0..OPS_PER_THREAD {
                                    let key_id = thread_id * OPS_PER_THREAD + op_id;
                                    let key = generate_key(key_id);
                                    let _ = storage.get(&key).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_lsm_skiplist_read_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lsm_skiplist_read");

    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for concurrency in THREAD_COUNTS {
        let total_ops = concurrency * OPS_PER_THREAD;
        let total_bytes = total_ops * CHUNK_SIZE;
        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_function(BenchmarkId::from_parameter(concurrency), |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let storage = rt.block_on(async {
                        let storage = LSMStorageOptions::default()
                            .memtable_size(8 * 1024 * 1024)
                            .max_l0_ssts(4)
                            .open::<SkipListMemtable>(temp_dir.path())
                            .await
                            .unwrap();

                        for key_id in 0..total_ops {
                            let key = generate_key(key_id);
                            let value = generate_value(key_id);
                            storage.insert(&key, value).await.unwrap();
                        }
                        storage
                    });
                    (temp_dir, Arc::new(storage))
                },
                |(temp_dir, storage)| {
                    rt.block_on(async {
                        let _ = temp_dir;
                        let mut handles = vec![];
                        for thread_id in 0..concurrency {
                            let storage = storage.clone();
                            let handle = tokio::spawn(async move {
                                for op_id in 0..OPS_PER_THREAD {
                                    let key_id = thread_id * OPS_PER_THREAD + op_id;
                                    let key = generate_key(key_id);
                                    let _ = storage.get(&key).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_lsm_btree_write_throughput,
    bench_lsm_skiplist_write_throughput,
    bench_lsm_btree_read_throughput,
    bench_lsm_skiplist_read_throughput
);
criterion_main!(benches);
