use std::env::temp_dir;
use std::fs;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rand::{thread_rng, Rng};

use lsm_tree::lsm_tree::LSMtree;

const CHUNK_SIZE: usize = 32 * 1024;
const NUM_OPERATIONS: usize = 1_000_000;
const MEMTABLE_THRESHOLD: usize = 8 * 1024 * 1024;

fn generate_key(index: usize) -> Vec<u8> {
    format!("key_{:016x}", index).into_bytes()
}

fn generate_value(seed: usize) -> Vec<u8> {
    let mut data = vec![0u8; CHUNK_SIZE];
    for i in 0..CHUNK_SIZE {
        data[i] = ((seed + i) % 256) as u8;
    }
    data
}

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_write_throughput");
    group.sample_size(20);
    let total_bytes = CHUNK_SIZE * NUM_OPERATIONS;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function(BenchmarkId::new("random_32kb", NUM_OPERATIONS), |b| {
        b.iter_batched(
            || {
                let temp_path = temp_dir().join(format!("lsm_bench_{}", thread_rng().gen::<u64>()));
                fs::create_dir_all(&temp_path).unwrap();

                let lsm = LSMtree::new(&temp_path, MEMTABLE_THRESHOLD);

                (lsm, temp_path)
            },
            |(mut lsm, temp_path)| {
                for op in 0..NUM_OPERATIONS {
                    lsm.insert(generate_key(op), generate_value(op)).unwrap();
                }

                drop(lsm);
                let _ = fs::remove_dir_all(temp_path);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_read_throughput");

    group.sample_size(20);
    let total_bytes = CHUNK_SIZE * NUM_OPERATIONS;
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function(BenchmarkId::new("random_32kb", NUM_OPERATIONS), |b| {
        b.iter_batched(
            || {
                // Setup: create and populate LSM tree
                let temp_path = temp_dir().join(format!("lsm_bench_{}", thread_rng().gen::<u64>()));
                fs::create_dir_all(&temp_path).unwrap();

                let mut lsm = LSMtree::new(&temp_path, MEMTABLE_THRESHOLD);

                // Pre-populate the tree
                for op in 0..NUM_OPERATIONS {
                    lsm.insert(generate_key(op), generate_value(op)).unwrap();
                }

                (lsm, temp_path)
            },
            |(lsm, temp_path)| {
                for op in 0..NUM_OPERATIONS {
                    let _ = lsm.get(&generate_key(op)).unwrap();
                }

                drop(lsm);
                let _ = fs::remove_dir_all(temp_path);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_write_throughput, bench_read_throughput,);
criterion_main!(benches);
