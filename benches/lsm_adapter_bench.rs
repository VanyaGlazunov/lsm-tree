//! Benchmarks for LSM-tree chunkfs adapter

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use chunkfs::bench::Dataset;
use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{create_cdc_filesystem, ChunkerRef};

use lsm_tree::chunkfs_adapter::LSMDatabaseAdapter;
use lsm_tree::memtable::{BtreeMapMemtable, SkipListMemtable};
use lsm_tree::options::LSMStorageOptions;

const SAMPLE_SIZE: usize = 30;

#[derive(Clone, Debug)]
enum Backend {
    HashMap,
    LSMBTree,
    LSMSkipList,
}

#[derive(Copy, Clone, Debug)]
enum Algorithms {
    Rabin,
    Leap,
    Super,
    Ultra,
}

fn chunkers() -> Vec<Algorithms> {
    vec![
        Algorithms::Rabin,
        Algorithms::Leap,
        Algorithms::Super,
        Algorithms::Ultra,
    ]
}

fn get_chunker(algorithm: Algorithms) -> ChunkerRef {
    match algorithm {
        Algorithms::Rabin => RabinChunker::default().into(),
        Algorithms::Leap => LeapChunker::default().into(),
        Algorithms::Super => SuperChunker::default().into(),
        Algorithms::Ultra => UltraChunker::default().into(),
    }
}

fn backends() -> Vec<Backend> {
    vec![Backend::HashMap, Backend::LSMBTree, Backend::LSMSkipList]
}

pub fn bench(c: &mut Criterion) {
    let datasets = vec![Dataset::new("arch_images.tar", "arch_images").unwrap()];

    for dataset in datasets {
        let mut group = c.benchmark_group("ChunkFS-Backends");
        group.sample_size(SAMPLE_SIZE);
        group.throughput(Throughput::Bytes(dataset.size as u64));

        for backend in backends() {
            for chunker in chunkers() {
                bench_write(&dataset, &mut group, backend.clone(), chunker);
            }
        }

        for backend in backends() {
            for chunker in chunkers() {
                bench_read(&dataset, &mut group, backend.clone(), chunker);
            }
        }
    }
}

fn bench_write(
    dataset: &Dataset,
    group: &mut BenchmarkGroup<WallTime>,
    backend: Backend,
    algorithm: Algorithms,
) {
    let bench_name = &dataset.name;
    let parameter = format!("write-{:?}-{:?}", backend, algorithm);

    match backend {
        Backend::HashMap => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let mut fs =
                            create_cdc_filesystem(HashMap::default(), Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let handle = fs.create_file("file", chunker).unwrap();
                        (fs, handle, data)
                    },
                    |(mut fs, mut handle, data)| {
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::LSMBTree => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let rt = Runtime::new().unwrap();
                        let storage = rt.block_on(async {
                            LSMStorageOptions::default()
                                .memtable_size(16 * 1024 * 1024)
                                .max_l0_ssts(8)
                                .open::<BtreeMapMemtable>(temp_dir.path())
                                .await
                                .unwrap()
                        });
                        let adapter = LSMDatabaseAdapter::with_worker_threads(storage, 4);
                        let mut fs = create_cdc_filesystem(adapter, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let handle = fs.create_file("file", chunker).unwrap();
                        (fs, handle, data, temp_dir)
                    },
                    |(mut fs, mut handle, data, _temp_dir)| {
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::LSMSkipList => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let rt = Runtime::new().unwrap();
                        let storage = rt.block_on(async {
                            LSMStorageOptions::default()
                                .memtable_size(16 * 1024 * 1024)
                                .max_l0_ssts(8)
                                .open::<SkipListMemtable>(temp_dir.path())
                                .await
                                .unwrap()
                        });
                        let adapter = LSMDatabaseAdapter::with_worker_threads(storage, 4);
                        let mut fs = create_cdc_filesystem(adapter, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let handle = fs.create_file("file", chunker).unwrap();
                        (fs, handle, data, temp_dir)
                    },
                    |(mut fs, mut handle, data, _temp_dir)| {
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
}

fn bench_read(
    dataset: &Dataset,
    group: &mut BenchmarkGroup<WallTime>,
    backend: Backend,
    algorithm: Algorithms,
) {
    let bench_name = &dataset.name;
    let parameter = format!("read-{:?}-{:?}", backend, algorithm);

    match backend {
        Backend::HashMap => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let base = HashMap::default();
                        let mut fs = create_cdc_filesystem(base, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let mut handle = fs.create_file("file", chunker).unwrap();
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        let handle = fs.open_file_readonly("file").unwrap();
                        (fs, handle)
                    },
                    |(fs, handle)| {
                        fs.read_file_complete(&handle).unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::LSMBTree => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let rt = Runtime::new().unwrap();
                        let storage = rt.block_on(async {
                            LSMStorageOptions::default()
                                .memtable_size(16 * 1024 * 1024)
                                .max_l0_ssts(8)
                                .open::<BtreeMapMemtable>(temp_dir.path())
                                .await
                                .unwrap()
                        });
                        let adapter = LSMDatabaseAdapter::with_worker_threads(storage, 4);
                        let mut fs = create_cdc_filesystem(adapter, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let mut handle = fs.create_file("file", chunker).unwrap();
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        let handle = fs.open_file_readonly("file").unwrap();
                        (fs, handle, temp_dir)
                    },
                    |(fs, handle, _temp_dir)| {
                        fs.read_file_complete(&handle).unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::LSMSkipList => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let rt = Runtime::new().unwrap();
                        let storage = rt.block_on(async {
                            LSMStorageOptions::default()
                                .memtable_size(16 * 1024 * 1024)
                                .max_l0_ssts(8)
                                .open::<SkipListMemtable>(temp_dir.path())
                                .await
                                .unwrap()
                        });
                        let adapter = LSMDatabaseAdapter::with_worker_threads(storage, 4);
                        let mut fs = create_cdc_filesystem(adapter, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let mut handle = fs.create_file("file", chunker).unwrap();
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        let handle = fs.open_file_readonly("file").unwrap();
                        (fs, handle, temp_dir)
                    },
                    |(fs, handle, _temp_dir)| {
                        fs.read_file_complete(&handle).unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
}

pub fn benches() {
    let mut criterion: Criterion<_> = Criterion::default().configure_from_args();
    bench(&mut criterion);
}

fn main() {
    benches();
    Criterion::default().configure_from_args().final_summary();
}
