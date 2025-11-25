//! Benchmarks for LSM-tree chunkfs adapter

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use rocksdb::{DBCompressionType, Options, DB};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use chunkfs::bench::Dataset;
use chunkfs::chunkers::{LeapChunker, RabinChunker, SuperChunker, UltraChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{create_cdc_filesystem, ChunkerRef, Data, DataContainer, Database};

use bplus_tree::bplus_tree::BPlusStorage;
use lsm_tree::chunkfs_adapter::LSMDatabaseAdapter;
use lsm_tree::memtable::BtreeMapMemtable;
use lsm_tree::options::LSMStorageOptions;

const SAMPLE_SIZE: usize = 10;

/// RocksDB adapter for ChunkFS
#[derive(Clone)]
struct RocksDBAdapter {
    db: Arc<DB>,
}

impl RocksDBAdapter {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(1 << 25);
        opts.set_max_write_buffer_number(4);
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_level_zero_file_num_compaction_trigger(32);
        let db = DB::open(&opts, path)?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl<K> Database<K, DataContainer<()>> for RocksDBAdapter
where
    K: AsRef<[u8]> + Clone + Send + Sync,
{
    fn get(&self, key: &K) -> Result<DataContainer<()>, std::io::Error> {
        self.db
            .get(key.as_ref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
            .map(|v| DataContainer::from(v.to_vec()))
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found"))
    }

    fn insert(&mut self, key: K, value: DataContainer<()>) -> std::io::Result<()> {
        let chunk_data = match value.extract() {
            Data::Chunk(data) => data,
            Data::TargetChunk(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "RocksDB adapter does not support TargetChunk storage",
                ));
            }
        };

        self.db
            .put(key.as_ref(), chunk_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn contains(&self, key: &K) -> bool {
        self.db.get(key.as_ref()).unwrap_or(None).is_some()
    }
}

#[derive(Clone, Debug)]
enum Backend {
    HashMap,
    RocksDB,
    LSMBTree,
    BPlus,
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
    vec![
        Backend::HashMap,
        Backend::RocksDB,
        Backend::BPlus,
        Backend::LSMBTree,
    ]
}

pub fn bench(c: &mut Criterion) {
    let datasets = vec![Dataset::new("./benches/data/arch_images.tar", "arch_images").unwrap()];

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
        Backend::BPlus => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().to_path_buf();
                        let rt = Runtime::new().unwrap();
                        let storage = BPlusStorage::new(rt, 100, path).unwrap();
                        let mut fs = create_cdc_filesystem(storage, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let handle = fs.create_file("file", chunker).unwrap();
                        (fs, handle, data, temp_dir)
                    },
                    |(mut fs, mut handle, data, _temp_dir)| {
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        drop(fs);
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
                                .memtable_size(1 << 27)
                                .max_l0_ssts(32)
                                .open::<BtreeMapMemtable>(temp_dir.path())
                                .await
                                .unwrap()
                        });
                        let adapter = LSMDatabaseAdapter::with_runtime(storage, rt);
                        let mut fs =
                            create_cdc_filesystem(adapter.clone(), Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let handle = fs.create_file("file", chunker).unwrap();
                        (fs, handle, data, temp_dir)
                    },
                    |(mut fs, mut handle, data, _temp_dir)| {
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        drop(fs);
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::HashMap => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let storage = HashMap::new();
                        let mut fs = create_cdc_filesystem(storage, Sha256Hasher::default());
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
        Backend::RocksDB => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let storage = RocksDBAdapter::new(temp_dir.path()).unwrap();
                        let mut fs = create_cdc_filesystem(storage, Sha256Hasher::default());
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
        Backend::LSMBTree => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let rt = Runtime::new().unwrap();
                        let storage = rt.block_on(async {
                            LSMStorageOptions::default()
                                .memtable_size(1 << 27)
                                .max_l0_ssts(32)
                                .open::<BtreeMapMemtable>(temp_dir.path())
                                .await
                                .unwrap()
                        });
                        let adapter = LSMDatabaseAdapter::with_runtime(storage, rt);
                        let mut fs =
                            create_cdc_filesystem(adapter.clone(), Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let mut handle = fs.create_file("file", chunker).unwrap();
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        let handle = fs.open_file_readonly("file").unwrap();
                        (fs, handle, temp_dir)
                    },
                    |(fs, handle, _temp_dir)| {
                        fs.read_file_complete(&handle).unwrap();
                        drop(fs);
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::BPlus => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().to_path_buf();
                        let rt = Runtime::new().unwrap();
                        let storage = BPlusStorage::new(rt, 100, path).unwrap();
                        let mut fs = create_cdc_filesystem(storage, Sha256Hasher::default());
                        let chunker = get_chunker(algorithm);
                        let mut handle = fs.create_file("file", chunker).unwrap();
                        fs.write_from_stream(&mut handle, data).unwrap();
                        fs.close_file(handle).unwrap();
                        let handle = fs.open_file_readonly("file").unwrap();
                        (fs, handle, temp_dir)
                    },
                    |(fs, handle, _temp_dir)| {
                        fs.read_file_complete(&handle).unwrap();
                        drop(fs);
                    },
                    BatchSize::PerIteration,
                )
            });
        }
        Backend::HashMap => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let storage = HashMap::new();
                        let mut fs = create_cdc_filesystem(storage, Sha256Hasher::default());
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
        Backend::RocksDB => {
            group.bench_function(BenchmarkId::new(bench_name, &parameter), |b| {
                b.iter_batched(
                    || {
                        let data = BufReader::new(File::open(&dataset.path).unwrap());
                        let temp_dir = TempDir::new().unwrap();
                        let storage = RocksDBAdapter::new(temp_dir.path()).unwrap();
                        let mut fs = create_cdc_filesystem(storage, Sha256Hasher::default());
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
