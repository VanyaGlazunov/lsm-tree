//! Integration adapter for chunkfs Database trait.
//!
//! This module provides a synchronous wrapper around the async LSMStorage
//! to implement the chunkfs Database trait for chunk deduplication.
//!
//! **Note**: This module is only available when the `chunkfs-integration` feature is enabled.

use std::io::{self, ErrorKind};
use std::sync::Arc;

use bytes::Bytes;
use tokio::runtime::Runtime;

use crate::lsm_storage::LSMStorage;
use crate::memtable::{Memtable, ThreadSafeMemtable};

use chunkfs::{Data, DataContainer, Database};

/// Synchronous wrapper around LSMStorage for chunkfs integration.
///
/// # Example
///
/// ```ignore
/// use lsm_tree::chunkfs_adapter::LSMDatabaseAdapter;
/// use lsm_tree::memtable::BtreeMapMemtable;
/// use lsm_tree::options::LSMStorageOptions;
/// use tokio::runtime::Runtime;
///
/// let rt = Runtime::new().unwrap();
/// let storage = rt.block_on(async {
///     LSMStorageOptions::default()
///         .open::<BtreeMapMemtable>("./data")
///         .await
///         .unwrap()
/// });
///
/// // Create adapter with its own runtime
/// let mut adapter = LSMDatabaseAdapter::new(storage);
///
/// // Use with chunkfs Database trait (sync context)
/// adapter.insert(vec![1,2,3], vec![4,5,6]).unwrap();
/// let value = adapter.get(&vec![1,2,3]).unwrap();
/// ```
#[derive(Clone)]
pub struct LSMDatabaseAdapter<M>
where
    M: Memtable + Send + Sync + 'static,
{
    storage: Arc<LSMStorage<M>>,
    runtime: Arc<Runtime>,
}

impl<M> LSMDatabaseAdapter<M>
where
    M: Memtable + Send + Sync + 'static,
{
    /// Creates a new adapter with a specified number of worker threads.
    ///
    /// # Arguments
    ///
    /// * `storage` - The LSMStorage instance to wrap
    /// * `worker_threads` - Number of worker threads for the runtime
    ///
    /// # Panics
    ///
    /// Panics if unable to create the Tokio runtime.
    pub fn with_worker_threads(storage: LSMStorage<M>, worker_threads: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("lsm-chunkfs")
            .enable_all()
            .build()
            .expect("Failed to create runtime for adapter");

        Self {
            storage: Arc::new(storage),
            runtime: Arc::new(runtime),
        }
    }

    /// Consumes the adapter and returns the underlying storage.
    pub fn into_inner(self) -> Arc<LSMStorage<M>> {
        self.storage
    }

    /// Returns a reference to the underlying storage.
    pub fn storage(&self) -> &Arc<LSMStorage<M>> {
        &self.storage
    }
}

impl<K, M> Database<K, DataContainer<()>> for LSMDatabaseAdapter<M>
where
    K: AsRef<[u8]>,
    M: ThreadSafeMemtable,
{
    fn insert(&mut self, key: K, value: DataContainer<()>) -> io::Result<()> {
        let chunk_data = match value.extract() {
            Data::Chunk(data) => Bytes::copy_from_slice(data),
            Data::TargetChunk(_) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "LSM storage does not support TargetChunk storage",
                ));
            }
        };

        let storage = self.storage.clone();
        let key = key.as_ref().to_vec();

        self.runtime
            .block_on(async move { storage.insert(key, chunk_data).await })
            .map_err(|e| io::Error::new(ErrorKind::Other, e))
    }

    fn get(&self, key: &K) -> io::Result<DataContainer<()>> {
        let storage = self.storage.clone();
        let key = key.as_ref().to_vec();

        let result = self
            .runtime
            .block_on(async move { storage.get(&key).await })
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        match result {
            Some(bytes) => Ok(DataContainer::from(bytes.to_vec())),
            None => Err(io::Error::new(ErrorKind::NotFound, "Key not found")),
        }
    }

    fn contains(&self, key: &K) -> bool {
        let storage = self.storage.clone();
        let key = key.as_ref().to_vec();

        self.runtime
            .block_on(async move { storage.get(&key).await })
            .map(|opt| opt.is_some())
            .unwrap_or(false)
    }

    fn get_multi(&self, keys: &[K]) -> io::Result<Vec<DataContainer<()>>> {
        let storage = self.storage.clone();

        let results = self
            .runtime
            .block_on(async move { storage.get_batch(keys).await })
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        results
            .into_iter()
            .map(|opt| {
                opt.map(|bytes| DataContainer::from(bytes.to_vec()))
                    .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "Key not found"))
            })
            .collect()
    }

    fn insert_multi(&mut self, pairs: Vec<(K, DataContainer<()>)>) -> io::Result<()> {
        let mut processed_pairs = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let chunk_data = match value.extract() {
                Data::Chunk(data) => Bytes::copy_from_slice(data),
                Data::TargetChunk(_) => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        "LSM storage does not support TargetChunk storage",
                    ));
                }
            };
            processed_pairs.push((key.as_ref().to_vec(), chunk_data));
        }

        let storage = self.storage.clone();

        self.runtime
            .block_on(async move { storage.insert_batch(processed_pairs).await })
            .map_err(|e| io::Error::new(ErrorKind::Other, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::BtreeMapMemtable;
    use crate::options::LSMStorageOptions;
    use tempfile::tempdir;

    #[test]
    fn test_basic_operations() {
        let dir = tempdir().unwrap();

        let storage = {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                LSMStorageOptions::default()
                    .open::<BtreeMapMemtable>(&dir)
                    .await
                    .unwrap()
            })
        };

        let mut adapter = LSMDatabaseAdapter::with_worker_threads(storage, num_cpus::get());

        adapter
            .insert(b"key1", DataContainer::from(vec![1, 2, 3]))
            .unwrap();
        assert!(adapter.contains(&b"key1"));

        let result = adapter.get(&b"key1").unwrap();
        match result.extract() {
            Data::Chunk(data) => assert_eq!(data, &[1, 2, 3]),
            _ => panic!("Unexpected data type"),
        }

        assert!(!adapter.contains(&b"key2"));
        assert!(adapter.get(&b"key2").is_err());
    }

    #[test]
    fn test_multi_operations() {
        let dir = tempdir().unwrap();

        let storage = {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                LSMStorageOptions::default()
                    .open::<BtreeMapMemtable>(&dir)
                    .await
                    .unwrap()
            })
        };

        let mut adapter = LSMDatabaseAdapter::with_worker_threads(storage, num_cpus::get());

        let pairs = vec![
            (b"key1".to_vec(), DataContainer::from(vec![1, 2, 3])),
            (b"key2".to_vec(), DataContainer::from(vec![4, 5, 6])),
            (b"key3".to_vec(), DataContainer::from(vec![7, 8, 9])),
        ];
        adapter.insert_multi(pairs).unwrap();

        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let results = adapter.get_multi(&keys).unwrap();
        assert_eq!(results.len(), 3);

        match results[0].extract() {
            Data::Chunk(data) => assert_eq!(data, &[1, 2, 3]),
            _ => panic!("Unexpected data type"),
        }
    }

    #[test]
    fn test_concurrent_operations() {
        use std::sync::Mutex;
        use std::thread;

        let dir = tempdir().unwrap();

        let storage = {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                LSMStorageOptions::default()
                    .open::<BtreeMapMemtable>(&dir)
                    .await
                    .unwrap()
            })
        };

        let adapter = Arc::new(Mutex::new(LSMDatabaseAdapter::with_worker_threads(
            storage, 8,
        )));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let adapter = adapter.clone();
                thread::spawn(move || {
                    let key = format!("key{}", i).into_bytes();
                    let value = vec![i as u8; 10];

                    {
                        let mut adapter_guard = adapter.lock().unwrap();
                        adapter_guard
                            .insert(&key, DataContainer::from(value.clone()))
                            .unwrap();
                    }

                    {
                        let adapter_guard = adapter.lock().unwrap();
                        let result = adapter_guard.get(&key).unwrap();
                        match result.extract() {
                            Data::Chunk(data) => assert_eq!(data, &value[..]),
                            _ => panic!("Unexpected data type"),
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let adapter_guard = adapter.lock().unwrap();
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            assert!(adapter_guard.contains(&key));
        }
    }
}
