use std::{
    collections::{BTreeMap, BTreeSet},
    fs::create_dir_all,
    mem::replace,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::{
    self,
    sync::{Mutex, RwLock, RwLockWriteGuard},
};

use crate::{
    flush_system::{FlushCommand, FlushHandle, FlushSystem},
    lsm_utils::{flush_memtable, get_sst_path},
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    sstable::SSTable,
};

const TOMBSTONE: Record = Record::Delete;
pub(crate) const FLUSH_CHANNEL_SIZE: usize = 100;
const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_MEMTABLE_CAP: usize = 2;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;

pub type ImmutableMemtableMap<M> = Arc<RwLock<BTreeMap<usize, Arc<M>>>>;
pub type SSTableMap = Arc<RwLock<BTreeMap<usize, SSTable>>>;
pub type L0TaablesList = Arc<RwLock<Vec<usize>>>;

#[derive(Clone, PartialEq, Debug)]
pub enum Record {
    Put(Bytes),
    Delete,
}

impl Record {
    pub fn value_len(&self) -> usize {
        match self {
            Record::Put(val) => val.len(),
            Record::Delete => 0,
        }
    }

    pub fn put_from_slice(data: impl AsRef<[u8]>) -> Self {
        Record::Put(Bytes::copy_from_slice(data.as_ref()))
    }

    pub fn into_inner(self) -> Option<Bytes> {
        match self {
            Record::Delete => None,
            Record::Put(val) => Some(val),
        }
    }
}

/// Configuration for [LSMStorage]
#[derive(Clone)]
pub struct LSMStorageOptions {
    /// Size of a block in SSTable.
    pub block_size: usize,
    /// Size of memtables
    pub memtables_size: usize,
    // Number of immutable memtables stored in memory.
    #[allow(unused)]
    pub memtables_cap: usize,
    /// Number of async flush tasks
    pub num_flush_jobs: usize,
}

/// Main storage engine.
pub struct LSMStorage<M: Memtable + Send + Sync + 'static> {
    path: PathBuf,                          // Storage directory
    swap_lock: Mutex<()>,                   // Memtable swap synchronization
    memtable: RwLock<M>,                    // Active mutable memtable
    imm_memtables: ImmutableMemtableMap<M>, // Frozen memtables by IDs
    #[allow(dead_code)]
    l0_tables: L0TaablesList, // SSTable IDs in L0
    sstables: SSTableMap,                   // All SSTables by IDs
    options: Arc<LSMStorageOptions>,        // Configuration
    next_sst_id: Arc<AtomicUsize>,          // SSTable ID counter
    manifest: Arc<Mutex<Manifest>>,         // Recovery log
    flush_handle: Arc<FlushHandle<M>>,      // Background flush system handle
}

impl Default for LSMStorageOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            memtables_size: DEFAULT_MEMTABLE_SIZE,
            memtables_cap: DEFAULT_MEMTABLE_CAP,
            num_flush_jobs: DEFAULT_NUM_FLUSH_WORKERS,
        }
    }
}

impl<M: Memtable + Clone + Send + Sync + std::fmt::Debug> LSMStorage<M> {
    /// Opens/Creates storage in path given.
    pub fn open(path: impl AsRef<Path>, options: LSMStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut l0_tables = Vec::<usize>::new();
        let mut sstables = BTreeMap::<usize, SSTable>::new();

        let mut next_sst_id = 0_usize;

        if !path.exists() {
            create_dir_all(path).context("Failed to create DB directory")?;
        }

        let manifest_path = path.join("MANIFEST");
        let manifest;
        // recover sstables from manifest...
        if manifest_path.exists() {
            let records: Vec<ManifestRecord>;
            (manifest, records) = Manifest::recover(manifest_path)?;
            let mut memtables = BTreeSet::<usize>::new();
            for record in records {
                match record {
                    ManifestRecord::NewMemtable(id) => {
                        memtables.insert(id);
                    }
                    ManifestRecord::Flush(id) => {
                        let removed = memtables.remove(&id);
                        if !removed {
                            anyhow::bail!("Wrong id for flushed memtable {id}. Can not recover.")
                        }

                        l0_tables.push(id);
                        next_sst_id = next_sst_id.max(id);
                    }
                }
            }
            next_sst_id += 1;
            for id in &l0_tables {
                let sstable_path = get_sst_path(path, *id);
                let sstable = SSTable::open(&sstable_path)
                    .context(format!("Cannot open sst {sstable_path:?}"))?;
                sstables.insert(*id, sstable);
            }
        } else {
            manifest = Manifest::new(manifest_path)?;
        }

        // TODO: recover memtables from WALs..

        manifest.add_record(ManifestRecord::NewMemtable(next_sst_id))?;

        let memtable = RwLock::new(M::new(next_sst_id));
        let imm_memtables: ImmutableMemtableMap<M> = Arc::new(RwLock::new(BTreeMap::new()));
        let l0_tables = Arc::new(RwLock::new(l0_tables));
        let sstables = Arc::new(RwLock::new(sstables));
        let manifest = Arc::new(Mutex::new(manifest));

        let flush_system = FlushSystem {
            imm_memtables: imm_memtables.clone(),
            l0_tables: l0_tables.clone(),
            sstables: sstables.clone(),
            manifest: manifest.clone(),
            path: path.to_path_buf(),
            options: options.clone(),
        };
        let flush_handle = flush_system.init();

        Ok(Self {
            swap_lock: Mutex::new(()),
            memtable,
            imm_memtables,
            l0_tables,
            sstables,
            path: path.to_path_buf(),
            next_sst_id: Arc::new(AtomicUsize::new(next_sst_id)),
            options: Arc::new(options),
            manifest,
            flush_handle: Arc::new(flush_handle),
        })
    }

    /// Graceful shutdown (waits until all data will be flushed).
    pub async fn close(self) -> Result<()> {
        // Destructure self to take ownership of all fields
        // Needed to solve partially moved problems
        let LSMStorage {
            path,
            swap_lock: _,
            memtable,
            imm_memtables,
            l0_tables: _,
            sstables: _,
            options,
            next_sst_id: _,
            manifest,
            flush_handle,
        } = self;

        let flush_handle = Arc::try_unwrap(flush_handle).unwrap();
        flush_handle.sender.send(FlushCommand::Shutdown).await?;
        flush_handle.handle.await?;

        let memtable = memtable.into_inner();
        if memtable.size_estimate() > 0 {
            let id = memtable.get_id();
            flush_memtable(options.block_size, &path, &memtable).await?;
            manifest
                .lock()
                .await
                .add_record(ManifestRecord::Flush(id))?;
        }

        let imm_memtables = Arc::try_unwrap(imm_memtables).unwrap().into_inner();
        for memtable in imm_memtables.values() {
            let id = memtable.get_id();
            flush_memtable(options.block_size, &path, &**memtable).await?;

            manifest
                .lock()
                .await
                .add_record(ManifestRecord::Flush(id))?;
        }

        Ok(())
    }

    /// Inserts key-value pair.
    ///
    /// # Errors
    /// Returns error if key is empty.
    pub async fn insert(&self, key: impl AsRef<[u8]>, value: Bytes) -> Result<()> {
        self.insert_inner(key, Record::Put(value)).await
    }

    /// Retrieves value for key.
    pub async fn get(&self, key: &impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let memtable = self.memtable.read().await;
        let key = key.as_ref();

        let mut candidate = memtable.get(key);
        if let Some(rec) = candidate {
            return Ok(rec.into_inner());
        }

        let imm_memtables = self.imm_memtables.read().await;
        for memtable in imm_memtables.values() {
            let value = memtable.get(key);
            if value.is_some() {
                candidate = value;
            }
        }

        if let Some(rec) = candidate {
            return Ok(rec.into_inner());
        }

        drop(memtable);
        drop(imm_memtables);

        let sstables = self.sstables.read().await;
        for sst in sstables.values() {
            let value = sst.get(key)?;
            if value.is_some() {
                candidate = value;
            }
        }

        if let Some(rec) = candidate {
            return Ok(rec.into_inner());
        }

        drop(sstables);

        Ok(None)
    }

    /// Deletes key.
    ///
    /// # Errors
    /// Returns error if key is empty.
    pub async fn delete(&self, key: &impl AsRef<[u8]>) -> Result<()> {
        self.insert_inner(key, TOMBSTONE).await
    }

    async fn insert_inner(&self, key: impl AsRef<[u8]>, value: Record) -> Result<()> {
        let key = Bytes::copy_from_slice(key.as_ref());
        if key.is_empty() {
            anyhow::bail!("Empty keys are not allowed");
        }

        let mut memtable = self.memtable.write().await;
        memtable.set(key, value.clone());

        if memtable.size_estimate() > self.options.memtables_size {
            self.flush(memtable).await.context("Failed to flush")?;
        }

        Ok(())
    }

    /// Forces flush of current memtable.
    pub async fn force_flush(&self) -> Result<()> {
        let memtable = self.memtable.write().await;
        self.flush(memtable).await
    }

    async fn flush(&self, mut memtable: RwLockWriteGuard<'_, M>) -> Result<()> {
        let old_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let new_memtable = M::new(old_id + 1);

        let old_memtable = {
            let _lock = self.swap_lock.lock().await;
            replace(&mut *memtable, new_memtable)
        };
        let old_memtable = Arc::new(old_memtable);

        self.manifest
            .lock()
            .await
            .add_record(ManifestRecord::NewMemtable(old_id))
            .context("Failed to add NewMemtable record to manifest")?;

        self.imm_memtables
            .write()
            .await
            .insert(old_id, old_memtable.clone());

        drop(memtable);

        self.flush_handle
            .sender
            .send(FlushCommand::Memtable(old_memtable))
            .await
            .context("Failed to send memtable into flush channel")?;

        Ok(())
    }
}

#[cfg(test)]
mod lsm_storage_tests {
    use std::time::Duration;

    use super::*;
    use crate::memtable::BtreeMapMemtable;
    use tempfile::tempdir;

    type Storage = LSMStorage<BtreeMapMemtable>;

    #[tokio::test]
    async fn test_basic_crud() -> Result<()> {
        let dir = tempdir()?;
        let storage = Storage::open(&dir, LSMStorageOptions::default())?;

        let expected = Bytes::from("val1");
        let key = b"key";
        storage.insert(&key, expected.clone()).await?;
        let actual = storage.get(&key).await?;

        assert_eq!(actual, Some(expected));

        storage.delete(&key).await?;
        let actual = storage.get(&key).await?;

        assert_eq!(actual, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_persistence() -> Result<()> {
        let dir = tempdir()?;

        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        let key = b"key";
        let expected = Bytes::from("data");
        storage.insert(&key, expected.clone()).await?;
        storage.close().await?;

        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_recovery() -> Result<()> {
        let dir = tempdir()?;
        let storage = Storage::open(
            &dir,
            LSMStorageOptions {
                memtables_size: 1,
                ..Default::default()
            },
        )?;

        let key = b"key";
        let expected = Bytes::from("data");
        storage.insert(key.clone(), expected.clone()).await?;
        tokio::time::sleep(Duration::from_secs(1)).await; // Wait for flush
        storage.close().await?;

        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        let actual = storage.get(key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }
}
