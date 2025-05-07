use std::{
    collections::{BTreeSet, HashMap},
    fs::create_dir_all,
    mem,
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
    sync::{Mutex, RwLock},
};

use crate::{
    flush_system::{FlushCommand, FlushHandle, FlushSystem},
    lsm_utils::{flush_memtable, get_sst_path},
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    sstable::SSTable,
};

// TODO: Fix tombstone logic. i.e user shouldn't be able to replicate tombstone.
const TOMBSTONE: Record = Record::Delete;
pub const FLUSH_CHANNEL_SIZE: usize = 100;
const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_MEMTABLE_CAP: usize = 2;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;

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

#[derive(Clone)]
/// Represents options to configure LSMStorage:
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

/// Represents key-value storage based on the log-structured merge tree.
pub struct LSMStorage<M: Memtable + Send + Sync + 'static> {
    /// Path in which all files of the storage will be created.
    path: PathBuf,
    /// Lock for swapping memtables.
    swap_lock: Mutex<()>,
    /// Current memtable.
    memtable: Arc<RwLock<M>>,
    /// All frozen immutable memtables that are not yet flushed indexed by their id.
    imm_memtables: Arc<RwLock<HashMap<usize, M>>>,
    /// SSTables of the l0 level.
    l0_tables: Arc<RwLock<Vec<usize>>>,
    /// All SSTables indexed by their id.
    sstables: Arc<RwLock<HashMap<usize, SSTable>>>,
    /// Options of the storage.
    options: Arc<LSMStorageOptions>,
    /// Id of the next SSTable; Increasing monotonically when new memtable is created.
    next_sst_id: AtomicUsize,
    /// Storage manifest.
    manifest: Arc<Mutex<Manifest>>,
    /// Handle of the flush system; Used for shutting down flush_system and wait for every flush completion.
    flush_handle: FlushHandle<M>,
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
    /// Opens db from path if there is a manifest file, creates new db in the given path otherwise.
    pub fn open(path: impl AsRef<Path>, options: LSMStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut l0_tables = Vec::<usize>::new();
        let mut sstables = HashMap::<usize, SSTable>::new();

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
            manifest.add_record(ManifestRecord::NewMemtable(0))?;
        }

        // TODO: recover memtables from WALs..

        let memtable = Arc::new(RwLock::new(M::new(next_sst_id)));
        let imm_memtables = Arc::new(RwLock::new(HashMap::new()));
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
            next_sst_id: AtomicUsize::new(next_sst_id),
            options: Arc::new(options),
            manifest,
            flush_handle,
        })
    }

    /// Waits for all flushes to be done then closes db.
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

        flush_handle.sender.send(FlushCommand::Shutdown).await?;
        flush_handle.handle.await?;

        let memtable = Arc::try_unwrap(memtable).unwrap().into_inner();
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
            flush_memtable(options.block_size, &path, memtable).await?;

            manifest
                .lock()
                .await
                .add_record(ManifestRecord::Flush(id))?;
        }

        Ok(())
    }

    /// Inserts given key-value pair into the db.
    pub async fn insert(&mut self, key: impl AsRef<[u8]>, value: Bytes) -> Result<()> {
        self.insert_inner(key, Record::Put(value)).await
    }

    /// Returns Some(value) if the corresponding value is found for the given key, None otherwise.
    pub async fn get(&self, key: &impl AsRef<[u8]>) -> Option<Bytes> {
        let memtable = self.memtable.read().await;
        let key = key.as_ref();

        let mut candidate = memtable.get(key);
        if let Some(rec) = candidate {
            return rec.into_inner();
        }

        let imm_memtables = self.imm_memtables.read().await;
        for memtable in imm_memtables.values() {
            match memtable.get(key) {
                None => continue,
                Some(val) => candidate = Some(val),
            }
        }

        if let Some(rec) = candidate {
            return rec.into_inner();
        }

        drop(imm_memtables);

        let l0_tables = self.l0_tables.read().await;
        let sstables = self.sstables.read().await;
        l0_tables.iter().for_each(|sst_id| {
            // TODO: Propper error handling
            let sst = sstables.get(sst_id).unwrap();
            let try_get_value = sst.get(key);
            // TODO: Propper error handling
            if let Ok(maybe_value) = try_get_value {
                if maybe_value.is_some() {
                    candidate = maybe_value;
                }
            }
        });

        drop(sstables);
        drop(l0_tables);

        if let Some(rec) = candidate {
            return rec.into_inner();
        }

        None
    }

    /// Deletes value corresponding to the given key if it is present in the db.
    pub async fn delete(&mut self, key: &impl AsRef<[u8]>) -> Result<()> {
        self.insert_inner(key, TOMBSTONE).await
    }

    async fn insert_inner(&mut self, key: impl AsRef<[u8]>, value: Record) -> Result<()> {
        let key = key.as_ref();
        if key.is_empty() {
            anyhow::bail!("Empty keys are not allowed");
        }

        let mut memtable = self.memtable.write().await;
        memtable.set(Bytes::copy_from_slice(key), value);

        if memtable.size_estimate() > self.options.memtables_size {
            drop(memtable);

            let old_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
            let new_memtable = Arc::new(RwLock::new(M::new(old_id + 1)));
            let old_memtable = self.swap_memtable(new_memtable).await;

            self.imm_memtables
                .write()
                .await
                .insert(old_id, old_memtable.clone());
            {
                self.manifest
                    .lock()
                    .await
                    .add_record(ManifestRecord::NewMemtable(old_id))
                    .context("Failed to add NewMemtable record to manifest")?
            }

            self.flush_handle
                .sender
                .send(FlushCommand::Memtable(old_memtable))
                .await
                .context("Failed to send memtable into flush channel")?;
        }
        Ok(())
    }

    async fn swap_memtable(&mut self, new_memtable: Arc<RwLock<M>>) -> M {
        let _lock = self.swap_lock.lock().await;
        let old_memtable = mem::replace(&mut self.memtable, new_memtable);
        Arc::try_unwrap(old_memtable).unwrap().into_inner()
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
        let mut storage = Storage::open(&dir, LSMStorageOptions::default())?;

        storage.insert(b"key1", Bytes::from("val1")).await?;
        assert_eq!(storage.get(&b"key1").await, Some(Bytes::from("val1")));

        storage.delete(&b"key1").await?;
        assert_eq!(storage.get(&b"key1").await, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_persistence() -> Result<()> {
        let dir = tempdir()?;
        {
            let mut storage = Storage::open(&dir, LSMStorageOptions::default())?;
            storage.insert(b"persisted", Bytes::from("data")).await?;
            storage.close().await?;
        }

        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        assert_eq!(storage.get(&b"persisted").await, Some(Bytes::from("data")));
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_recovery() -> Result<()> {
        let dir = tempdir()?;
        let mut storage = Storage::open(
            &dir,
            LSMStorageOptions {
                memtables_size: 1,
                ..Default::default()
            },
        )?;

        storage.insert(b"flushed", Bytes::from("value")).await?;
        tokio::time::sleep(Duration::from_secs(1)).await; // Wait for flush
        storage.close().await.unwrap();

        let storage = Storage::open(&dir, LSMStorageOptions::default())?;
        assert_eq!(storage.get(&b"flushed").await, Some(Bytes::from("value")));
        Ok(())
    }
}
