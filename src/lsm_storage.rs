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
    sync::{mpsc::Receiver, Mutex, RwLock, Semaphore},
    task::JoinHandle,
};

use crate::{
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    sstable::{builder::SSTableBuilder, SSTable},
};

// TODO: Fix tombstone logic. i.e user shouldn't be able to replicate tombstone.
const TOMBSTONE: Bytes = Bytes::from_static(b"__TOMBSTONE__");
const FLUSH_CHANNEL_SIZE: usize = 100;
const DEFAULT_BLOCK_SIZE: usize = 1 << 12;
const DEFAULT_MEMTABLE_SIZE: usize = 1 << 23;
const DEFAULT_MEMTABLE_CAP: usize = 2;
const DEFAULT_NUM_FLUSH_WORKERS: usize = 2;

#[derive(Clone)]
pub struct LSMStorageOptions {
    pub block_size: usize,
    pub memtables_size: usize,
    // Number of immutable memtables stored in memory.
    #[allow(unused)]
    pub memtables_cap: usize,
    pub num_flush_jobs: usize,
}

pub struct LSMStorage<M: Memtable + Send + Sync + 'static> {
    path: PathBuf,
    swap_lock: Mutex<()>,
    memtable: Arc<RwLock<M>>,
    imm_memtables: Arc<RwLock<HashMap<usize, M>>>,
    l0_tables: Arc<RwLock<Vec<usize>>>,
    sstables: Arc<RwLock<HashMap<usize, SSTable>>>,
    options: Arc<LSMStorageOptions>,
    next_sst_id: AtomicUsize,
    manifest: Arc<Mutex<Manifest>>,
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

struct FlushSystem<M: Memtable + Send + Sync> {
    imm_memtables: Arc<RwLock<HashMap<usize, M>>>,
    l0_tables: Arc<RwLock<Vec<usize>>>,
    sstables: Arc<RwLock<HashMap<usize, SSTable>>>,
    manifest: Arc<Mutex<Manifest>>,
    path: PathBuf,
    options: LSMStorageOptions,
}

struct FlushHandle<M: Memtable + Send + Sync + 'static> {
    handle: JoinHandle<()>,
    sender: tokio::sync::mpsc::Sender<FlushCommand<M>>,
}

enum FlushCommand<M: Memtable + Send + Sync + 'static> {
    Memtable(M),
    Shutdown,
}

impl<M: Memtable + Send + Sync> FlushSystem<M> {
    /// Spawns task with reciever for immutable memtables.
    /// For each recieved memtable spawns task (number of tasks always <= flush_num_jobs) that
    /// flushes it, writes metainfo into manifest, removes immutable memtable from LSMStorage, adds sstable to LSMStorage.
    fn init(self) -> FlushHandle<M> {
        let (tx, rx) = tokio::sync::mpsc::channel(FLUSH_CHANNEL_SIZE);
        let path = Arc::new(self.path);

        let handle = tokio::spawn({
            async move {
                let semaphore = Arc::new(Semaphore::new(self.options.num_flush_jobs));
                let mut receiver: Receiver<FlushCommand<M>> = rx;

                while let Some(cmd) = receiver.recv().await {
                    match cmd {
                        FlushCommand::Memtable(memtable) => {
                            let permit = match semaphore.clone().acquire_owned().await {
                                Ok(p) => p,
                                Err(_) => break, // Semaphore closed
                            };

                            let imm_memtables = self.imm_memtables.clone();
                            let l0_sstables = self.l0_tables.clone();
                            let sstables = self.sstables.clone();
                            let manifest = self.manifest.clone();
                            let path = Arc::clone(&path);

                            tokio::spawn(async move {
                                let id = memtable.get_id();

                                // TODO: Error handling
                                let sst = flush_memtable(
                                    self.options.block_size,
                                    path.as_path(),
                                    &memtable,
                                )
                                .await
                                .unwrap();

                                manifest
                                    .lock()
                                    .await
                                    .add_record(ManifestRecord::Flush(id))
                                    .unwrap();

                                {
                                    l0_sstables.write().await.push(id);
                                    sstables.write().await.insert(id, sst);
                                    imm_memtables.write().await.remove(&id);
                                }

                                drop(permit);
                            });
                        }
                        FlushCommand::Shutdown => break,
                    }
                }

                // Wait for remaining permits
                let _ = semaphore
                    .acquire_many(self.options.num_flush_jobs as u32)
                    .await;
            }
        });

        FlushHandle { handle, sender: tx }
    }
}

fn get_sst_path(path: impl AsRef<Path>, id: usize) -> PathBuf {
    path.as_ref().to_path_buf().join(format!("{id}.sst"))
}

/// Creates sstable from memtable via build method in SSTableBuilder.
async fn flush_memtable(
    block_size: usize,
    path: impl AsRef<Path>,
    memtable: &impl Memtable,
) -> Result<SSTable> {
    let mut builder = SSTableBuilder::new(block_size);
    for (key, value) in memtable.iter() {
        builder.add(key, value);
    }

    let id = memtable.get_id();
    builder
        .build(get_sst_path(path, id))
        .context(format!("Failed to flush memtable. id: {id}"))
}

impl<M: Memtable + Clone + Send + Sync + std::fmt::Debug> LSMStorage<M> {
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

        let flush_worker = FlushSystem {
            imm_memtables: imm_memtables.clone(),
            l0_tables: l0_tables.clone(),
            sstables: sstables.clone(),
            manifest: manifest.clone(),
            path: path.to_path_buf(),
            options: options.clone(),
        };
        let flush_handle = flush_worker.init();

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

    pub async fn insert(&mut self, key: impl AsRef<[u8]>, value: Bytes) -> Result<()> {
        let key = key.as_ref();
        // TODO: Propper handling of channel overflow.
        if key.is_empty() {
            panic!("Empty keys are not allowed")
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
                // TODO: Propper handling of channel overflow.
                self.manifest
                    .lock()
                    .await
                    .add_record(ManifestRecord::NewMemtable(old_id))
                    .unwrap();
            }

            // TODO: Error handling.
            self.flush_handle
                .sender
                .send(FlushCommand::Memtable(old_memtable))
                .await?;
        }
        Ok(())
    }

    pub async fn get(&self, key: &impl AsRef<[u8]>) -> Option<Bytes> {
        let memtable = self.memtable.read().await;
        let key = key.as_ref();

        let mut candidate = memtable.get(key);
        if candidate.is_some() {
            if candidate == Some(TOMBSTONE) {
                return None;
            } else {
                return candidate;
            }
        }

        let imm_memtables = self.imm_memtables.read().await;
        for memtable in imm_memtables.values() {
            match memtable.get(key) {
                None => continue,
                Some(val) => candidate = Some(val),
            }
        }

        if candidate.is_some() {
            if candidate == Some(TOMBSTONE) {
                return None;
            }
            return candidate;
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

        if candidate.is_some() {
            if candidate == Some(TOMBSTONE) {
                return None;
            }
            return candidate;
        }

        None
    }

    pub async fn delete(&mut self, key: &impl AsRef<[u8]>) -> Result<()> {
        self.insert(key, TOMBSTONE).await
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
