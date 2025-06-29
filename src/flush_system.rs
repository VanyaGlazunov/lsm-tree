use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use tokio::{
    sync::{mpsc::Receiver, Mutex, RwLock, Semaphore},
    task::JoinHandle,
};

use crate::lsm_storage::FLUSH_CHANNEL_SIZE;
use crate::{
    lsm_storage::ImmutableMemtableMap,
    lsm_storage::LSMStorageOptions,
    lsm_utils::flush_memtable,
    manifest::{Manifest, ManifestRecord},
    memtable::Memtable,
    sstable::SSTable,
};

pub(crate) struct FlushSystem<M: Memtable + Send + Sync> {
    pub imm_memtables: ImmutableMemtableMap<M>,
    pub l0_tables: Arc<RwLock<Vec<usize>>>,
    pub sstables: Arc<RwLock<BTreeMap<usize, SSTable>>>,
    pub manifest: Arc<Mutex<Manifest>>,
    pub path: PathBuf,
    pub options: LSMStorageOptions,
}

/// Handle to operate on flush channel.
#[derive(Debug)]
pub struct FlushHandle<M: Memtable + Send + Sync + 'static> {
    pub handle: JoinHandle<()>,
    pub sender: tokio::sync::mpsc::Sender<FlushCommand<M>>,
}

/// Types of comands in flush channel.
pub enum FlushCommand<M: Memtable + Send + Sync + 'static> {
    Memtable(Arc<M>),
    Shutdown,
}

pub struct FlushResult {
    pub id: usize,
    pub sstable: SSTable,
}

impl<M: Memtable + Send + Sync> FlushSystem<M> {
    /// Spawns task with reciever for immutable memtables.
    /// For each recieved memtable spawns task
    /// (number of tasks always <= flush_num_jobs)
    /// that flushes it, writes metainfo into manifest,
    /// removes immutable memtable from LSMStorage, adds sstable to LSMStorage.
    pub fn init(self) -> FlushHandle<M> {
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
                                    &*memtable,
                                )
                                .await
                                .unwrap();

                                manifest
                                    .lock()
                                    .await
                                    .add_record(ManifestRecord::Flush(id))
                                    .unwrap();

                                sstables.write().await.insert(id, sst);
                                l0_sstables.write().await.push(id);
                                imm_memtables.write().await.remove(&id);

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
