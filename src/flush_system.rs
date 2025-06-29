use std::{path::PathBuf, sync::Arc};

use tokio::sync::{mpsc::Receiver, Semaphore};

use crate::lsm_storage::FLUSH_CHANNEL_SIZE;
use crate::{lsm_utils::flush_memtable, memtable::Memtable, sstable::SSTable};

/// Types of comands in flush channel.
pub enum FlushCommand<M: Memtable + Send + Sync + 'static> {
    Memtable(Arc<M>),
    Shutdown,
}

pub struct FlushResult {
    pub id: usize,
    pub sstable: SSTable,
}

pub fn start_flush_workers<M: Memtable + Send + Sync>(
    path: PathBuf,
    num_jobs: usize,
    block_size: usize,
) -> (
    tokio::sync::mpsc::Sender<FlushCommand<M>>,
    tokio::sync::mpsc::Receiver<FlushResult>,
) {
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(FLUSH_CHANNEL_SIZE);
    let (res_tx, res_rx) = tokio::sync::mpsc::channel(FLUSH_CHANNEL_SIZE);

    tokio::spawn({
        async move {
            let semaphore = Arc::new(Semaphore::new(num_jobs));
            let mut receiver: Receiver<FlushCommand<M>> = cmd_rx;
            let path = Arc::new(path);

            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    FlushCommand::Memtable(memtable) => {
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => break, // Semaphore closed
                        };
                        let res_tx = res_tx.clone();
                        let path = path.clone();

                        tokio::spawn(async move {
                            let id = memtable.get_id();

                            // TODO: Error handling
                            let sst = flush_memtable(block_size, &*path, &*memtable)
                                .await
                                .unwrap();

                            let result = FlushResult { id, sstable: sst };

                            if res_tx.send(result).await.is_err() {
                                eprintln!("Flush result receiver closed. Dropping flush result.");
                            }

                            drop(permit);
                        });
                    }
                    FlushCommand::Shutdown => break,
                }
            }

            // Wait for remaining permits
            let _ = semaphore.acquire_many(num_jobs as u32).await;
        }
    });

    (cmd_tx, res_rx)
}
