use std::{path::PathBuf, sync::Arc};

use tokio::sync::{mpsc, Semaphore};
use tracing::warn;

use crate::lsm_storage::{memtable_to_sst, LSMStorage, StateUpdateEvent};
use crate::memtable::Memtable;

impl<M: Memtable + Send + Sync + 'static> LSMStorage<M> {
    pub(crate) fn start_flush_workers(
        path: PathBuf,
        num_jobs: usize,
        block_size: usize,
        flush_receiver: mpsc::Receiver<Arc<M>>,
        state_update_sender: mpsc::Sender<StateUpdateEvent<M>>,
    ) {
        tokio::spawn({
            async move {
                let semaphore = Arc::new(Semaphore::new(num_jobs));
                let mut receiver = flush_receiver;
                let path = Arc::new(path);

                while let Some(memtable) = receiver.recv().await {
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break, // Semaphore closed
                    };
                    let state_update_sender = state_update_sender.clone();
                    let path = path.clone();

                    tokio::spawn(async move {
                        let id = memtable.get_id();

                        match memtable_to_sst(block_size, &*path, &*memtable).await {
                            Ok(sst) => {
                                if let Err(e) = state_update_sender
                                    .send(StateUpdateEvent::FlushComplete(id, sst))
                                    .await
                                {
                                    warn!(error = %e, memtable_id = id, "Failed to send flush complete (likely shutdown)");
                                }
                            }
                            Err(e) => {
                                if let Err(send_err) = state_update_sender
                                    .send(StateUpdateEvent::FlushFail(e))
                                    .await
                                {
                                    warn!(error = %send_err, "Failed to send flush failure (likely shutdown)");
                                }
                            }
                        }

                        drop(permit);
                    });
                }

                // Wait for remaining permits
                let _ = semaphore.acquire_many(num_jobs as u32).await;
            }
        });
    }
}
