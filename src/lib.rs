pub mod block;
pub mod lsm_storage;
pub mod manifest;
pub mod memtable;
pub mod sstable;

// while let Some(memtable) = receiver.recv().await {
//     let semaphore = Box::new(Semaphore::new(num_flush_jobs));
//     let semaphore = Box::leak(semaphore);
//     let permit = semaphore.acquire().await.unwrap();
//     let data = Arc::clone(&data);
//     let path = Arc::clone(&path);
//     let manifest = Arc::clone(&manifest);

//     tokio::spawn(async move {
//         let id = memtable.get_id();
//         let sst_path = get_sst_path(path.as_path(), id);

//         let sst = {
//             let mut builder = SSTableBuilder::new(block_size);
//             memtable.flush(&mut builder);
//             // Writing data to file happens here
//             builder.build(sst_path).unwrap()
//         };

//         {
//             let manifest = manifest.lock().await;
//             // TODO: Propper error handling
//             manifest.add_record(ManifestRecord::Flush(id)).unwrap();
//         }

//         {
//             let mut guard = data.write().await;
//             // TODO: check if nothing was removed
//             guard.l0_tables.push(id);
//             guard.sstables.insert(id, sst);
//             guard.imm_memtables.remove(&id);
//         }

//         drop(permit);
//     });
// }
