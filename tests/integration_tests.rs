mod tests {
    use anyhow::Result;
    use bytes::Bytes;
    use crossbeam_skiplist::SkipMap;
    use lsm_tree::memtable::BtreeMapMemtable;
    use lsm_tree::memtable::Memtable;
    use lsm_tree::memtable::SkipListMemtable;
    use lsm_tree::memtable::ThreadSafeMemtable;
    use lsm_tree::options::LSMStorageOptions;
    use rstest::rstest;
    use std::result::Result::Ok;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::{watch, Barrier};

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test]
    async fn test_wal_recovery_after_crash<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_path_buf();
        {
            let options = LSMStorageOptions::default();
            let storage = options.open::<M>(&path).await?;
            storage
                .insert(b"key_wal_1", Bytes::from("value_recovered_1"))
                .await?;
            storage
                .insert(b"key_wal_2", Bytes::from("value_recovered_2"))
                .await?;
            storage.delete(&b"key_to_delete").await?;
            storage
                .insert(b"key_to_delete", Bytes::from("should_not_exist"))
                .await?;
            storage.delete(&b"key_to_delete").await?;
            std::mem::forget(storage);
        }
        let options = LSMStorageOptions::default();
        let storage = options.open::<M>(&path).await?;
        assert_eq!(
            storage.get(&b"key_wal_1").await?,
            Some(Bytes::from("value_recovered_1"))
        );
        assert_eq!(
            storage.get(&b"key_wal_2").await?,
            Some(Bytes::from("value_recovered_2"))
        );
        assert_eq!(
            storage.get(&b"key_to_delete").await?,
            None,
            "Key should be deleted"
        );
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test]
    async fn test_compaction_is_triggered_and_succeeds<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let dir = tempdir()?;
        let options = LSMStorageOptions::default().max_l0_ssts(2);
        let storage = options.open::<M>(&dir).await?;
        storage.insert(b"a", Bytes::from("1")).await?;
        storage.insert(b"b", Bytes::from("2")).await?;
        storage.force_flush().await?;
        storage.insert(b"c", Bytes::from("3")).await?;
        storage.insert(b"d", Bytes::from("4")).await?;
        storage.force_flush().await?;
        storage.insert(b"c", Bytes::from("100")).await?;
        storage.insert(b"d", Bytes::from("1000")).await?;
        storage.force_flush().await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(storage.get(&b"a").await?, Some(Bytes::from("1")));
        assert_eq!(storage.get(&b"b").await?, Some(Bytes::from("2")));
        assert_eq!(storage.get(&b"c").await?, Some(Bytes::from("100")));
        assert_eq!(storage.get(&b"d").await?, Some(Bytes::from("1000")));
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test]
    async fn test_sstable_priority<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let dir = tempdir()?;
        let storage = LSMStorageOptions::default().open::<M>(&dir).await?;
        let key = b"key";
        let old = Bytes::from("old");
        let new = Bytes::from("new");
        storage.insert(key, old).await?;
        storage.close().await?;
        let storage = LSMStorageOptions::default().open::<M>(&dir).await?;
        storage.insert(key, new.clone()).await?;
        storage.close().await?;
        let storage = LSMStorageOptions::default().open::<M>(&dir).await?;
        let actual = storage.get(key).await?;
        assert_eq!(actual, Some(new));
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test]
    async fn test_concurrent_write_non_overlapping_keys<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let dir = tempdir()?;
        let storage = Arc::new(
            LSMStorageOptions::default()
                .memtable_size(100)
                .open::<M>(&dir)
                .await?,
        );
        let num_tasks = num_cpus::get();
        let barrier = Arc::new(Barrier::new(num_tasks));
        let mut handles = Vec::with_capacity(num_tasks);
        for i in 0..num_tasks {
            let storage = storage.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for j in 0..100 {
                    let key = format!("key-{i}-{j}");
                    let val = Bytes::from_owner(format!("value-{i}-{j}"));
                    storage.insert(&key, val).await.unwrap();
                }
            }));
        }
        for handle in handles {
            handle.await?;
        }
        for i in 0..num_tasks {
            for j in 0..100 {
                let key = format!("key-{i}-{j}").into_bytes();
                let expected = Bytes::from_owner(format!("value-{i}-{j}"));
                let actual = storage.get(&key).await?;
                assert_eq!(actual, Some(expected));
            }
        }
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test]
    async fn test_concurrent_read_non_overlapping_keys<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let dir = tempdir()?;
        let storage = Arc::new(
            LSMStorageOptions::default()
                .memtable_size(100)
                .open::<M>(&dir)
                .await?,
        );
        let num_tasks = num_cpus::get();
        for i in 0..num_tasks {
            for j in 0..100 {
                let key = format!("key-{i}-{j}");
                let val = Bytes::from_owner(format!("value-{i}-{j}"));
                storage.insert(&key, val).await?;
            }
        }
        let barrier = Arc::new(Barrier::new(num_tasks));
        let mut handles = Vec::with_capacity(num_tasks);
        for i in 0..num_tasks {
            let storage = storage.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for j in 0..100 {
                    let key = format!("key-{i}-{j}").into_bytes();
                    let expected = Bytes::from_owner(format!("value-{i}-{j}"));
                    let actual = storage.get(&key).await.unwrap();
                    assert_eq!(actual, Some(expected));
                }
            }));
        }
        for handle in handles {
            handle.await?;
        }
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test]
    async fn test_large_value<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let dir = tempdir()?;
        let storage = LSMStorageOptions::default().open::<M>(&dir).await?;
        let expected = Bytes::from_owner(vec![1u8; 1 << 28]);
        let key = b"key";
        storage.insert(&key, expected.clone()).await?;
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected.clone()));
        storage.close().await?;
        let storage = LSMStorageOptions::default().open::<M>(&dir).await?;
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_repeat_same_key<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let dir = tempdir()?;
        let storage = LSMStorageOptions::default()
            .memtable_size(1)
            .open::<M>(&dir)
            .await?;
        let key = b"key";
        for i in 0..1000 {
            storage.insert(&key, Bytes::from(i.to_string())).await?;
        }
        let actual = storage.get(&key).await?;
        let expected = Some(Bytes::from("999"));
        assert_eq!(actual, expected);
        storage.delete(&key).await?;
        storage.close().await?;
        let storage = LSMStorageOptions::default()
            .memtable_size(1)
            .open::<M>(&dir)
            .await?;
        storage.delete(&key).await?;
        let actual = storage.get(&key).await?;
        assert_eq!(actual, None);
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_race_conditions<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let dir = tempdir()?;
        let options = LSMStorageOptions::default()
            .memtable_size(1)
            .num_flush_jobs(2);
        let storage = options.clone().open::<M>(&dir).await?;
        let expected = Bytes::from("value");
        for i in 0..100 {
            storage
                .insert(format!("key-{}", i).as_bytes(), expected.clone())
                .await?;
        }
        for i in 0..100 {
            let key = format!("key-{}", i).into_bytes();
            let actual = storage.get(&key).await?;
            assert_eq!(actual, Some(expected.clone()));
        }
        storage.close().await?;
        let storage = options.open::<M>(&dir).await?;
        for i in 0..100 {
            let key = format!("key-{}", i).into_bytes();
            let actual = storage.get(&key).await?;
            assert_eq!(actual, Some(expected.clone()));
        }
        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_read_write_stress_test<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        use tokio::task::JoinHandle;

        const NUM_WRITER_TASKS: usize = 6;
        const NUM_READER_TASKS: usize = 4;
        const OPS_PER_WRITER: usize = 10_000;

        let options = LSMStorageOptions::default()
            .memtable_size(1024 * 1024)
            .max_l0_ssts(8);

        let dir = tempdir()?;
        let storage = Arc::new(options.open::<M>(&dir).await?);

        let ground_truth: Arc<SkipMap<Bytes, Option<Bytes>>> = Arc::new(SkipMap::new());

        let mut writer_tasks = Vec::new();
        let mut reader_tasks: Vec<JoinHandle<()>> = Vec::new();
        let (stop_tx, stop_rx) = watch::channel(());
        let write_barrier = Arc::new(Barrier::new(NUM_WRITER_TASKS));

        for i in 0..NUM_WRITER_TASKS {
            let storage_clone = storage.clone();
            let barrier = write_barrier.clone();
            let ground_truth = ground_truth.clone();

            let task = tokio::spawn(async move {
                barrier.wait().await;
                for j in 0..OPS_PER_WRITER {
                    let key = Bytes::from(format!("key_{}_{}", i, j));

                    let value = Bytes::from(format!("value_{}_{}", i, j));
                    storage_clone
                        .insert(key.clone(), value.clone())
                        .await
                        .unwrap();
                    ground_truth.insert(key, Some(value));
                }
            });
            writer_tasks.push(task);
        }

        let read_barrier = Arc::new(Barrier::new(NUM_READER_TASKS));
        for _ in 0..NUM_READER_TASKS {
            let storage_clone = storage.clone();
            let stop = stop_rx.clone();
            let barrier = read_barrier.clone();

            let task = tokio::spawn(async move {
                barrier.wait().await;
                while !stop.has_changed().unwrap() {
                    let writer_id = rand::random_range(0..NUM_WRITER_TASKS as u32);
                    let op_id = rand::random_range(0..OPS_PER_WRITER as u32);
                    let key = Bytes::from(format!("key_{}_{}", writer_id, op_id));
                    let _ = storage_clone.get(&key).await.unwrap();
                }
            });
            reader_tasks.push(task);
        }

        for task in writer_tasks {
            task.await?;
        }

        dbg!("Done write");

        stop_tx.send(())?;
        for task in reader_tasks {
            task.await?
        }
        dbg!("Done read");

        storage.force_flush().await?;

        dbg!("Done flush");
        tokio::time::sleep(Duration::from_secs(5)).await;

        dbg!("Done wating");

        for e in &*ground_truth {
            let actual_value = storage.get(e.key()).await?;
            match e.value() {
                Some(value) => {
                    assert_eq!(
                        actual_value.as_ref(),
                        Some(value),
                        "Value mismatch for key {:?}",
                        String::from_utf8_lossy(e.key())
                    );
                }
                None => {
                    assert!(
                        actual_value.is_none(),
                        "Key {:?} should be deleted",
                        String::from_utf8_lossy(e.key())
                    );
                }
            }
        }

        Ok(())
    }
}
