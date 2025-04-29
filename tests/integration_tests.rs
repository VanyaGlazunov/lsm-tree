use anyhow::Result;
use bytes::Bytes;
use lsm_tree::lsm_storage::LSMStorage;
use lsm_tree::{lsm_storage::LSMStorageOptions, memtable::BtreeMapMemtable};
use proptest::prelude::*;
use std::{collections::BTreeMap, sync::Arc};
use tempfile::tempdir;

type Storage = LSMStorage<BtreeMapMemtable>;

fn operation_strategy() -> impl Strategy<Value = Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    prop::collection::vec(
        (
            prop::collection::vec(any::<u8>(), 1..2024),
            (any::<bool>(), prop::collection::vec(any::<u8>(), 1..2024)),
        ),
        1..100, // Number of operations
    )
    .prop_map(|ops| {
        ops.into_iter()
            .map(|(key, (delete, value))| (key, if delete { None } else { Some(value) }))
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn prop_lsm_behaves_like_btreemap(operations in operation_strategy()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempdir().unwrap();
            let mut storage = Storage::open(&dir, LSMStorageOptions::default()).unwrap();
            let mut reference = BTreeMap::new();

            for (key, value) in &operations {
                match value {
                    Some(v) => {
                        storage.insert(key, Bytes::copy_from_slice(v)).await;
                        reference.insert(key.clone(), v.clone());
                    }
                    None => {
                        storage.delete(key).await;
                        reference.remove(key);
                    }
                }
            }

            // Verify all entries match
            for (key, value) in reference.iter() {
                let storage_value = storage.get(key).await;
                assert_eq!(
                    storage_value,
                    Some(Bytes::from(value.clone())),
                    "Mismatch for key: {:?}",
                    key
                );
            }

            // Verify deleted entries are gone
            for (key, _) in operations.iter().filter(|(_, v)| v.is_none()) {
                assert_eq!(storage.get(key).await, None, "Key not deleted: {:?}", key);
            }

            storage.close().await.unwrap();
        });
    }
}

#[tokio::test]
async fn test_concurrent_access() -> Result<()> {
    let dir = tempdir()?;
    let storage = Arc::new(tokio::sync::Mutex::new(Storage::open(
        &dir,
        LSMStorageOptions::default(),
    )?));

    let mut handles = vec![];
    for i in 0..10 {
        let storage = storage.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..100 {
                let key = format!("key-{i}-{j}").into_bytes();
                storage
                    .lock()
                    .await
                    .insert(&key, Bytes::from("value"))
                    .await;
                assert_eq!(
                    storage.lock().await.get(&key).await,
                    Some(Bytes::from("value"))
                );
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    let st = storage.lock().await;
    for i in 0..10 {
        for j in 0..100 {
            let key = format!("key-{i}-{j}").into_bytes();
            assert_eq!(st.get(&key).await, Some(Bytes::from("value")));
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_edge_cases() -> Result<()> {
    let dir = tempdir()?;
    let mut storage = Storage::open(&dir, LSMStorageOptions::default())?;

    // Large values
    let big_val = vec![0u8; 1 << 20]; // 1MB
    storage.insert(b"big", Bytes::from(big_val.clone())).await;
    assert_eq!(storage.get(&b"big").await, Some(Bytes::from(big_val)));

    // Repeated overwrites
    for i in 0..100 {
        storage.insert(b"key", Bytes::from(i.to_string())).await;
    }
    assert_eq!(storage.get(&b"key").await, Some(Bytes::from("99")));

    // Tombstone persistence
    storage.insert(b"temp", Bytes::from("data")).await;
    storage.delete(&b"temp").await;
    storage.close().await?;
    let storage = Storage::open(&dir, LSMStorageOptions::default())?;
    assert_eq!(storage.get(&b"temp").await, None);

    Ok(())
}

#[tokio::test]
async fn test_flush_race_conditions() -> Result<()> {
    let dir = tempdir()?;
    let options = LSMStorageOptions {
        memtables_size: 1, // Force immediate flushing
        num_flush_jobs: 2,
        ..Default::default()
    };

    let mut storage = Storage::open(&dir, options)?;

    // Insert enough data to trigger multiple flushes
    for i in 0..100 {
        storage
            .insert(format!("key-{}", i).as_bytes(), Bytes::from("value"))
            .await;
    }

    // Verify all data while flushes are happening
    for i in 0..100 {
        let key = format!("key-{}", i).into_bytes();
        assert_eq!(storage.get(&key).await, Some(Bytes::from("value")));
    }

    storage.close().await?;

    // Verify persistence after forced flushes
    let storage = Storage::open(&dir, LSMStorageOptions::default())?;
    for i in 0..100 {
        let key = format!("key-{}", i).into_bytes();
        assert_eq!(storage.get(&key).await, Some(Bytes::from("value")));
    }

    Ok(())
}
