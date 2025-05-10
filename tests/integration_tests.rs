use anyhow::Result;
use bytes::Bytes;
use lsm_tree::lsm_storage::LSMStorage;
use lsm_tree::{lsm_storage::LSMStorageOptions, memtable::BtreeMapMemtable};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::result::Result::Ok;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Barrier;

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
            let storage = Storage::open(&dir, LSMStorageOptions::default()).unwrap();
            let mut reference = BTreeMap::new();

            for (key, value) in &operations {
                match value {
                    Some(v) => {
                        storage.insert(key, Bytes::copy_from_slice(v)).await.unwrap();
                        reference.insert(key.clone(), v.clone());
                    }
                    None => {
                        storage.delete(key).await.unwrap();
                        reference.remove(key);
                    }
                }
            }

            // Verify all entries match
            for (key, value) in reference.iter() {
                let storage_value = storage.get(key).await.unwrap();
                assert_eq!(
                    storage_value,
                    Some(Bytes::from(value.clone())),
                    "Mismatch for key: {:?}",
                    key
                );
            }

            // Verify deleted entries are gone
            for (key, _) in operations.iter().filter(|(_, v)| v.is_none()) {
                let actual = storage.get(key).await.unwrap();
                assert_eq!(actual, None, "Key not deleted: {:?}", key);
            }

            storage.close().await.unwrap();
        });
    }
}

#[tokio::test]
async fn test_concurrent_write_non_overlapping_keys() -> Result<()> {
    let dir = tempdir()?;
    let storage = Arc::new(Storage::open(
        &dir,
        LSMStorageOptions {
            memtables_size: 100,
            ..Default::default()
        },
    )?);

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

#[tokio::test]
async fn test_concurrent_read_non_overlapping_keys() -> Result<()> {
    let dir = tempdir()?;
    let storage = Arc::new(Storage::open(
        &dir,
        LSMStorageOptions {
            memtables_size: 100,
            ..Default::default()
        },
    )?);

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

#[tokio::test]
async fn test_large_value() -> Result<()> {
    let dir = tempdir()?;
    let storage = Storage::open(&dir, LSMStorageOptions::default())?;

    let expected = Bytes::from_owner(vec![1u8; 1 << 28]); // 256 mb
    let key = b"key";
    storage.insert(&key, expected.clone()).await?;
    let actual = storage.get(&key).await?;
    assert_eq!(actual, Some(expected.clone()));

    storage.close().await?;
    let storage = Storage::open(&dir, LSMStorageOptions::default())?;
    let actual = storage.get(&key).await?;
    assert_eq!(actual, Some(expected));

    Ok(())
}

#[tokio::test]
async fn test_repeat_same_key() -> Result<()> {
    let dir = tempdir()?;
    let storage = Storage::open(
        &dir,
        LSMStorageOptions {
            memtables_size: 1,
            ..Default::default()
        },
    )?;

    let key = b"key";
    for i in 0..100 {
        storage.insert(&key, Bytes::from(i.to_string())).await?;
    }

    let actual = storage.get(&key).await?;
    let expected = Some(Bytes::from("99"));
    assert_eq!(actual, expected);

    storage.delete(&key).await?;

    // make sure everything is flushed.
    storage.close().await?;
    let storage = Storage::open(
        &dir,
        LSMStorageOptions {
            memtables_size: 1,
            ..Default::default()
        },
    )?;

    storage.delete(&key).await?;
    let actual = storage.get(&key).await?;
    assert_eq!(actual, None);

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

    let storage = Storage::open(&dir, options)?;

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

    // Verify persistence after forced flushes
    let storage = Storage::open(&dir, LSMStorageOptions::default())?;
    for i in 0..100 {
        let key = format!("key-{}", i).into_bytes();
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected.clone()));
    }

    Ok(())
}
