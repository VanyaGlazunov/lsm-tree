use anyhow::Result;
use bytes::Bytes;
use lsm_tree::memtable::BtreeMapMemtable;
use lsm_tree::options::LSMStorageOptions;
use std::result::Result::Ok;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_sstable_priority() -> Result<()> {
    let dir = tempdir()?;
    let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>(&dir)?;
    let key = b"key";

    let old = Bytes::from("old");
    let new = Bytes::from("new");
    storage.insert(key, old).await?;
    storage.close().await?;

    let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>(&dir)?;
    storage.insert(key, new.clone()).await?;
    storage.close().await?;

    let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>(&dir)?;
    let actual = storage.get(key).await?;
    assert_eq!(actual, Some(new));
    Ok(())
}

#[tokio::test]
async fn test_concurrent_write_non_overlapping_keys() -> Result<()> {
    let dir = tempdir()?;

    let storage = Arc::new(
        LSMStorageOptions::default()
            .memtable_size(100)
            .open::<BtreeMapMemtable>(&dir)?,
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

#[tokio::test]
async fn test_concurrent_read_non_overlapping_keys() -> Result<()> {
    let dir = tempdir()?;

    let storage = Arc::new(
        LSMStorageOptions::default()
            .memtable_size(100)
            .open::<BtreeMapMemtable>(&dir)?,
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

#[tokio::test]
async fn test_large_value() -> Result<()> {
    let dir = tempdir()?;
    let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>(&dir)?;

    // let expected = Bytes::from_owner(vec![1u8; 1 << 28]); // 256 mb
    let expected = Bytes::from_owner(vec![1u8; 1 << 23]);
    let key = b"key";
    storage.insert(&key, expected.clone()).await?;
    let actual = storage.get(&key).await?;
    assert_eq!(actual, Some(expected.clone()));

    // storage.close().await?;
    // let storage = LSMStorageOptions::default().open::<BtreeMapMemtable>(&dir)?;
    // let actual = storage.get(&key).await?;
    // assert_eq!(actual, Some(expected));

    Ok(())
}

#[tokio::test]
async fn test_repeat_same_key() -> Result<()> {
    let dir = tempdir()?;

    let storage = LSMStorageOptions::default()
        .memtable_size(1)
        .open::<BtreeMapMemtable>(&dir)?;

    let key = b"key";
    for i in 0..100 {
        storage.insert(&key, Bytes::from(i.to_string())).await?;
    }

    let actual = storage.get(&key).await?;
    let expected = Some(Bytes::from("99"));
    assert_eq!(actual, expected);

    storage.delete(&key).await?;

    storage.close().await?;
    let storage = LSMStorageOptions::default()
        .memtable_size(1)
        .open::<BtreeMapMemtable>(&dir)?;

    storage.delete(&key).await?;
    let actual = storage.get(&key).await?;
    assert_eq!(actual, None);

    Ok(())
}

#[tokio::test]
async fn test_flush_race_conditions() -> Result<()> {
    let dir = tempdir()?;

    let options = LSMStorageOptions::default()
        .memtable_size(1)
        .num_flush_jobs(2);

    let storage = options.clone().open::<BtreeMapMemtable>(&dir)?;

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
    let storage = options.open::<BtreeMapMemtable>(&dir)?;
    for i in 0..100 {
        let key = format!("key-{}", i).into_bytes();
        let actual = storage.get(&key).await?;
        assert_eq!(actual, Some(expected.clone()));
    }

    Ok(())
}
