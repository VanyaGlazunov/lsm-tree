// all the test are taken from original chunkfs repo https://github.com/Piletskii-Oleg/chunkfs/commit/ae79a19ceeb91de776296e7592b8f5bc4000571e.
#[cfg(all(test, feature = "chunkfs-integration"))]
mod tests {
    use anyhow::{Ok, Result};
    use chunkfs::chunkers::{FSChunker, LeapChunker};
    use chunkfs::hashers::SimpleHasher;
    use chunkfs::{create_cdc_filesystem, FileSystem, Hasher};
    use lsm_tree::chunkfs_adapter::LSMDatabaseAdapter;
    use lsm_tree::memtable::{BtreeMapMemtable, Memtable, SkipListMemtable, ThreadSafeMemtable};
    use lsm_tree::options::LSMStorageOptions;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::io;
    use tempfile::{tempdir, TempDir};

    const MB: usize = 1 << 20;

    fn create_cdc_filesystem_with_lsm<M, H>(
        hasher: H,
        rt: &tokio::runtime::Runtime,
    ) -> Result<(
        FileSystem<LSMDatabaseAdapter<M>, H, Vec<u8>, (), HashMap<(), Vec<u8>>>,
        TempDir,
    )>
    where
        M: ThreadSafeMemtable,
        H: Hasher<Hash = Vec<u8>>,
    {
        let dir = tempdir()?;
        let storage = rt.block_on(LSMStorageOptions::default().open::<M>(&dir))?;
        let adapter = LSMDatabaseAdapter::with_worker_threads(storage, num_cpus::get());
        let fs = create_cdc_filesystem(adapter, hasher);
        Ok((fs, dir))
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn write_read_complete_test<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;
        let mut handle = fs.create_file("file", LeapChunker::default()).unwrap();
        fs.write_to_file(&mut handle, &[1; MB]).unwrap();
        fs.write_to_file(&mut handle, &[1; MB]).unwrap();

        let measurements = fs.close_file(handle).unwrap();
        println!("{:?}", measurements);

        let handle = fs.open_file("file", LeapChunker::default()).unwrap();
        let read = fs.read_file_complete(&handle).unwrap();
        assert_eq!(read.len(), MB * 2);
        assert_eq!(read, [1; MB * 2]);

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn write_read_blocks_test<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;
        let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

        let ones = vec![1; MB];
        let twos = vec![2; MB];
        let threes = vec![3; MB];
        fs.write_to_file(&mut handle, &ones).unwrap();
        fs.write_to_file(&mut handle, &twos).unwrap();
        fs.write_to_file(&mut handle, &threes).unwrap();
        let measurements = fs.close_file(handle).unwrap();
        println!("{:?}", measurements);

        let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
        assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
        assert_eq!(fs.read_from_file(&mut handle).unwrap(), twos);
        assert_eq!(fs.read_from_file(&mut handle).unwrap(), threes);

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn read_file_with_size_less_than_1mb<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;

        let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

        let ones = vec![1; 10];
        fs.write_to_file(&mut handle, &ones).unwrap();
        let measurements = fs.close_file(handle).unwrap();
        println!("{:?}", measurements);

        let mut handle = fs.open_file_readonly("file").unwrap();
        assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn write_read_big_file_at_once<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;

        let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

        let data = vec![1; 3 * MB + 50];
        fs.write_to_file(&mut handle, &data).unwrap();
        fs.close_file(handle).unwrap();

        let handle = fs.open_file("file", LeapChunker::default()).unwrap();
        assert_eq!(fs.read_file_complete(&handle).unwrap().len(), data.len());

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn two_file_handles_to_one_file<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;
        let mut handle1 = fs.create_file("file", LeapChunker::default()).unwrap();
        let handle2 = fs.open_file_readonly("file").unwrap();
        fs.write_to_file(&mut handle1, &[1; MB]).unwrap();
        fs.close_file(handle1).unwrap();
        assert_eq!(fs.read_file_complete(&handle2).unwrap().len(), MB);

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn different_chunkers_from_vec_can_be_used_with_same_filesystem<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        use chunkfs::chunkers::SuperChunker;
        use chunkfs::hashers::Sha256Hasher;
        use chunkfs::ChunkerRef;

        let dir = tempdir()?;
        let storage = rt.block_on(LSMStorageOptions::default().open::<M>(&dir))?;
        let adapter = LSMDatabaseAdapter::with_worker_threads(storage, 4);
        let mut fs = create_cdc_filesystem(adapter, Sha256Hasher::default());

        let chunkers: Vec<ChunkerRef> = vec![
            SuperChunker::default().into(),
            LeapChunker::default().into(),
        ];

        let data = vec![0; 1024 * 1024];
        for chunker in chunkers {
            let name = format!("file-{chunker:?}");
            let mut fh = fs.create_file(&name, chunker).unwrap();
            fs.write_to_file(&mut fh, &data).unwrap();
            fs.close_file(fh).unwrap();

            let fh = fs.open_file(&name, FSChunker::default()).unwrap();
            let read = fs.read_file_complete(&fh).unwrap();

            assert_eq!(read.len(), data.len());
            //assert_eq!(read, data);
        }

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn readonly_file_handle_cannot_write_can_read<M: ThreadSafeMemtable>(
        #[case] _memtable: M,
    ) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        use chunkfs::WriteMeasurements;

        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;
        let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
        fs.write_to_file(&mut fh, &[1; MB]).unwrap();
        fs.close_file(fh).unwrap();

        // cannot write
        let mut ro_fh = fs.open_file_readonly("file").unwrap();
        let result = fs.write_to_file(&mut ro_fh, &[1; MB]);
        assert!(result.is_err());
        assert!(result.is_err_and(|e| e.kind() == io::ErrorKind::PermissionDenied));

        // can read complete
        let read = fs.read_file_complete(&ro_fh).unwrap();
        assert_eq!(read.len(), MB);
        assert_eq!(read, [1; MB]);

        let _ = fs.read_from_file(&mut ro_fh).unwrap();

        // can close
        let measurements = fs.close_file(ro_fh).unwrap();
        assert_eq!(measurements, WriteMeasurements::default());

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn write_from_stream_slice<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;
        let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
        fs.write_from_stream(&mut fh, &[1; MB * 2][..]).unwrap();
        fs.close_file(fh).unwrap();

        let ro_fh = fs.open_file_readonly("file").unwrap();
        let read = fs.read_file_complete(&ro_fh).unwrap();
        assert_eq!(read.len(), MB * 2);
        assert_eq!(fs.read_file_complete(&ro_fh).unwrap(), vec![1; MB * 2]);

        Ok(())
    }

    #[rstest]
    #[case(BtreeMapMemtable::new(0))]
    #[case(SkipListMemtable::new(0))]
    #[test]
    fn write_from_stream_buf_reader<M: ThreadSafeMemtable>(#[case] _memtable: M) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        use std::io::{Seek, Write};

        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[1; MB]).unwrap();
        file.seek(io::SeekFrom::Start(0)).unwrap();

        let (mut fs, _dir) = create_cdc_filesystem_with_lsm::<M, _>(SimpleHasher, &rt)?;
        let mut fh = fs.create_file("file", FSChunker::default()).unwrap();

        fs.write_from_stream(&mut fh, file).unwrap();
        fs.close_file(fh).unwrap();

        let ro_fh = fs.open_file_readonly("file").unwrap();
        let read = fs.read_file_complete(&ro_fh).unwrap();
        assert_eq!(read.len(), MB);
        assert_eq!(read, [1; MB]);

        Ok(())
    }
}
