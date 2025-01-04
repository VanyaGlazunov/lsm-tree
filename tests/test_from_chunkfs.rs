extern crate chunkfs;

use lsm_tree::lsm_tree::LSMtree;
use std::io;
use std::io::{Seek, Write};
use std::path::PathBuf;

use chunkfs::chunkers::{FSChunker, LeapChunker, SuperChunker};
use chunkfs::hashers::SimpleHasher;
use chunkfs::{create_cdc_filesystem, WriteMeasurements};

const MB: usize = 1024 * 1024;
const THRESHOLD: usize = 1024;
const STORAGEPATH: &str = "./tests/storage";

#[test]
fn write_read_complete_test() {
    let path = PathBuf::from(STORAGEPATH).join("storage1");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);

    let mut handle = fs.create_file("file", LeapChunker::default()).unwrap();
    fs.write_to_file(&mut handle, &[1; MB]).unwrap();
    fs.write_to_file(&mut handle, &[1; MB]).unwrap();

    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    let read = fs.read_file_complete(&handle).unwrap();
    assert_eq!(read.len(), MB * 2);
    assert_eq!(read, [1; MB * 2]);
}

#[test]
fn write_read_blocks_test() {
    let path = PathBuf::from(STORAGEPATH).join("storage2");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);

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
}

#[test]
fn read_file_with_size_less_than_1mb() {
    let path = PathBuf::from(STORAGEPATH).join("storage3");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let ones = vec![1; 10];
    fs.write_to_file(&mut handle, &ones).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
}

#[test]
fn write_read_big_file_at_once() {
    let path = PathBuf::from(STORAGEPATH).join("storage4");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let data = vec![1; 3 * MB + 50];
    fs.write_to_file(&mut handle, &data).unwrap();
    fs.close_file(handle).unwrap();

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_file_complete(&handle).unwrap().len(), data.len());
}

#[test]
fn two_file_handles_to_one_file() {
    let path = PathBuf::from(STORAGEPATH).join("storage5");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);
    let mut handle1 = fs.create_file("file", LeapChunker::default()).unwrap();
    let mut handle2 = fs.open_file("file", LeapChunker::default()).unwrap();
    fs.write_to_file(&mut handle1, &[1; MB]).unwrap();
    fs.close_file(handle1).unwrap();
    assert_eq!(fs.read_from_file(&mut handle2).unwrap().len(), MB)
}

#[test]
fn different_chunkers_from_vec_can_be_used_with_same_filesystem() {
    let path = PathBuf::from(STORAGEPATH).join("storage6");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);
    let chunkers: Vec<Box<dyn chunkfs::Chunker>> = vec![
        SuperChunker::default().into(),
        LeapChunker::default().into(),
    ];

    let data = vec![0; MB];
    for chunker in chunkers {
        let name = format!("file-{chunker:?}");
        let mut fh = fs.create_file(&name, chunker).unwrap();
        fs.write_to_file(&mut fh, &data).unwrap();
        fs.close_file(fh).unwrap();

        let fh = fs.open_file(&name, FSChunker::default()).unwrap();
        let read = fs.read_file_complete(&fh).unwrap();

        assert_eq!(read.len(), data.len());
        assert_eq!(read, data);
    }
}

#[test]
fn readonly_file_handle_cannot_write_can_read() {
    let path = PathBuf::from(STORAGEPATH).join("storage7");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);
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

    let read = fs.read_from_file(&mut ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);

    // can close
    let measurements = fs.close_file(ro_fh).unwrap();
    assert_eq!(measurements, WriteMeasurements::default())
}

#[test]
fn write_from_stream_slice() {
    let path = PathBuf::from(STORAGEPATH).join("storage8");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
    fs.write_from_stream(&mut fh, &[1; MB * 2][..]).unwrap();
    fs.close_file(fh).unwrap();

    let ro_fh = fs.open_file_readonly("file").unwrap();
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB * 2);
    assert_eq!(fs.read_file_complete(&ro_fh).unwrap(), vec![1; MB * 2]);
}

#[test]
fn write_from_stream_buf_reader() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[1; MB]).unwrap();
    file.seek(io::SeekFrom::Start(0)).unwrap();

    let path = PathBuf::from(STORAGEPATH).join("storage9");
    let mut fs = create_cdc_filesystem(LSMtree::new(path.as_path(), THRESHOLD), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();

    fs.write_from_stream(&mut fh, file).unwrap();
    fs.close_file(fh).unwrap();

    let ro_fh = fs.open_file_readonly("file").unwrap();
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);
}
