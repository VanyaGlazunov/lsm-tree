use anyhow::{Context, Result};
use bytes::Bytes;
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

use crate::lsm_storage::Record;

const TAG_PUT: u8 = 0;
const TAG_DELETE: u8 = 1;

#[derive(Debug)]
struct WalEntry {
    key: Bytes,
    value: Record,
}

impl WalEntry {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        match &self.value {
            Record::Put(v) => {
                buf.push(TAG_PUT);
                buf.write_all(&(self.key.len() as u64).to_le_bytes())?;
                buf.write_all(&(v.len() as u64).to_le_bytes())?;
                buf.write_all(&self.key)?;
                buf.write_all(v)?;
            }
            Record::Delete => {
                buf.push(TAG_DELETE);
                buf.write_all(&(self.key.len() as u64).to_le_bytes())?;
                buf.write_all(&(0u64).to_le_bytes())?;
                buf.write_all(&self.key)?;
            }
        }
        Ok(())
    }

    fn decode(mut reader: impl Read) -> Result<Self> {
        let mut u8_buf = [0u8; 1];
        reader.read_exact(&mut u8_buf)?;
        let tag = u8_buf[0];

        let mut u64_buf = [0u8; 8];

        reader.read_exact(&mut u64_buf)?;
        let key_len = u64::from_le_bytes(u64_buf) as usize;

        reader.read_exact(&mut u64_buf)?;
        let val_len = u64::from_le_bytes(u64_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf)?;
        let key = Bytes::from(key_buf);

        let value = if tag == TAG_PUT {
            let mut val_buf = vec![0u8; val_len];
            reader.read_exact(&mut val_buf)?;
            Record::Put(Bytes::from(val_buf))
        } else {
            Record::Delete
        };

        Ok(Self { key, value })
    }
}

pub struct Wal {
    file: File,
    durable: bool,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>, durable: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .context("Failed to open WAL file")?;

        Ok(Self { file, durable })
    }

    pub fn append(&mut self, key: Bytes, value: &Record) -> Result<()> {
        let entry = WalEntry {
            key,
            value: value.clone(),
        };

        let mut payload_buf: Vec<u8> = Vec::new();
        entry.encode(&mut payload_buf)?;

        let total_len = payload_buf.len() as u64;

        self.file.write_all(&total_len.to_le_bytes())?;
        self.file.write_all(&payload_buf)?;

        if self.durable {
            self.file.sync_all()?;
        }
        Ok(())
    }

    pub fn replay(path: impl AsRef<Path>) -> Result<Vec<(Bytes, Record)>> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .context("Failed to open WAL for replay")?;

        let mut out = Vec::new();
        loop {
            let mut len_buf = [0u8; 8];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e).context("Failed to read WAL entry length"),
            };

            let total_len = u64::from_le_bytes(len_buf);

            let entry_reader = std::io::Read::by_ref(&mut file).take(total_len);
            let entry = match WalEntry::decode(entry_reader) {
                Ok(entry) => entry,
                Err(e) => {
                    if e.downcast_ref::<io::Error>()
                        .is_some_and(|io_err| io_err.kind() == io::ErrorKind::UnexpectedEof)
                    {
                        eprintln!("WAL is corrupted at the end, stopping replay.");
                        break;
                    }
                    return Err(e).context("Failed to decode WAL entry");
                }
            };
            out.push((entry.key, entry.value));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use bytes::Bytes;
    use std::fs::OpenOptions;
    use tempfile::tempdir;

    use crate::lsm_storage::Record;

    #[test]
    fn test_wal_append_and_replay_put() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_put.wal");

        {
            let mut wal = Wal::open(&path, true)?;
            wal.append(Bytes::from("k1"), &Record::put_from_slice("v1"))?;
            wal.append(Bytes::from("k2"), &Record::put_from_slice("v2"))?;
        }

        let entries = Wal::replay(&path)?;
        assert_eq!(
            entries,
            vec![
                (Bytes::from("k1"), Record::put_from_slice("v1")),
                (Bytes::from("k2"), Record::put_from_slice("v2")),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_wal_append_and_replay_delete() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_delete.wal");

        {
            let mut wal = Wal::open(&path, false)?;
            wal.append(Bytes::from("key"), &Record::Delete)?;
        }

        let entries = Wal::replay(&path)?;
        assert_eq!(entries, vec![(Bytes::from("key"), Record::Delete)]);
        Ok(())
    }

    #[test]
    fn test_wal_truncated_tail_is_ignored() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_trunc.wal");

        {
            let mut wal = Wal::open(&path, true)?;
            wal.append(Bytes::from("a"), &Record::put_from_slice("1"))?;
            wal.append(Bytes::from("b"), &Record::put_from_slice("2"))?;
            wal.append(Bytes::from("c"), &Record::put_from_slice("3"))?;
        }

        let meta = std::fs::metadata(&path)?;
        let orig_len = meta.len();

        let truncate_to = orig_len.saturating_sub(5);
        {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(truncate_to)?;
        }

        let entries = Wal::replay(&path)?;
        assert!(!entries.is_empty());
        assert_eq!(entries[0], (Bytes::from("a"), Record::put_from_slice("1")));

        assert!(entries.len() <= 3);
        Ok(())
    }
}
