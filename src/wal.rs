use anyhow::{Context, Result};
use bytes::Bytes;
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use crate::lsm_storage::Record;

enum EntryTag {
    Put,
    Delete,
    Invalid,
}

impl From<u8> for EntryTag {
    fn from(value: u8) -> Self {
        match value {
            0 => EntryTag::Put,
            1 => EntryTag::Delete,
            _ => EntryTag::Invalid,
        }
    }
}

struct WalReader<R> {
    reader: R,
}

impl<R: Read> WalReader<R> {
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

#[derive(Debug)]
struct WalEntry {
    key: Bytes,
    value: Record,
}

impl WalEntry {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        match &self.value {
            Record::Put(v) => {
                buf.push(EntryTag::Put as u8);
                buf.write_all(&(self.key.len() as u64).to_le_bytes())?;
                buf.write_all(&(v.len() as u64).to_le_bytes())?;
                buf.write_all(&self.key)?;
                buf.write_all(v)?;
            }
            Record::Delete => {
                buf.push(EntryTag::Delete as u8);
                buf.write_all(&(self.key.len() as u64).to_le_bytes())?;
                buf.write_all(&(0u64).to_le_bytes())?;
                buf.write_all(&self.key)?;
            }
        }
        Ok(())
    }

    fn decode(reader: impl Read) -> Result<Self> {
        let mut wal_reader = WalReader { reader };

        let tag = wal_reader.read_u8()?;

        let key_len = wal_reader.read_u64()? as usize;
        let val_len = wal_reader.read_u64()? as usize;

        let key = wal_reader.read_bytes(key_len)?;

        let value = match tag.into() {
            EntryTag::Put => Record::Put(wal_reader.read_bytes(val_len)?),
            EntryTag::Delete => Record::Delete,
            EntryTag::Invalid => anyhow::bail!("Wrong tag for wal entry"),
        };

        Ok(Self { key, value })
    }
}

/// Write-Ahead Log for durability.
///
/// Each memtable has its own WAL file. Writes are appended sequentially
/// for high write throughput. When a memtable is flushed, its WAL is deleted.
pub struct Wal {
    file: File,
    durable: bool,
}

impl Wal {
    /// Opens or creates a WAL file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the WAL file
    /// * `durable` - If true, fsync after each write for durability
    pub fn open(path: impl AsRef<Path>, durable: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .context(format!("Failed to open WAL file {:?}", &path))?;

        Ok(Self { file, durable })
    }

    /// Appends a key-value record to the WAL.
    ///
    /// If `durable` is true, the write is fsynced to ensure persistence.
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

    /// Returns an iterator over all records in the WAL.
    ///
    /// Used during recovery to replay writes.
    pub fn iter(self) -> WalIterator {
        WalIterator {
            wal: self,
            stop: false,
        }
    }

    /// Constructs the path for a WAL file given a memtable ID.
    pub(crate) fn get_wal_path(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().to_path_buf().join(format!("{id}.wal"))
    }
}

/// Iterator over WAL entries.
///
/// Stops on first decode error to avoid corrupting state with partial writes.
pub struct WalIterator {
    wal: Wal,
    stop: bool,
}

impl Iterator for WalIterator {
    type Item = Result<(Bytes, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stop {
            return None;
        }

        let mut reader = &self.wal.file;
        let mut len_buf = [0u8; 8];
        match reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => {
                self.stop = true;
                return Some(Err(e).context("Failed to read entry length"));
            }
        }
        let total_len = u64::from_le_bytes(len_buf);
        let mut entry_buf = vec![0u8; total_len as usize];
        if let Err(e) = reader.read_exact(&mut entry_buf) {
            return Some(Err(e).context("Failed to read entry data"));
        }

        let mut entry_reader = &entry_buf[..];
        match WalEntry::decode(&mut entry_reader) {
            Ok(entry) => Some(Ok((entry.key, entry.value))),
            Err(e) => {
                self.stop = true;
                Some(Err(e).context("Failed to decode WAL entry"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Ok, Result};
    use bytes::Bytes;
    use std::{fs::OpenOptions, os::unix::fs::FileExt};
    use tempfile::tempdir;

    use crate::lsm_storage::Record;

    #[test]
    fn test_wal_append_and_replay_put() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_put.wal");
        let durable = true;

        {
            let mut wal = Wal::open(&path, durable)?;
            wal.append(Bytes::from("k1"), &Record::put_from_slice("v1"))?;
            wal.append(Bytes::from("k2"), &Record::put_from_slice("v2"))?;
        }

        let entries: Result<Vec<_>, _> = Wal::open(&path, false)?.iter().collect();
        assert_eq!(
            entries?,
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

        let entries: Result<Vec<_>, _> = Wal::open(&path, false)?.iter().collect();
        assert_eq!(entries?, vec![(Bytes::from("key"), Record::Delete)]);
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

        let entries: Vec<_> = Wal::open(&path, false)?
            .iter()
            .filter_map(Result::ok)
            .collect();
        assert!(!entries.is_empty());
        assert_eq!(entries[0], (Bytes::from("a"), Record::put_from_slice("1")));

        assert!(entries.len() <= 3);
        Ok(())
    }

    #[test]
    fn test_wal_corruption_in_payload_stops_iterator() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test_corrupt_payload.wal");

        {
            let mut wal = Wal::open(&path, true)?;
            wal.append(Bytes::from("good1"), &Record::put_from_slice("ok1"))?;
            wal.append(
                Bytes::from("bad"),
                &Record::put_from_slice("this_will_be_corrupted"),
            )?;
            wal.append(Bytes::from("good2"), &Record::put_from_slice("ok2"))?;
        }

        {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.write_at(&[0xFF], 50)?;
        }

        let mut iter = Wal::open(&path, false)?.iter();

        assert_eq!(
            iter.next().unwrap()?,
            (Bytes::from("good1"), Record::put_from_slice("ok1"))
        );

        assert!(iter.next().unwrap().is_err());

        assert!(iter.next().is_none());

        Ok(())
    }
}
