use anyhow::{Context, Ok, Result};
use bytes::Buf;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex},
};

pub(crate) struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum ManifestRecord {
    NewMemtable(usize),
    Flush(usize),
}

impl Manifest {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)
                    .context("Failed to create manifest file")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // TODO: Check for corruption
        let mut file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(path)
            .context("Failed to open manifest")?;
        let mut buf = Vec::<u8>::new();
        file.read_to_end(&mut buf)
            .context("Failed to read manifest file")?;
        let mut buf = &buf[..];
        let mut records = Vec::<ManifestRecord>::new();
        while buf.has_remaining() {
            let len = buf.get_u64() as usize;
            let data = &buf[..len];
            let record = serde_json::from_slice::<ManifestRecord>(data)
                .context("Failed to deserealize record")?;
            buf.advance(len);
            records.push(record);
        }

        let manifest = Self {
            file: Arc::new(Mutex::new(file)),
        };

        Ok((manifest, records))
    }

    /// Adds record and syncs file.
    pub fn add_record(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let buf = serde_json::to_vec(&record).context("Failed to serialize record")?;
        file.write_all(&buf.len().to_be_bytes())
            .context("Failed to wrtie record len to file")?;
        file.write_all(&buf)
            .context("Failed to write record to file")?;
        file.sync_all().context("Failed to sync file")?;
        Ok(())
    }
}

#[cfg(test)]
mod manifest_tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_empty_manifest() -> Result<()> {
        let file = NamedTempFile::new()?;
        let _ = Manifest::new(file.path())?;
        let (_, records) = Manifest::recover(file.path())?;
        assert!(records.is_empty());
        Ok(())
    }

    #[test]
    fn test_record_roundtrip() -> Result<()> {
        let file = NamedTempFile::new()?;
        let manifest = Manifest::new(file.path())?;

        manifest.add_record(ManifestRecord::NewMemtable(1))?;
        manifest.add_record(ManifestRecord::Flush(1))?;

        let (_, records) = Manifest::recover(file.path())?;
        assert_eq!(records.len(), 2);
        matches!(records[0], ManifestRecord::NewMemtable(1));
        matches!(records[1], ManifestRecord::Flush(1));
        Ok(())
    }
}
