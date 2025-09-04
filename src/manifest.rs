use anyhow::{Context, Ok, Result};
use bincode::{config::standard, decode_from_slice, encode_to_vec, Decode, Encode};
use bytes::Buf;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

/// Append-only log for normal/crash recovery.
pub(crate) struct Manifest {
    /// File handle of the log.
    file: File,
}

/// Operations types logged in manifest.
#[derive(Encode, Decode, Debug)]
pub(crate) enum ManifestRecord {
    NewMemtable(usize), // Creation of memtable with given ID
    Flush(usize),       // Flush of a memtable with given ID
    Compaction {
        // Compaction result: new files `add_ssts` are added to `level`, old files `remove_ssts` are removed.
        level: usize,
        add_stts: Vec<usize>,
        remove_stts: Vec<usize>,
    },
}

impl Manifest {
    /// Creates new instance of manifest in the path given.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)
                .context("Failed to create manifest file")?,
        })
    }

    /// Recovers state from existing manifest.
    ///
    /// # Returns
    /// (Manifest handle, parsed records)
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
            let record = decode_from_slice(data, standard())
                .context("Failed to deserealize record")?
                .0;
            buf.advance(len);
            records.push(record);
        }

        let manifest = Self { file };

        Ok((manifest, records))
    }

    /// Adds record and fsyncs
    pub fn add_record(&self, record: ManifestRecord) -> Result<()> {
        let mut file = &self.file;

        let buf = encode_to_vec(record, standard())?;

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
