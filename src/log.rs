//! Segment file implementation.

use crate::error::Result;
use crate::util::{checksum, parse_file_id, BufReaderWithOffset, BufWriterWithOffset};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use log::trace;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};


/// Data entry definition.
#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    // timestamp in seconds
    pub timestamp: u32,
    // crc32 checksum
    checksum: u32,
}

impl Entry {
    fn new(key: &[u8], value: &[u8], timestamp: u32) -> Self {
        let mut ent = Entry {
            key: key.into(),
            value: value.into(),
            timestamp,
            checksum: 0,
        };
        ent.checksum = ent.fresh_checksum();
        ent
    }

    fn fresh_checksum(&self) -> u32 {
        // TODO: optimize it to avoid cloning.
        checksum(&[self.key.clone(), self.value.clone()].concat())
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.checksum == self.fresh_checksum()
    }
}

/// SegmentFile represents a immutable or immutalbe data log file.
#[derive(Debug)]
pub(crate) struct SegmentFile {
    pub path: PathBuf,
    /// Segment file id (12 digital characters).
    pub id: u64,
    /// Only one segment can be writeable at any time.
    /// Mark current segment file can be writeable or not.
    writeable: bool,
    /// File handle of current segment file for writting.
    /// Only writeable file can hold a writer.
    writer: Option<BufWriterWithOffset<File>>,
    /// File handle of current segment file for reading.
    reader: BufReaderWithOffset<File>,
}

impl SegmentFile {
    /// Create a new segment file instance.
    /// It parses segment id from file path, which wraps an optional
    /// writer (only for writeable segement file) and reader.
    pub(crate) fn new(path: &Path, writeable: bool) -> Result<Self> {
        // Segment name must starts with valid file id.
        let file_id = parse_file_id(path).expect("file id not found in file path");

        let mut w = None;
        if writeable {
            w = Some(BufWriterWithOffset::new(
                fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(path)?,
            )?);
        }

        let sf = SegmentFile {
            path: path.to_path_buf(),
            id: file_id,
            writeable,
            reader: BufReaderWithOffset::new(fs::File::open(path)?)?,
            writer: w,
        };

        Ok(sf)
    }

    /// Save key-value pair to segement file.
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8], timestamp: u32) -> Result<u64> {
        let ent = Entry::new(key, value, timestamp);
        trace!(
            "append entry {:?} to segement {}",
            &ent,
            self.path.display()
        );
        let encoded = bincode::serialize(&ent)?;
        let w = self
            .writer
            .as_mut()
            .ok_or(anyhow!("segment file is not writeable"))?;
        let offset = w.offset();
        w.write(&encoded)?;
        // TODO: custom flushing strategies.
        w.flush()?;
        Ok(offset)
    }

    /// Read key value in segment file.
    pub(crate) fn read(&mut self, offset: u64) -> Result<Entry> {
        trace!(
            "read key value with offset {} in file {}",
            offset,
            self.path.display()
        );
        // Note: we have to get a mutable reader here.
        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(offset))?;
        let ent: Entry = bincode::deserialize_from(reader)?;
        Ok(ent)
    }

    pub(crate) fn iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            reader: fs::File::open(self.path.clone()).unwrap(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SegmentEntryIter {
    reader: fs::File,
}

impl Iterator for SegmentEntryIter {
    type Item = (u64, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.reader.seek(SeekFrom::Current(0)).unwrap();
        let ent: Entry = bincode::deserialize_from(&self.reader).ok()?;
        Some((offset, ent))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::*;

    #[test]
    fn test_new_entry() {
        let ts = current_timestamp();
        let ent = Entry::new(&b"key".to_vec(), &b"value".to_vec(), ts);
        assert_eq!(ent.timestamp <= current_timestamp(), true);
        assert_eq!(ent.checksum, 3327521766);
    }

    #[test]
    fn test_checksum_valid() {
        let ts = current_timestamp();
        let ent = Entry::new(&b"key".to_vec(), &b"value".to_vec(), ts);
        assert_eq!(ent.is_valid(), true);
    }

    #[test]
    fn test_checksum_invalid() {
        let ts = current_timestamp();
        let mut ent = Entry::new(&b"key".to_vec(), &b"value".to_vec(), ts);
        ent.value = b"value_changed".to_vec();
        assert_eq!(ent.is_valid(), false);
    }
}
