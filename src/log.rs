//! Segment file implementation.

use crate::error::Result;
use crate::util::{checksum, parse_file_id, BufReaderWithOffset, BufWriterWithOffset};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use log::{trace, error};
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Read, Write, copy};
use std::path::{Path, PathBuf};

/// Data entry definition.
/// It will be serialized and saved to segment file.
#[derive(Serialize, Deserialize, Debug)]
struct InnerEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    // timestamp in nanos.
    timestamp: u128,
    // crc32 checksum
    checksum: u32,
}

impl InnerEntry {
    /// New data entry with given key and value.
    /// Checksum will be updated internally.
    fn new(key: &[u8], value: &[u8], timestamp: u128) -> Self {
        let mut ent = InnerEntry {
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

    /// Check data entry is corrupted or not.
    fn is_valid(&self) -> bool {
        self.checksum == self.fresh_checksum()
    }
}

/// An entry wrapper with size and offset.
#[derive(Debug)]
pub(crate) struct Entry {
    inner: InnerEntry,
    // size of inner entry in log file.
    pub size: u64,
    // position of inner entry in log file.
    pub offset: u64,
}

impl Entry {
    /// Create a new entry instance with size and offset.
    fn new(inner: InnerEntry, size: u64, offset: u64) -> Self {
        Self {
            inner,
            size,
            offset,
        }
    }

    /// Check the inner data entry is corrupted or not.
    pub(crate) fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    /// Return key of the inner entry.
    pub(crate) fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// Return value of the inner entry.
    pub(crate) fn value(&self) -> &[u8] {
        &self.inner.value
    }

    /// Return timestamp of the inner entry.
    pub(crate) fn timestamp(&self) -> u128 {
        self.inner.timestamp
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
    /// Segment file size.
    pub size: u64,
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

        let file = fs::File::open(path)?;
        let size = file.metadata()?.len();
        let sf = SegmentFile {
            path: path.to_path_buf(),
            id: file_id,
            writeable,
            reader: BufReaderWithOffset::new(file)?,
            writer: w,
            size: size,
        };

        Ok(sf)
    }

    /// Save key-value pair to segement file.
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> Result<Entry> {
        let inner = InnerEntry::new(key, value, timestamp);
        trace!(
            "append entry {:?} to segement file {}",
            &inner,
            self.path.display()
        );
        let encoded = bincode::serialize(&inner)?;
        let w = self
            .writer
            .as_mut()
            .ok_or(anyhow!("segment file is not writeable"))?;
        let offset = w.offset();
        w.write(&encoded)?;
        // TODO: custom flushing strategies.
        w.flush()?;

        self.size = offset + encoded.len() as u64;

        let entry = Entry::new(inner, encoded.len() as u64, offset);
        trace!(
            "successfully append {:?} to segment file {}",
            &entry,
            self.path.display()
        );

        Ok(entry)
    }

    /// Read key value in segment file.
    pub(crate) fn read(&mut self, offset: u64) -> Result<Entry> {
        trace!(
            "read key value with offset {} in segment file {}",
            offset,
            self.path.display()
        );
        // Note: we have to get a mutable reader here.
        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(offset))?;
        let inner: InnerEntry = bincode::deserialize_from(reader)?;

        let entry = Entry::new(inner, self.reader.offset() - offset, offset);
        trace!(
            "successfully read {:?} from segment {}",
            &entry,
            self.path.display()
        );
        Ok(entry)
    }

    /// Copy `size` bytes from `src` segment file.
    /// Return offset of the newly written entry.
    pub(crate) fn copy_bytes_from(&mut self, src: &mut SegmentFile, offset: u64, size: u64) -> Result<u64> {
        let reader = &mut src.reader;
        if reader.offset() != offset {
            reader.seek(SeekFrom::Start(offset))?;
        }

        let mut r = reader.take(size);
        let w = self.writer.as_mut().expect("segment file is not writeable");
        let offset = w.offset();

        let num_bytes = copy(&mut r, w)?;
        assert_eq!(num_bytes, size);
        self.size += num_bytes;
        Ok(offset)
    }

    /// Return an entry iterator.
    pub(crate) fn entry_iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            path: self.path.clone(),
            reader: fs::File::open(self.path.clone()).unwrap(),
        }
    }

    /// Flush buf writer.
    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.writeable {
            self.writer.as_mut().unwrap().flush()?;
        } 
        Ok(())
    }
}

impl Drop for SegmentFile {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            error!("failed to flush segment file: {}, got error: {}", self.path.display(), e);
        }

        // auto clean up if file size is zero.
        if self.writeable && self.size == 0 && fs::remove_file(self.path.as_path()).is_ok() {
            trace!(
                "segment file '{}' is empty, remove it.",
                self.path.display()
            );
        }
    }
}

#[derive(Debug)]
pub(crate) struct SegmentEntryIter {
    path: PathBuf,
    reader: fs::File,
}

impl Iterator for SegmentEntryIter {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.reader.seek(SeekFrom::Current(0)).unwrap();
        let mut inner: InnerEntry = bincode::deserialize_from(&self.reader).ok()?;
        let new_offset = self.reader.seek(SeekFrom::Current(0)).unwrap();

        let entry = Entry::new(inner, new_offset - offset, offset);

        trace!(
            "successfully read next {:?} from segment file {}",
            &entry,
            self.path.display()
        );

        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::*;

    #[test]
    fn test_new_entry() {
        let ts = current_timestamp();
        let ent = InnerEntry::new(&b"key".to_vec(), &b"value".to_vec(), ts);
        assert_eq!(ent.timestamp <= current_timestamp(), true);
        assert_eq!(ent.checksum, 3327521766);
    }

    #[test]
    fn test_checksum_valid() {
        let ts = current_timestamp();
        let ent = InnerEntry::new(&b"key".to_vec(), &b"value".to_vec(), ts);
        assert_eq!(ent.is_valid(), true);
    }

    #[test]
    fn test_checksum_invalid() {
        let ts = current_timestamp();
        let mut ent = InnerEntry::new(&b"key".to_vec(), &b"value".to_vec(), ts);
        ent.value = b"value_changed".to_vec();
        assert_eq!(ent.is_valid(), false);
    }
}
