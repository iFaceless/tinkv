//! Maintain data files.
use crate::error::{Result, TinkvError};
use crate::util::{checksum, parse_file_id, BufReaderWithOffset, FileWithBufWriter};
use serde::{Deserialize, Serialize};

use log::{error, trace};
use std::fmt;
use std::fs::{self, File};
use std::io::{copy, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Data entry definition.
/// It will be serialized and saved to data file.
#[derive(Serialize, Deserialize, Debug)]
struct InnerEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    // crc32 checksum
    checksum: u32,
}

impl InnerEntry {
    /// New data entry with given key and value.
    /// Checksum will be updated internally.
    fn new(key: &[u8], value: &[u8]) -> Self {
        let mut ent = InnerEntry {
            key: key.into(),
            value: value.into(),
            checksum: 0,
        };
        ent.checksum = ent.fresh_checksum();
        ent
    }

    fn fresh_checksum(&self) -> u32 {
        checksum(&self.value)
    }

    /// Check data entry is corrupted or not.
    fn is_valid(&self) -> bool {
        self.checksum == self.fresh_checksum()
    }
}

impl fmt::Display for InnerEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataInnerEntry(key='{}', checksum={})",
            String::from_utf8_lossy(self.key.as_ref()),
            self.checksum,
        )
    }
}
/// An entry wrapper with size and offset.
#[derive(Debug)]
pub(crate) struct Entry {
    inner: InnerEntry,
    // size of inner entry in data file.
    pub size: u64,
    // position of inner entry in data file.
    pub offset: u64,
    // related data file id.
    pub file_id: u64,
}

impl Entry {
    /// Create a new entry instance with size and offset.
    fn new(file_id: u64, inner: InnerEntry, size: u64, offset: u64) -> Self {
        Self {
            inner,
            size,
            offset,
            file_id,
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
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataEntry(file_id={}, key='{}', offset={}, size={})",
            self.file_id,
            String::from_utf8_lossy(self.key().as_ref()),
            self.offset,
            self.size,
        )
    }
}

/// DataFile represents a data file.
#[derive(Debug)]
pub(crate) struct DataFile {
    pub path: PathBuf,
    /// Data file id (12 digital characters).
    pub id: u64,
    /// Only one data file can be writeable at any time.
    /// Mark current data file can be writeable or not.
    writeable: bool,

    /// File handle of data file for writting.
    writer: Option<FileWithBufWriter>,

    /// File handle of current data file for reading.
    reader: BufReaderWithOffset<File>,
    /// Data file size.
    pub size: u64,
}

impl DataFile {
    /// Create a new data file instance.
    /// It parses data id from file path, which wraps an optional
    /// writer (only for writeable segement file) and reader.
    pub(crate) fn new(path: &Path, writeable: bool) -> Result<Self> {
        // Data name must starts with valid file id.
        let file_id = parse_file_id(path).expect("file id not found in file path");

        let mut w = None;
        if writeable {
            let f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)?;
            w = Some(FileWithBufWriter::from(f)?);
        }

        let file = fs::File::open(path)?;
        let size = file.metadata()?.len();
        let df = DataFile {
            path: path.to_path_buf(),
            id: file_id,
            writeable,
            reader: BufReaderWithOffset::new(file)?,
            writer: w,
            size: size,
        };

        Ok(df)
    }

    /// Save key-value pair to segement file.
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) -> Result<Entry> {
        let inner = InnerEntry::new(key, value);
        trace!("append {} to segement file {}", &inner, self.path.display());
        // avoid immutable borrowing issue.
        let path = self.path.as_path();
        let encoded = bincode::serialize(&inner)?;
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| TinkvError::FileNotWriteable(path.to_path_buf()))?;
        let offset = w.offset();
        w.write(&encoded)?;
        w.flush()?;

        self.size = offset + encoded.len() as u64;

        let entry = Entry::new(self.id, inner, encoded.len() as u64, offset);
        trace!(
            "successfully append {} to data file {}",
            &entry,
            self.path.display()
        );

        Ok(entry)
    }

    /// Read key value in data file.
    pub(crate) fn read(&mut self, offset: u64) -> Result<Entry> {
        trace!(
            "read key value with offset {} in data file {}",
            offset,
            self.path.display()
        );
        // Note: we have to get a mutable reader here.
        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(offset))?;
        let inner: InnerEntry = bincode::deserialize_from(reader)?;

        let entry = Entry::new(self.id, inner, self.reader.offset() - offset, offset);
        trace!(
            "successfully read {} from data log file {}",
            &entry,
            self.path.display()
        );
        Ok(entry)
    }

    /// Copy `size` bytes from `src` data file.
    /// Return offset of the newly written entry
    pub(crate) fn copy_bytes_from(
        &mut self,
        src: &mut DataFile,
        offset: u64,
        size: u64,
    ) -> Result<u64> {
        let reader = &mut src.reader;
        if reader.offset() != offset {
            reader.seek(SeekFrom::Start(offset))?;
        }

        let mut r = reader.take(size);
        let w = self.writer.as_mut().expect("data file is not writeable");
        let offset = w.offset();

        let num_bytes = copy(&mut r, w)?;
        assert_eq!(num_bytes, size);
        self.size += num_bytes;
        Ok(offset)
    }

    /// Return an entry iterator.
    pub(crate) fn entry_iter(&self) -> EntryIter {
        // TODO: refactor entry iter.
        EntryIter {
            path: self.path.clone(),
            reader: fs::File::open(self.path.clone()).unwrap(),
            file_id: self.id,
        }
    }

    /// Flush all pending writes to disk.
    pub(crate) fn sync(&mut self) -> Result<()> {
        self.flush()?;
        if self.writer.is_some() {
            self.writer.as_mut().unwrap().sync()?;
        }
        Ok(())
    }

    /// Flush buf writer.
    fn flush(&mut self) -> Result<()> {
        if self.writeable {
            self.writer.as_mut().unwrap().flush()?;
        }
        Ok(())
    }
}

impl Drop for DataFile {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            error!(
                "failed to sync data file: {}, got error: {}",
                self.path.display(),
                e
            );
        }

        // auto clean up if file size is zero.
        if self.writeable && self.size == 0 && fs::remove_file(self.path.as_path()).is_ok() {
            trace!("data file '{}' is empty, remove it.", self.path.display());
        }
    }
}

/// An iterator over a data file, return data entries.
#[derive(Debug)]
pub(crate) struct EntryIter {
    path: PathBuf,
    reader: fs::File,
    file_id: u64,
}

impl Iterator for EntryIter {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.reader.seek(SeekFrom::Current(0)).unwrap();
        let inner: InnerEntry = bincode::deserialize_from(&self.reader).ok()?;
        let new_offset = self.reader.seek(SeekFrom::Current(0)).unwrap();

        let entry = Entry::new(self.file_id, inner, new_offset - offset, offset);

        trace!(
            "iter read {} from data file {}",
            &entry,
            self.path.display()
        );

        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_entry() {
        let ent = InnerEntry::new(&b"key".to_vec(), &b"value".to_vec());
        assert_eq!(ent.checksum, 494360628);
    }

    #[test]
    fn test_checksum_valid() {
        let ent = InnerEntry::new(&b"key".to_vec(), &b"value".to_vec());
        assert_eq!(ent.is_valid(), true);
    }

    #[test]
    fn test_checksum_invalid() {
        let mut ent = InnerEntry::new(&b"key".to_vec(), &b"value".to_vec());
        ent.value = b"value_changed".to_vec();
        assert_eq!(ent.is_valid(), false);
    }
}
