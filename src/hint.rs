//! Hint file implementation.
use crate::error::Result;
use crate::util::parse_file_id;
use bincode;
use log::{error, trace};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};

/// Entry in the hint file.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub key: Vec<u8>,
    pub offset: u64,
    pub size: u64,
    pub timestamp: u128,
}

/// A hint file persists key value indexes in a related segment log file.
/// TinKV can rebuild keydir (in-memory index) much faster if hint file
/// exists.
#[derive(Debug)]
pub struct SegmentHintFile {
    pub path: PathBuf,
    pub id: u64,
    entries_written: u64,
    writer: Option<BufWriter<File>>,
    reader: BufReader<File>,
}

impl SegmentHintFile {
    pub(crate) fn new(path: &Path, writeable: bool) -> Result<Self> {
        // Segment name must starts with valid file id.
        let file_id = parse_file_id(path).expect("file id not found in file path");
        let w = if writeable {
            Some(BufWriter::new(
                fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(path)?,
            ))
        } else {
            None
        };

        Ok(Self {
            path: path.to_path_buf(),
            id: file_id,
            writer: w,
            entries_written: 0,
            reader: BufReader::new(File::open(path)?),
        })
    }

    pub(crate) fn write(
        &mut self,
        key: &[u8],
        offset: u64,
        size: u64,
        timestamp: u128,
    ) -> Result<()> {
        let entry = Entry {
            key: key.into(),
            offset,
            size,
            timestamp,
        };
        trace!(
            "append hint entry: {:?} to file {}",
            &entry,
            self.path.display()
        );

        let w = self.writer.as_mut().expect("hint file is not writeable");
        bincode::serialize_into(w, &entry)?;
        self.entries_written += 1;

        self.flush()?;

        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.writer.is_some() {
            self.writer.as_mut().unwrap().flush()?;
        }
        Ok(())
    }

    pub(crate) fn entry_iter(&mut self) -> EntryIter {
        EntryIter::new(self)
    }
}

impl Drop for SegmentHintFile {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            error!(
                "failed to flush hint file: {}, got error: {}",
                self.path.display(),
                e
            );
        }

        if self.entries_written == 0 && fs::remove_file(self.path.as_path()).is_ok() {
            trace!("hint file {} is empty, remove it.", self.path.display());
        }
    }
}

pub(crate) struct EntryIter<'a> {
    hint_file: &'a mut SegmentHintFile,
    offset: u64,
}

impl<'a> EntryIter<'a> {
    fn new(hint_file: &'a mut SegmentHintFile) -> Self {
        EntryIter {
            hint_file,
            offset: 0,
        }
    }
}

impl<'a> Iterator for EntryIter<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = &mut self.hint_file.reader;
        reader.seek(SeekFrom::Start(self.offset)).unwrap();
        let entry: Entry = bincode::deserialize_from(reader).unwrap();
        self.offset = self.hint_file.reader.seek(SeekFrom::Current(0)).unwrap();
        trace!(
            "successfully read hint entry {:?} from hint file {}",
            &entry,
            self.hint_file.path.display()
        );
        Some(entry)
    }
}
