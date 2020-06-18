//! Maintain hint files. Each compacted log file
//! should bind with a hint file for faster loading.
use crate::error::Result;
use crate::util::parse_file_id;
use bincode;
use log::{error, trace};
use serde::{Deserialize, Serialize};
use std::fmt;
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
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HintEntry(key='{}', offset={}, size={})",
            String::from_utf8_lossy(self.key.as_ref()),
            self.offset,
            self.size,
        )
    }
}

/// A hint file persists key value indexes in a related data file.
/// TinKV can rebuild keydir (in-memory index) much faster if hint file
/// exists.
#[derive(Debug)]
pub struct HintFile<'a> {
    pub path: PathBuf,
    pub id: u64,
    entries_written: u64,
    writeable: bool,
    writer: Option<BufWriter<&'a File>>,
    inner: Option<File>,
    reader: BufReader<File>,
}

impl<'a> HintFile<'a> {
    pub(crate) fn new(path: &Path, writeable: bool) -> Result<Self> {
        // File name must starts with valid file id.
        let file_id = parse_file_id(path).expect("file id not found in file path");
        let mut inner = None;
        let mut w = None;
        
        if writeable {
            let f = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(path)?;
            w = Some(BufWriter::new(&f));
            inner = Some(f);
        }
        
        Ok(Self {
            path: path.to_path_buf(),
            id: file_id,
            writer: w,
            writeable,
            inner: inner,
            entries_written: 0,
            reader: BufReader::new(File::open(path)?),
        })
    }

    pub(crate) fn write(&mut self, key: &[u8], offset: u64, size: u64) -> Result<()> {
        let entry = Entry {
            key: key.into(),
            offset,
            size,
        };
        trace!("append {} to file {}", &entry, self.path.display());

        let w = self.writer.as_mut().expect("hint file is not writeable");
        bincode::serialize_into(w, &entry)?;
        self.entries_written += 1;

        self.flush()?;

        Ok(())
    }

    /// Flush all pending writes to disk.
    pub(crate) fn sync(&mut self) -> Result<()> {
        self.flush()?;
        if self.inner.is_some() {
            self.inner.as_mut().unwrap().sync_all()?;
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

    pub(crate) fn entry_iter(&'a mut self) -> EntryIter<'a> {
        EntryIter::new(self)
    }
}

impl<'a> Drop for HintFile<'a> {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            error!(
                "failed to sync hint file: {}, got error: {}",
                self.path.display(),
                e
            );
        }

        if self.writeable
            && self.entries_written == 0
            && fs::remove_file(self.path.as_path()).is_ok()
        {
            trace!("hint file {} is empty, remove it.", self.path.display());
        }
    }
}

pub(crate) struct EntryIter<'a> {
    hint_file: &'a mut HintFile<'a>,
    offset: u64,
}

impl<'a> EntryIter<'a> {
    fn new(hint_file: &'a mut HintFile<'a>) -> Self {
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
        let entry: Entry = bincode::deserialize_from(reader).ok()?;
        self.offset = self.hint_file.reader.seek(SeekFrom::Current(0)).unwrap();
        trace!(
            "iter read {} from hint file {}",
            &entry,
            self.hint_file.path.display()
        );
        Some(entry)
    }
}
