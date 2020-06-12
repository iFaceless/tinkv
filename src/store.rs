//! A simple key-value store.
use crate::config;
use crate::error::Result;
use crate::{
    util::{self, BufReaderWithOffset, BufWriterWithOffset},
    keydir::{KeyDir},
    data,
};
use glob::glob;
use log::{debug};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::prelude::*;

use std::path::{Path, PathBuf};
use serde::{Serialize};
use bincode;

#[derive(Debug)]
pub struct Store {
    // directory for log file and hint file.
    path: PathBuf,
    file_manager: FileManager,
    key_dir: KeyDir,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Store> {
        fs::create_dir_all(&path)?;
        let file_manager = FileManager::new(&path.as_ref())?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file_manager: file_manager,
            key_dir: KeyDir::new(),
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut df = self.file_manager.active_data_file.unwrap();
        let (offset, entry) = df.set(key, value)?;
        self.key_dir.set(key, df.file_id().unwrap(), offset, entry.timestamp);
        Ok(())
    }

    pub fn get(&self, _key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    pub fn remove(&self, _key: Vec<u8>) -> Result<()> {
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct DataFile {
    path: PathBuf,
    writeable: bool,
    reader: BufReaderWithOffset<File>,
    writer: Option<Box<BufWriterWithOffset<File>>>,
}

impl DataFile {
    fn new(path: &Path, writeable: bool) -> Result<Self> {
        let mut df = DataFile {
            path: path.to_path_buf(),
            writeable: writeable,
            reader: BufReaderWithOffset::new(fs::File::open(path)?)?,
            writer: None,
        };

        if writeable {
            let w = BufWriterWithOffset::new(
                fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(path)?,
            )?;
            df.writer = Some(Box::new(w))
        }
        Ok(df)
    }

    fn file_id(&self) -> Option<u64> {
        util::parse_file_id(self.path.as_path())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(u64, data::Entry)> {
        let ent = data::Entry::new(key, value);
        let encoded = bincode::serialize(&ent)?;
        let mut w = self.writer.unwrap();
        w.write(&encoded)?;
        Ok((w.offset(), ent))
    }
}

#[derive(Debug)]
struct FileManager {
    path: PathBuf,
    active_data_file: Option<DataFile>,
    data_files: HashMap<u64, DataFile>,
}

impl FileManager {
    fn new(path: &Path) -> Result<FileManager> {
        let mut fm = FileManager {
            path: path.to_path_buf(),
            active_data_file: None,
            data_files: HashMap::new(),
        };
        fm.new_active_data_file()?;
        fm.open_data_files()?;
        Ok(fm)
    }

    fn new_active_data_file(&mut self) -> Result<()> {
        let p = self.next_data_file_path()?;
        debug!("new active data file at: {}", &p.display());
        self.active_data_file = Some(DataFile::new(p.as_path(), true)?);
        Ok(())
    }

    fn open_data_files(&mut self) -> Result<()> {
        for path in self.list_data_files()? {
            let df = DataFile::new(path.as_path(), false)?;
            // TODO: whatif parsing file id failed?
            self.data_files.insert(df.file_id().unwrap(), df);
        }
        Ok(())
    }

    fn next_data_file_path(&self) -> Result<PathBuf> {
        let file_id = self.next_file_id()?;
        let filename = format!("{:012}{}", file_id, config::DATA_FILE_SUFFIX);
        let mut p = self.path.clone();
        p.push(filename);
        Ok(p)
    }

    fn next_file_id(&self) -> Result<u64> {
        // Collect file ids.
        let max_file_id: u64 = self
            .list_data_files()?
            .iter()
            .map(|path| util::parse_file_id(path.as_path()).unwrap_or(0))
            .max()
            .unwrap_or(0);
        Ok(max_file_id + 1)
    }

    fn list_data_files(&self) -> Result<Vec<PathBuf>> {
        let pattern = format!("{}/*{}", self.path.display(), config::DATA_FILE_SUFFIX);
        let mut filenames = vec![];
        for path in glob(&pattern)? {
            filenames.push(path?);
        }
        Ok(filenames)
    }
}
