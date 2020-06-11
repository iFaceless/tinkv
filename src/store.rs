//! A simple key-value store.
use crate::config;
use crate::error::Result;
use crate::{
    data, hint, keydir,
    util::{self, BufReaderWithOffset, BufWriterWithOffset},
};
use glob::glob;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Store {
    // directory for log file and hint file.
    path: PathBuf,
    file_manager: FileManager,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Store> {
        fs::create_dir_all(&path)?;
        let mut file_manager = FileManager::new(&path.as_ref());
        file_manager.init()?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file_manager: file_manager,
        })
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        Ok(())
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    pub fn remove(&self, key: Vec<u8>) -> Result<()> {
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        Ok(())
    }
}



#[derive(Debug)]
struct FileManager {
    path: PathBuf,
    writer: BufWriter<File>,
    readers: HashMap<u64, BufReader<File>>,
}

impl FileManager {
    fn new(path: &Path) -> Result<FileManager> {
        FileManager {
            path: path.to_path_buf(),
            writer: Self::new_active_file(path)?,
            readers: HashMap::new(),
        }
    }

    fn new_active_file(path: &Path) -> Result<BufWriterWithOffset<File>> {
        let p = Self::next_data_file_path(path)?;
        debug!("new active data file at: {}", &p.display());
        let w = BufWriterWithOffset::new(
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&p)?,
        )?;
        Ok(w)
    }

    fn next_data_file_path(path: &Path) -> Result<PathBuf> {
        let file_id = Self::next_file_id(path)?;
        let filename = format!("{:012}{}", file_id, config::DATA_FILE_SUFFIX);
        let mut p = path.to_path_buf();
        p.push(filename);
        Ok(p)
    }

    fn next_file_id(path: &Path) -> Result<u64> {
        // Collect file ids.
        let max_file_id: u64 = Self::
            list_data_files(path)?
            .iter()
            .map(|path| util::parse_file_id(path.as_path()).unwrap_or(0))
            .max()
            .unwrap_or(0);
        Ok(max_file_id + 1)
    }

    fn list_data_files(path: &Path) -> Result<Vec<PathBuf>> {
        let pattern = format!("{}/*{}", path.display(), config::DATA_FILE_SUFFIX);
        let mut filenames = vec![];
        for path in glob(&pattern)? {
            filenames.push(path?);
        }
        Ok(filenames)
    }
}
