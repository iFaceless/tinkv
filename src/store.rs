//! A simple key-value store.
use std::path::{Path, PathBuf};
use std::fs;
use crate::error::Result;
use crate::{hint, data, keydir};

#[derive(Debug)]
pub struct Store {
    // directory for log file and hint file.
    path: PathBuf,
    data_file_manager: data::FileManager,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        fs::create_dir_all(&path)?;
        let mut data_file_manager = data::FileManager::new(&path.as_ref());
        data_file_manager.prepare()?;

        Ok(Self { 
            path: path.as_ref().to_path_buf(),
            data_file_manager: data_file_manager,
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
