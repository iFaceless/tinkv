//! A simple key-value store.
use std::path::{PathBuf};
use crate::error::Result;
use crate::{hint, data, keydir};

#[derive(Debug)]
pub struct Store {
    // directory for log file and hint file.
    path: PathBuf,
    data_files: Vec<data::File>,
    active_data_file: data::File,
    active_hint_file: hint::File,
    keydir: keydir::KeyDir,
}

impl Store {
    pub fn open<P: Into<PathBuf>>(path: P) -> Self {
        
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
