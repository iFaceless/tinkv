//! A simple key-value store.
use crate::config;
use crate::error::Result;
use crate::{
    data::SegmentFile,
    keydir::KeyDir,
    util::{self},
};
use glob::glob;
use log::{info, trace};
use std::collections::HashMap;
use std::fs::create_dir_all;

use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Store {
    // directory for database.
    path: PathBuf,
    segments: HashMap<u64, SegmentFile>,
    active_segment: Option<SegmentFile>,
    key_dir: KeyDir,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Store> {
        info!("[store] open store path: {}", path.as_ref().display());
        create_dir_all(&path)?;

        let mut store = Store {
            path: path.as_ref().to_path_buf(),
            segments: HashMap::new(),
            active_segment: None,
            key_dir: KeyDir::new(path.as_ref()),
        };

        store.open_segments()?;
        store.new_segment_file()?;

        Ok(store)
    }

    /// Open segment files (they are immutable).
    fn open_segments(&mut self) -> Result<()> {
        let pattern = format!("{}/*{}", self.path.display(), config::DATA_FILE_SUFFIX);
        trace!("[store] read segment files with pattern: {}", &pattern);
        for path in glob(&pattern)? {
            let segment = SegmentFile::new(path?.as_path(), false)?;
            self.segments.insert(segment.id, segment);
        }
        trace!("[store] got {} segment files", self.segments.len());
        Ok(())
    }

    fn new_segment_file(&mut self) -> Result<()> {
        // Get new data file id.
        let max_file_id: &u64 = self.segments.keys().max().unwrap_or(&0);
        let next_file_id: u64 = max_file_id + 1;

        // Build data file path.
        let filename = format!("{:012}{}", next_file_id, config::DATA_FILE_SUFFIX);
        let mut p = self.path.clone();
        p.push(filename);

        trace!("[store] new segment data file at: {}", &p.display());
        self.active_segment = Some(SegmentFile::new(p.as_path(), true)?);
        Ok(())
    }

    /// Save key & value pair to database.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // We'are sure that active data file has been prepared.
        let segment = self.active_segment.as_mut().unwrap();
        let timestamp = util::current_timestamp();

        // Save data to data log file.
        let offset = segment.write(key, value, timestamp)?;

        // Update key dir, the in-memory index.
        self.key_dir.set(key, segment.id, offset, timestamp);

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
