//! A simple key-value store.
use crate::config;
use crate::error::Result;
use crate::{keydir::KeyDir, log::SegmentFile, util};
use glob::glob;
use log::{info, trace};
use std::collections::HashMap;
use std::fs::create_dir_all;

use anyhow::anyhow;
use std::path::{Path, PathBuf};

/// The `Store` stores key/value pairs.
/// 
/// Key/value pairs are persisted in log segment files.
/// Log segment files will be merged when needed to release
/// disk space.
#[derive(Debug)]
pub struct Store {
    // directory for database.
    path: PathBuf,
    // holds a bunch of segments.
    segments: HashMap<u64, SegmentFile>,
    // only active segment is writeable.
    active_segment: Option<SegmentFile>,
    // key dir maintains key value index for fast query.
    key_dir: KeyDir,
}

impl Store {
    /// Initialize key value store with the given path.
    /// If the given path not found, a new one will be created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Store> {
        info!("open store path: {}", path.as_ref().display());
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
        trace!("read segment files with pattern: {}", &pattern);
        for path in glob(&pattern)? {
            let segment = SegmentFile::new(path?.as_path(), false)?;
            self.segments.insert(segment.id, segment);
        }
        trace!("got {} segment files", self.segments.len());
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

        trace!("new segment data file at: {}", &p.display());
        self.active_segment = Some(SegmentFile::new(p.as_path(), true)?);
        // Preapre a read-only segment file with the same path.
        let segment = SegmentFile::new(p.as_path(), false)?;
        self.segments.insert(segment.id, segment);

        Ok(())
    }

    /// Save key & value pair to database.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // We'are sure that active data file has been prepared.
        let segment = self
            .active_segment
            .as_mut()
            .expect("active segment not found");
        let timestamp = util::current_timestamp();

        // Save data to data log file.
        let offset = segment.write(key, value, timestamp)?;

        // Update key dir, the in-memory index.
        self.key_dir.set(key, segment.id, offset, timestamp);

        Ok(())
    }

    /// Get key value from database.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(ent) = self.key_dir.get(key) {
            trace!(
                "found key '{}' in keydir, got value {:?}",
                String::from_utf8_lossy(key),
                &ent
            );
            let segment = self
                .segments
                .get_mut(&ent.segment_id)
                .expect(format!("segment {} not found", &ent.segment_id).as_str());
            let data_entry = segment.read(ent.offset).unwrap();
            if !data_entry.is_valid() {
                return Err(anyhow!("data entry was corrupted"));
            }
            return Ok(Some(data_entry.value));
        } else {
            Ok(None)
        }
    }

    /// Remove key value from database.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        if self.key_dir.contains_key(key) {
            let segment = self
                .active_segment
                .as_mut()
                .expect("active segment not found");
            let timestamp = util::current_timestamp();
            // Write tomestone, will be removed on compaction.
            segment.write(key, config::REMOVE_TOMESTONE, timestamp)?;
            // Remove key from in-memory index.
            self.key_dir.remove(key).expect("key not found");
            Ok(())
        } else {
            Err(anyhow!(
                "key '{}' not found in database",
                String::from_utf8_lossy(key)
            ))
        }
    }

    /// Clear staled entries from segment files.
    /// Merge segment files.
    pub fn compact(&mut self) -> Result<()> {
        Ok(())
    }
}
