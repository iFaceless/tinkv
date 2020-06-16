//! A simple key-value store.
use crate::config;
use crate::error::Result;
use crate::{log::SegmentFile, util};
use glob::glob;
use log::{info, trace};
use std::collections::{BTreeMap, HashMap};
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
    // keydir maintains key value index for fast query.
    keydir: BTreeMap<Vec<u8>, KeyDirEntry>,
    // size of stale entries that could be reclaimed during compaction.
    uncompacted: u64,
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
            keydir: BTreeMap::new(),
            uncompacted: 0,
        };

        store.open_segments()?;
        store.build_keydir()?;
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

    fn build_keydir(&mut self) -> Result<()> {
        // TODO: build keydir from index file.
        // Fallback to original log file to rebuild keydir.
        let mut segment_ids: Vec<_> = self.segments.keys().collect();
        segment_ids.sort();

        for segment_id in segment_ids {
            let segment = self.segments.get(segment_id).unwrap();
            trace!("build key dir from segment file {}", segment.path.display());
            for (offset, ent) in segment.entry_iter() {
                if !ent.is_valid() {
                    return Err(anyhow!("data entry was corrupted, current key is '{}', segment file id {}, offset {}", String::from_utf8_lossy(&ent.key), segment.id, offset));
                }

                if ent.value == config::REMOVE_TOMESTONE {
                    self.keydir.remove(&ent.key);
                }

                let keydir_ent = KeyDirEntry::new(segment_id.clone(), offset, ent.timestamp);
                let existed = self.keydir.get(&ent.key);
                match existed {
                    None => {
                        self.keydir.insert(ent.key, keydir_ent);
                    }
                    Some(existed_ent) => {
                        if existed_ent.timestamp < ent.timestamp {
                            self.keydir.insert(ent.key, keydir_ent);
                        }
                    }
                }
            }
        }
        info!("build keydir done, got {} keys", self.keydir.len());
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
        self.keydir.insert(key.to_vec(), KeyDirEntry::new(segment.id, offset, timestamp));

        Ok(())
    }

    /// Get key value from database.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(ent) = self.keydir.get(key) {
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
                return Err(anyhow!(
                    "data entry was corrupted, current key is '{}', segment file id {}, offset {}",
                    String::from_utf8_lossy(key),
                    segment.id,
                    ent.offset
                ));
            }
            return Ok(Some(data_entry.value));
        } else {
            Ok(None)
        }
    }

    /// Remove key value from database.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        if self.keydir.contains_key(key) {
            let segment = self
                .active_segment
                .as_mut()
                .expect("active segment not found");
            let timestamp = util::current_timestamp();
            // Write tomestone, will be removed on compaction.
            segment.write(key, config::REMOVE_TOMESTONE, timestamp)?;
            // Remove key from in-memory index.
            self.keydir.remove(key).expect("key not found");
            Ok(())
        } else {
            Err(anyhow!(
                "key '{}' not found in database",
                String::from_utf8_lossy(key)
            ))
        }
    }

    /// Clear staled entries from segment files and reclaim disk space.
    pub fn compact(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Entry definition in the keydir (the in-memory index).
#[derive(Debug, Clone, Copy)]
struct KeyDirEntry {
    segment_id: u64,
    offset: u64,
    timestamp: u128,
}

impl KeyDirEntry {
    fn new(segment_id: u64, offset: u64, timestamp: u128) -> Self {
        KeyDirEntry {
            segment_id,
            offset,
            timestamp,
        }
    }
}
