//! A simple key-value store.
use crate::config;
use crate::error::Result;
use crate::{log::SegmentFile, util};
use glob::glob;
use log::{debug, info, trace};
use std::collections::{BTreeMap, HashMap};
use std::fs;
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
    /// monitor tinkv store status, record statistics data.
    stats: Stats,
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
            stats: Stats::default(),
        };

        store.open_segments()?;
        store.build_keydir()?;
        store.new_active_segment(None)?;

        Ok(store)
    }

    /// Open segment files (they are immutable).
    fn open_segments(&mut self) -> Result<()> {
        let pattern = format!("{}/*{}", self.path.display(), config::DATA_FILE_SUFFIX);
        trace!("read segment files with pattern: {}", &pattern);
        for path in glob(&pattern)? {
            let segment = SegmentFile::new(path?.as_path(), false)?;

            self.stats.total_segment_files += 1;
            self.stats.size_of_all_segment_files += segment.size;

            self.segments.insert(segment.id, segment);
        }
        trace!("got {} segment files", self.segments.len());

        Ok(())
    }

    fn build_keydir(&mut self) -> Result<()> {
        // TODO: build keydir from index file.
        // fallback to original log file to rebuild keydir.
        let mut segment_ids: Vec<_> = self.segments.keys().collect();
        segment_ids.sort();

        for segment_id in segment_ids {
            let segment = self.segments.get(segment_id).unwrap();
            debug!("build key dir from segment file {}", segment.path.display());
            for entry in segment.entry_iter() {
                if !entry.is_valid() {
                    return Err(anyhow!("data entry was corrupted, current key is '{}', segment file id {}, offset {}", String::from_utf8_lossy(entry.key()), segment.id, entry.offset));
                }

                if entry.value() == config::REMOVE_TOMESTONE {
                    self.stats.total_stale_entries += 1;
                    self.stats.size_of_stale_entries += entry.size;
                    self.stats.total_active_entries -= 1;

                    if let Some(old_ent) = self.keydir.remove(entry.key()) {
                        self.stats.size_of_stale_entries += old_ent.size;
                        self.stats.total_stale_entries += 1;
                    }
                }

                let keydir_ent = KeyDirEntry::new(
                    segment_id.clone(),
                    entry.offset,
                    entry.size,
                    entry.timestamp(),
                );
                let existed = self.keydir.get(entry.key());
                match existed {
                    None => {
                        self.keydir.insert(entry.key().into(), keydir_ent);
                        self.stats.total_active_entries += 1;
                    }
                    Some(existed_ent) => {
                        if existed_ent.timestamp < entry.timestamp() {
                            self.keydir.insert(entry.key().into(), keydir_ent);
                        }
                    }
                }
            }
        }
        info!(
            "build keydir done, got {} keys. current stats: {:?}",
            self.keydir.len(),
            self.stats
        );
        Ok(())
    }

    fn new_active_segment(&mut self, segment_id: Option<u64>) -> Result<()> {
        // default next segment id should be `max_segment_id` + 1
        let next_file_id: u64 =
            segment_id.unwrap_or_else(|| self.segments.keys().max().unwrap_or(&0) + 1);

        // build data file path.
        let p = segment_filename(&self.path, next_file_id);
        trace!("new segment data file at: {}", &p.display());
        self.active_segment = Some(SegmentFile::new(p.as_path(), true)?);

        // preapre a read-only segment file with the same path.
        let segment = SegmentFile::new(p.as_path(), false)?;
        self.segments.insert(segment.id, segment);

        self.stats.total_segment_files += 1;

        Ok(())
    }

    /// Save key & value pair to database.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // we'are sure that active data file has been prepared.
        let segment = self
            .active_segment
            .as_mut()
            .expect("active segment not found");
        let timestamp = util::current_timestamp();

        // save data to log file.
        let ent = segment.write(key, value, timestamp)?;

        // update key dir, the in-memory index.
        let old = self.keydir.insert(
            key.to_vec(),
            KeyDirEntry::new(segment.id, ent.offset, ent.size, timestamp),
        );

        if old.is_none() {
            self.stats.total_active_entries += 1;
        }

        self.stats.size_of_all_segment_files += ent.size;

        if self.stats.size_of_stale_entries > config::COMPATION_THRESHOLD {
            // TODO: trigger compaction asynchrously.
            self.compact()?;
        }

        Ok(())
    }

    /// Get key value from database.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(keydir_ent) = self.keydir.get(key) {
            trace!(
                "found key '{}' in keydir, got value {:?}",
                String::from_utf8_lossy(key),
                &keydir_ent
            );
            let segment = self
                .segments
                .get_mut(&keydir_ent.segment_id)
                .expect(format!("segment {} not found", &keydir_ent.segment_id).as_str());
            let entry = segment.read(keydir_ent.offset).unwrap();
            if !entry.is_valid() {
                return Err(anyhow!(
                    "data entry was corrupted, current key is '{}', segment file id {}, offset {}",
                    String::from_utf8_lossy(key),
                    segment.id,
                    keydir_ent.offset
                ));
            }
            return Ok(Some(entry.value().into()));
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
            // write tomestone, will be removed on compaction.
            let entry = segment.write(key, config::REMOVE_TOMESTONE, timestamp)?;
            // remove key from in-memory index.
            let old = self.keydir.remove(key).expect("key not found");

            self.stats.total_active_entries -= 1;
            self.stats.total_stale_entries += 1;
            self.stats.size_of_all_segment_files += entry.size;
            self.stats.size_of_stale_entries += old.size + entry.size;

            if self.stats.size_of_stale_entries > config::COMPATION_THRESHOLD {
                // TODO: trigger compaction asynchrously.
                self.compact()?;
            }

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
        let compaction_segment_id = self.next_segment_id();
        // switch to another active segment.
        self.new_active_segment(Some(compaction_segment_id + 1))?;

        // create a new segment file for compaction.
        let p = segment_filename(&self.path, compaction_segment_id);

        trace!("create compaction segment file: {}", p.display());
        let mut compaction_segment = SegmentFile::new(&p, true)?;

        // copy all the data entries into compaction segment file.
        // TODO: check if segment file size exceeds threshold, switch
        // to another one if nessesary.
        for (key, keydir_ent) in self.keydir.iter_mut() {
            let segment = self
                .segments
                .get_mut(&keydir_ent.segment_id)
                .expect("cannot find segment file");
            trace!(
                "copy key '{}': original segment({}) -> compaction segment({})",
                String::from_utf8_lossy(key),
                segment.path.display(),
                compaction_segment.path.display()
            );
            let offset =
                compaction_segment.copy_bytes_from(segment, keydir_ent.offset, keydir_ent.size)?;

            keydir_ent.segment_id = compaction_segment.id;
            keydir_ent.offset = offset;
        }

        compaction_segment.flush()?;

        // remove stale segments.
        let mut stale_segment_count = 0;
        for segment in self.segments.values() {
            if segment.id < compaction_segment.id {
                trace!("try to remove stale segment: {}", segment.path.display());
                fs::remove_file(&segment.path)?;
                stale_segment_count += 1;
            }
        }

        self.segments.retain(|&k, _| k >= compaction_segment_id);
        trace!("cleaned {} stale segments", stale_segment_count);

        // update stats.
        self.stats.total_segment_files = self.segments.len() as u64;
        self.stats.total_active_entries = self.keydir.len() as u64;
        self.stats.total_stale_entries = 0;
        self.stats.size_of_stale_entries = 0;
        self.stats.size_of_all_segment_files = compaction_segment.size;

        Ok(())
    }

    fn next_segment_id(&self) -> u64 {
        self.active_segment
            .as_ref()
            .expect("active segment not found")
            .id
            + 1
    }

    /// Return stats of storage engine.
    pub fn stats(&mut self) -> &Stats {
        &self.stats
    }
}

/// Entry definition in the keydir (the in-memory index).
#[derive(Debug, Clone, Copy)]
struct KeyDirEntry {
    /// segement file that stores key value pair.
    segment_id: u64,
    /// data entry offset in segment file.
    offset: u64,
    /// data entry size.
    size: u64,
    timestamp: u128,
}

impl KeyDirEntry {
    fn new(segment_id: u64, offset: u64, size: u64, timestamp: u128) -> Self {
        KeyDirEntry {
            segment_id,
            offset,
            size,
            timestamp,
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Stats {
    /// size (bytes) of stale entries in log files, which can be
    /// deleted after a compaction.
    pub size_of_stale_entries: u64,
    /// total stale entries in log files.
    pub total_stale_entries: u64,
    /// total active key value pairs in database.
    pub total_active_entries: u64,
    /// total log segment files.
    pub total_segment_files: u64,
    /// total size (bytes) of all log segment files.
    pub size_of_all_segment_files: u64,
}

fn segment_filename(dir: &Path, segment_id: u64) -> PathBuf {
    let mut p = dir.to_path_buf();
    p.push(format!("{:012}{}", segment_id, config::DATA_FILE_SUFFIX));
    p
}
