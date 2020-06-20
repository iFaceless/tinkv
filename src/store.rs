//! A simple key-value store.
use crate::config;
use crate::error::{Result, TinkvError};
use crate::segment::{DataEntry, DataFile, HintFile};
use glob::glob;
use log::{info, trace};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::create_dir_all;

use std::path::{Path, PathBuf};

/// The `Store` stores key/value pairs.
///
/// Key/value pairs are persisted in data files.
#[derive(Debug)]
pub struct Store {
    // directory for database.
    path: PathBuf,
    // holds a bunch of data files.
    data_files: HashMap<u64, DataFile>,
    // only active data file is writeable.
    active_data_file: Option<DataFile>,
    // keydir maintains key value index for fast query.
    keydir: BTreeMap<Vec<u8>, KeyDirEntry>,
    /// monitor tinkv store status, record statistics data.
    stats: Stats,
    /// store config.
    config: Config,
}

impl Store {
    /// Initialize key value store with the given path.
    /// If the given path not found, a new one will be created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, Config::default())
    }

    /// Open datasotre directory with custom options.
    fn open_with_options<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        info!("open store path: {}", path.as_ref().display());
        create_dir_all(&path)?;
        let mut store = Store {
            path: path.as_ref().to_path_buf(),
            data_files: HashMap::new(),
            active_data_file: None,
            keydir: BTreeMap::new(),
            stats: Stats::default(),
            config,
        };

        store.open_data_files()?;
        store.build_keydir()?;
        store.new_active_data_file(None)?;

        Ok(store)
    }

    /// Open data files (they are immutable).
    fn open_data_files(&mut self) -> Result<()> {
        let pattern = format!("{}/*{}", self.path.display(), config::DATA_FILE_SUFFIX);
        trace!("read data files with pattern: {}", &pattern);
        for path in glob(&pattern)? {
            let df = DataFile::new(path?.as_path(), false)?;

            self.stats.total_data_files += 1;
            self.stats.size_of_all_data_files += df.size;

            self.data_files.insert(df.id, df);
        }
        trace!("got {} immutable data files", self.data_files.len());

        Ok(())
    }

    fn build_keydir(&mut self) -> Result<()> {
        // TODO: build keydir from index file.
        // fallback to the original data file to rebuild keydir.
        let mut file_ids = self.data_files.keys().cloned().collect::<Vec<_>>();
        file_ids.sort();

        for file_id in file_ids {
            let hint_file_path = segment_hint_file_path(&self.path, file_id);
            if hint_file_path.exists() {
                self.build_keydir_from_hint_file(&hint_file_path)?;
            } else {
                self.build_keydir_from_data_file(file_id)?;
            }
        }

        // update stats.
        self.stats.total_active_entries = self.keydir.len() as u64;

        info!(
            "build keydir done, got {} keys. current stats: {:?}",
            self.keydir.len(),
            self.stats
        );
        Ok(())
    }

    fn build_keydir_from_hint_file(&mut self, path: &Path) -> Result<()> {
        trace!("build keydir from hint file {}", path.display());
        let mut hint_file = HintFile::new(path, false)?;
        let hint_file_id = hint_file.id.clone();

        for entry in hint_file.entry_iter() {
            let keydir_ent = KeyDirEntry::new(hint_file_id, entry.offset, entry.size);
            self.keydir.insert(entry.key, keydir_ent);
        }
        Ok(())
    }

    fn build_keydir_from_data_file(&mut self, file_id: u64) -> Result<()> {
        let df = self.data_files.get(&file_id).unwrap();
        info!("build key dir from data file {}", df.path.display());
        for entry in df.entry_iter() {
            if !entry.is_valid() {
                return Err(TinkvError::DataEntryCorrupted {
                    file_id: df.id,
                    key: entry.key().into(),
                    offset: entry.offset,
                });
            }

            if entry.value() == config::REMOVE_TOMESTONE {
                trace!("{} is a remove tomestone", &entry);
                self.stats.total_stale_entries += 1;
                self.stats.size_of_stale_entries += entry.size;

                if let Some(old_ent) = self.keydir.remove(entry.key()) {
                    self.stats.size_of_stale_entries += old_ent.size;
                    self.stats.total_stale_entries += 1;
                }
            } else {
                let keydir_ent = KeyDirEntry::new(file_id.clone(), entry.offset, entry.size);
                self.keydir.insert(entry.key().into(), keydir_ent);
            }
        }
        Ok(())
    }

    fn new_active_data_file(&mut self, file_id: Option<u64>) -> Result<()> {
        // default next file id should be `max_file_id` + 1
        let next_file_id: u64 =
            file_id.unwrap_or_else(|| self.data_files.keys().max().unwrap_or(&0) + 1);

        // build data file path.
        let p = segment_data_file_path(&self.path, next_file_id);
        trace!("new data file at: {}", &p.display());
        self.active_data_file = Some(DataFile::new(p.as_path(), true)?);

        // preapre a read-only data file with the same path.
        let df = DataFile::new(p.as_path(), false)?;
        self.data_files.insert(df.id, df);

        self.stats.total_data_files += 1;

        Ok(())
    }

    /// Save key & value pair to database.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() as u64 > self.config.max_key_size {
            return Err(TinkvError::KeyIsTooLarge);
        }

        if value.len() as u64 > self.config.max_value_size {
            return Err(TinkvError::ValueIsTooLarge);
        }

        // save data to data file.
        let ent = self.write(key, value)?;

        // update key dir, the in-memory index.
        let old = self.keydir.insert(
            key.to_vec(),
            KeyDirEntry::new(ent.file_id, ent.offset, ent.size),
        );

        match old {
            None => {
                self.stats.total_active_entries += 1;
            }
            Some(entry) => {
                self.stats.size_of_stale_entries += entry.size;
                self.stats.total_stale_entries += 1;
            }
        }

        self.stats.size_of_all_data_files += ent.size;

        Ok(())
    }

    /// Remove key value from database.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        if self.keydir.contains_key(key) {
            trace!(
                "remove key '{}' from datastore",
                String::from_utf8_lossy(key)
            );
            // write tomestone, will be removed on compaction.
            let entry = self.write(key, config::REMOVE_TOMESTONE)?;
            // remove key from in-memory index.
            let old = self.keydir.remove(key).expect("key not found");

            self.stats.total_active_entries -= 1;
            self.stats.total_stale_entries += 1;
            self.stats.size_of_all_data_files += entry.size;
            self.stats.size_of_stale_entries += old.size + entry.size;

            Ok(())
        } else {
            trace!(
                "remove key '{}' failed, not found in datastore",
                String::from_utf8_lossy(key)
            );
            Err(TinkvError::KeyNotFound(key.into()))
        }
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<DataEntry> {
        let mut df = self
            .active_data_file
            .as_mut()
            .expect("active data file not found");

        // check file size, switch to another one if nessesary.
        if df.size > self.config.max_data_file_size {
            info!("size of active data file '{}' exceeds maximum size of {} bytes, switch to another one.", df.path.display(), self.config.max_data_file_size);

            // close current active data file.
            drop(df);

            // create a new active data file.
            self.new_active_data_file(None)?;

            // get new active data file for writting.
            df = self
                .active_data_file
                .as_mut()
                .expect("active data file not found");
        }

        let entry = df.write(key, value)?;
        if self.config.sync {
            // make sure data entry is persisted in storage.
            df.sync()?;
        }

        Ok(entry)
    }

    /// Get key value from database.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(keydir_ent) = self.keydir.get(key) {
            trace!(
                "found key '{}' in keydir, got value {:?}",
                String::from_utf8_lossy(key),
                &keydir_ent
            );
            let df = self
                .data_files
                .get_mut(&keydir_ent.segment_id)
                .expect(format!("data file {} not found", &keydir_ent.segment_id).as_str());
            let entry = df.read(keydir_ent.offset)?;
            if !entry.is_valid() {
                return Err(TinkvError::DataEntryCorrupted {
                    file_id: df.id,
                    key: entry.key().into(),
                    offset: entry.offset,
                });
            }
            return Ok(Some(entry.value().into()));
        } else {
            Ok(None)
        }
    }

    /// Clear stale entries from data files and reclaim disk space.
    pub fn compact(&mut self) -> Result<()> {
        info!(
            "there are {} data files need to be compacted",
            self.data_files.len()
        );
        let compaction_data_file_id = self.next_file_id();
        // switch to another active data file.
        self.new_active_data_file(Some(compaction_data_file_id + 1))?;

        // create a new data file for compaction.
        let data_file_path = segment_data_file_path(&self.path, compaction_data_file_id);

        trace!("create compaction data file: {}", data_file_path.display());
        let mut compaction_df = DataFile::new(&data_file_path, true)?;

        // create a new hint file to store compaction file index.
        let hint_file_path = segment_hint_file_path(&self.path, compaction_data_file_id);

        trace!("create compaction hint file: {}", hint_file_path.display());
        let mut hint_file = HintFile::new(&hint_file_path, true)?;

        // copy all the data entries into compaction data file.
        // TODO: check if data file size exceeds threshold, switch
        // to another one if nessesary.
        for (key, keydir_ent) in self.keydir.iter_mut() {
            let df = self
                .data_files
                .get_mut(&keydir_ent.segment_id)
                .expect("cannot find data file");
            trace!(
                "copy key '{}': original data file({}) -> compaction data file({})",
                String::from_utf8_lossy(key),
                df.path.display(),
                compaction_df.path.display()
            );
            let offset = compaction_df.copy_bytes_from(df, keydir_ent.offset, keydir_ent.size)?;

            keydir_ent.segment_id = compaction_df.id;
            keydir_ent.offset = offset;

            hint_file.write(key, keydir_ent.offset, keydir_ent.size)?;
        }

        compaction_df.sync()?;
        hint_file.sync()?;

        // remove stale segments.
        let mut stale_segment_count = 0;
        for df in self.data_files.values() {
            if df.id < compaction_data_file_id {
                if df.path.exists() {
                    trace!("try to remove stale data file: {}", df.path.display());
                    fs::remove_file(&df.path)?;
                }

                let hint_file_path = segment_hint_file_path(&self.path, df.id);
                if hint_file_path.exists() {
                    trace!(
                        "try to remove stale hint file: {}",
                        &hint_file_path.display()
                    );
                    fs::remove_file(&hint_file_path)?;
                }

                stale_segment_count += 1;
            }
        }

        self.data_files.retain(|&k, _| k >= compaction_data_file_id);
        trace!("cleaned {} stale segments", stale_segment_count);

        // register read-only compaction data file.
        self.data_files
            .insert(compaction_df.id, DataFile::new(&compaction_df.path, false)?);

        // update stats.
        self.stats.total_data_files = self.data_files.len() as u64;
        self.stats.total_active_entries = self.keydir.len() as u64;
        self.stats.total_stale_entries = 0;
        self.stats.size_of_stale_entries = 0;
        self.stats.size_of_all_data_files = compaction_df.size;

        Ok(())
    }

    fn next_file_id(&self) -> u64 {
        self.active_data_file
            .as_ref()
            .expect("active data file not found")
            .id
            + 1
    }

    /// Return current stats of storage engine.
    pub fn stats(&mut self) -> &Stats {
        &self.stats
    }

    /// Return all keys in data store.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.keydir.keys()
    }

    /// Force any writes to disk.
    pub fn sync(&mut self) -> Result<()> {
        if self.active_data_file.is_some() {
            self.active_data_file.as_mut().unwrap().sync()?;
        }
        Ok(())
    }

    /// Close a tinkv data store, flush all pending writes to disk.
    pub fn close(&mut self) -> Result<()> {
        self.sync()?;
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // ignore sync errors.
        trace!("sync all pending writes to disk.");
        let _r = self.sync();
    }
}

/// Entry definition in the keydir (the in-memory index).
#[derive(Debug, Clone, Copy)]
struct KeyDirEntry {
    /// data file id that stores key value pair.
    segment_id: u64,
    /// data entry offset in data file.
    offset: u64,
    /// data entry size.
    size: u64,
}

impl KeyDirEntry {
    fn new(segment_id: u64, offset: u64, size: u64) -> Self {
        KeyDirEntry {
            segment_id,
            offset,
            size,
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Stats {
    /// size (bytes) of stale entries in data files, which can be
    /// deleted after a compaction.
    pub size_of_stale_entries: u64,
    /// total stale entries in data files.
    pub total_stale_entries: u64,
    /// total active key value pairs in database.
    pub total_active_entries: u64,
    /// total data files.
    pub total_data_files: u64,
    /// total size (bytes) of all data files.
    pub size_of_all_data_files: u64,
}

fn segment_data_file_path(dir: &Path, segment_id: u64) -> PathBuf {
    segment_file_path(dir, segment_id, config::DATA_FILE_SUFFIX)
}

fn segment_hint_file_path(dir: &Path, segment_id: u64) -> PathBuf {
    segment_file_path(dir, segment_id, config::HINT_FILE_SUFFIX)
}

fn segment_file_path(dir: &Path, segment_id: u64, suffix: &str) -> PathBuf {
    let mut p = dir.to_path_buf();
    p.push(format!("{:012}{}", segment_id, suffix));
    p
}

#[derive(Debug, Copy, Clone)]
struct Config {
    max_data_file_size: u64,
    max_key_size: u64,
    max_value_size: u64,
    // sync data to storage after each writting operation.
    // we should balance data reliability and writting performance.
    sync: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_data_file_size: config::DEFAULT_MAX_DATA_FILE_SIZE,
            max_key_size: config::DEFAULT_MAX_KEY_SIZE,
            max_value_size: config::DEFAULT_MAX_VALUE_SIZE,
            sync: false,
        }
    }
}

/// Build custom open options.
#[derive(Debug)]
pub struct OpenOptions {
    config: Config,
}

impl OpenOptions {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

    #[allow(dead_code)]
    pub fn max_data_file_size(&mut self, value: u64) -> &mut Self {
        self.config.max_data_file_size = value;
        self
    }

    #[allow(dead_code)]
    pub fn max_key_size(&mut self, value: u64) -> &mut Self {
        self.config.max_key_size = value;
        self
    }

    #[allow(dead_code)]
    pub fn max_value_size(&mut self, value: u64) -> &mut Self {
        self.config.max_value_size = value;
        self
    }

    #[allow(dead_code)]
    pub fn sync(&mut self, value: bool) -> &mut Self {
        self.config.sync = value;
        self
    }

    #[allow(dead_code)]
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<Store> {
        Store::open_with_options(path, self.config.clone())
    }
}
