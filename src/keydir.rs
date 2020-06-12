//! Keydir is a in-memory hash table, which holds all the
//! keys with corresponding values for fast lookup.
use log::trace;
use std::collections::BTreeMap;
use std::path::{Path};

#[derive(Debug, Clone, Copy)]
struct Entry {
    pub file_id: u64,
    pub offset: u64,
    pub timestamp: u32,
}

#[derive(Debug)]
pub(crate) struct KeyDir {
    index: BTreeMap<Vec<u8>, Entry>,
}

impl KeyDir {
    pub fn new(_path: &Path) -> Self {
        KeyDir {
            index: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: &[u8], file_id: u64, offset: u64, timestamp: u32) {
        let ent = Entry {
            file_id,
            offset,
            timestamp,
        };

        trace!("[keydir] set {} => {:?}", String::from_utf8_lossy(key), &ent);
        self.index.insert(key.into(), ent);
    }
}
