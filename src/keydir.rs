//! Keydir is a in-memory hash table, which holds all the
//! keys with corresponding values for fast lookup.
use log::trace;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub segment_id: u64,
    pub offset: u64,
    pub timestamp: u32,
}

#[derive(Debug)]
pub(crate) struct KeyDir {
    index: BTreeMap<Vec<u8>, Entry>,
}

impl KeyDir {
    pub(crate) fn new(_path: &Path) -> Self {
        KeyDir {
            index: BTreeMap::new(),
        }
    }

    pub(crate) fn set(&mut self, key: &[u8], segment_id: u64, offset: u64, timestamp: u32) {
        let ent = Entry {
            segment_id,
            offset,
            timestamp,
        };

        trace!("set {} => {:?}", String::from_utf8_lossy(key), &ent);
        self.index.insert(key.into(), ent);
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.index.get(key)
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.index.remove(key)
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.index.len()
    }
}
