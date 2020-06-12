//! Keydir is a in-memory hash table, which holds all the
//! keys with corresponding values for fast lookup.
use std::collections::BTreeMap;
use crate::error::{Result};

#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub file_id: u64,
    pub offset: u64,
    pub timestamp: u32,
}

#[derive(Debug)]
pub(crate) struct KeyDir {
    index: BTreeMap<Vec<u8>, Entry>,
}

impl KeyDir {
    pub fn new() -> Self {
        KeyDir {
            index: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: &[u8], file_id: u64, offset: u64, timestamp: u32) {
        self.index.insert(key.into(), Entry {
            file_id,
            offset,
            timestamp,
        });
    }
}
