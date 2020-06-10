//! Keydir is a in-memory hash table, which holds all the
//! keys with corresponding values for fast lookup.
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
struct Entry {
    file_id: u64,
    position: u64,
    timestamp: u32,
}

#[derive(Debug)]
pub(crate) struct KeyDir {
    index: BTreeMap<Vec<u8>, Entry>,
}

impl KeyDir {
    
}