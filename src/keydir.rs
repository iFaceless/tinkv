//! Keydir is a in-memory hash table, which holds all the
//! keys with corresponding values for fast lookup.

#[derive(Debug, Clone, Copy)]
struct Entry {
    file_id: u64,
    position: u64,
    timestamp: u32,
}

struct KeyDir {

}

impl KeyDir {
    
}