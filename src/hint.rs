//! Hint file implementation.
use serde::{Deserialize, Serialize};

/// Entry in hint file, records file id, key, value position
/// and timestamp.
#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    key: Vec<u8>,
    file_id: u64,
    position: u64,
    timestamp: u32,
}

#[derive(Debug)]
pub(crate) struct File {}
