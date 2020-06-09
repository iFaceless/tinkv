use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy)]
struct KeyDirEntry {
    file_id: u64,
    offset: u64,
    timestamp: u64,
}