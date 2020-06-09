//! A simple key-value store.
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub struct TinkvStore {

}

impl TinkvStore {
    pub fn new() -> Self { 
        TinkvStore{}
    }

    pub fn open() -> Self {
        Self::new()
    }
}
