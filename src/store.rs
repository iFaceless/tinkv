//! A simple key-value store.
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub struct Store {

}

impl Store {
    pub fn new() -> Self { 
        Store{}
    }

    pub fn open() -> Self {
        Self::new()
    }
}
