//! A simple key-value store.

mod error;
mod tinkv;
mod config;
mod file;
mod util;

pub use error::{TinkvError, Result};
pub use tinkv::{TinkvStore};