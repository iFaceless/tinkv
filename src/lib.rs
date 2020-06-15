//! A simple key-value store.
mod config;
mod log;
mod error;
mod hint;
mod keydir;
mod store;
pub mod util;

pub use error::{Result, TinkvError};
pub use store::Store;
