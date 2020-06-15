//! A simple key-value store.
mod config;
mod error;
mod hint;
mod keydir;
mod log;
mod store;
pub mod util;

pub use error::{Result, TinkvError};
pub use store::Store;
