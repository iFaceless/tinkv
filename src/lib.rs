//! A simple key-value storage.
mod config;
mod error;
mod store;
mod segment;
pub mod util;

pub use error::{Result, TinkvError};
pub use store::Store;
