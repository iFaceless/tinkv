//! A simple key-value storage.
mod config;
mod error;
mod segment;
mod store;
mod resp;
mod server;
pub mod util;

pub use error::{Result, TinkvError};
pub use store::{OpenOptions, Store};
