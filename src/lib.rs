//! A simple key-value store.
mod error;
mod store;
mod config;
mod util;
mod data;
mod hint;
mod keydir;

pub use error::{TinkvError, Result};
pub use store::{Store};