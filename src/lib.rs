//! A simple key-value storage.
pub mod config;
mod error;
mod resp;
mod segment;
mod server;
mod store;
pub mod util;

pub use error::{Result, TinkvError};
pub use server::Server;
pub use store::{OpenOptions, Store};
