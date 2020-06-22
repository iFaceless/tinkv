//! TinKV server listens at specific address, and serves
//! any request, returns an response to the client.
//! Communicate with client using a redis-compatible protocol.
use crate::config;
use crate::error::Result;
use crate::store::OpenOptions;
use crate::store::Store;
use log::info;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Server {
    store: Store,
    options: ServerOptions,
}

impl Server {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::with_options(&ServerOptions::default())
    }

    pub fn with_options(options: &ServerOptions) -> Self {
        let store = OpenOptions::new()
            .max_data_file_size(options.max_data_file_size)
            .max_key_size(options.max_key_size)
            .max_value_size(options.max_value_size)
            .sync(options.sync);
        
    }

    #[allow(dead_code)]
    pub fn run<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let a = addr.to_socket_addrs()?.next().unwrap();
        info!("TinKV server is listening at {}", a);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ServerOptions {
    path: PathBuf,
    max_data_file_size: u64,
    max_key_size: u64,
    max_value_size: u64,
    // sync data to storage after each writting operation.
    // we should balance data reliability and writting performance.
    sync: bool,
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            path: "/tmp/tinkv".into(),
            max_data_file_size: config::DEFAULT_MAX_DATA_FILE_SIZE,
            max_key_size: config::DEFAULT_MAX_KEY_SIZE,
            max_value_size: config::DEFAULT_MAX_VALUE_SIZE,
            sync: false,
        }
    }
}
impl ServerOptions {
    pub fn new() -> ServerOptions {
        Self::default()
    }

    pub fn max_data_file_size(&mut self, value: u64) -> &mut Self {
        self.max_data_file_size = value;
        self
    }

    pub fn max_key_size(&mut self, value: u64) -> &mut Self {
        self.max_key_size = value;
        self
    }

    pub fn max_value_size(&mut self, value: u64) -> &mut Self {
        self.max_value_size = value;
        self
    }

    pub fn sync(&mut self, value: bool) -> &mut Self {
        self.sync = value;
        self
    }
}
