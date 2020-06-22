//! TinKV server listens at specific address, and serves
//! any request, returns an response to the client.
//! Communicate with client using a redis-compatible protocol.
use crate::error::Result;
use log::info;
use std::net::ToSocketAddrs;

#[derive(Debug)]
pub struct Server {}

impl Server {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Server {}
    }

    #[allow(dead_code)]
    pub fn run<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let a = addr.to_socket_addrs()?.next().unwrap();
        info!("TinKV server is listening at {}", a);
        Ok(())
    }
}
