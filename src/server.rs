//! TinKV server listens at specific address, and serves
//! any request, returns an response to the client.
//! Communicate with client using a redis-compatible protocol.

use crate::error::{Result};

use crate::store::{Store};
use log::info;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};


#[derive(Debug)]
pub struct Server {
    store: Store,
}

impl Server {
    #[allow(dead_code)]
    pub fn new(store: Store) -> Self {
        Server { store }
    }

    #[allow(dead_code)]
    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        info!("TinKV server is listening at '{}'", addr);
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let stream: TcpStream = stream?;
            info!("got connection from {:?}", &stream);
        }
        Ok(())
    }
}
