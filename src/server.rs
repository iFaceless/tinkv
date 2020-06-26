//! TinKV server listens at specific address, and serves
//! any request, returns an response to the client.
//! Communicate with client using a redis-compatible protocol.

use crate::error::{Result, TinkvError};

use crate::store::Store;

use crate::resp::{deserialize_from_reader, serialize_to_writer, Value};
use lazy_static::lazy_static;
use log::{debug, info, trace};

use std::convert::TryFrom;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

lazy_static! {
    static ref COMMANDS: Vec<&'static str> = vec!["ping", "get", "set", "del", "command",];
}

pub struct Server {
    _store: Store,
}

impl Server {
    #[allow(dead_code)]
    pub fn new(store: Store) -> Self {
        Server { _store: store }
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        info!("TinKV server is listening at '{}'", addr);
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            self.serve(stream?)?;
        }
        Ok(())
    }

    fn serve(&mut self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()?;
        debug!("got connection from {}", &peer_addr);
        let reader = BufReader::new(&stream);
        let writer = BufWriter::new(&stream);
        let mut conn = Conn::new(writer);

        for value in deserialize_from_reader(reader) {
            let req: Request = Request::try_from(value?)?;
            self.handle_request(&mut conn, req)?;
        }

        debug!("connection disconnected from {}", &peer_addr);

        Ok(())
    }

    fn handle_request<W: Write>(&mut self, conn: &mut Conn<W>, req: Request) -> Result<()> {
        trace!("got request: `{}`, args: `{:?}`", &req.name, &req.args);
        let args = req.args_as_slice();

        macro_rules! send {
            ($value:expr) => {
                match $value {
                    Err(TinkvError::RespCommon { name, msg }) => {
                        let err = Value::new_error(&name, &msg);
                        conn.write_value(err)?;
                    }
                    Err(TinkvError::RespWrongNumOfArgs(_)) => {
                        let msg = format!("{}", $value.unwrap_err());
                        let err = Value::new_error("ERR", &msg);
                        conn.write_value(err)?;
                    }
                    Err(e) => return Err(e),
                    Ok(v) => conn.write_value(v)?,
                }
            };
        }

        match req.name.as_ref() {
            "ping" => send!(self.handle_ping(&args)),
            "command" => send!(self.handle_command()),
            _ => {
                conn.write_value(Value::new_error(
                    "ERR",
                    &format!("unknown command `{}`", &req.name),
                ))?;
            }
        }

        conn.flush()?;

        Ok(())
    }

    fn handle_ping(&mut self, args: &Vec<&[u8]>) -> Result<Value> {
        match args.len() {
            0 => Ok(Value::new_simple_string("PONG")),
            1 => Ok(Value::new_bulk_string(args[0].to_vec())),
            _ => Err(TinkvError::resp_wrong_num_of_args("ping")),
        }
    }

    fn handle_command(&mut self) -> Result<Value> {
        let mut values = vec![];
        for cmd in COMMANDS.iter() {
            values.push(Value::new_bulk_string(cmd.as_bytes().to_vec()));
        }

        Ok(Value::new_array(values))
    }
}

#[derive(Debug)]
struct Request {
    name: String,
    args: Vec<Value>,
}
impl Request {
    fn args_as_slice(&self) -> Vec<&[u8]> {
        let mut res = vec![];
        for arg in self.args.iter() {
            if let Some(v) = arg.as_bulk_string() {
                res.push(v);
            }
        }
        res
    }
}

impl TryFrom<Value> for Request {
    type Error = TinkvError;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Array(mut v) => {
                if v.is_empty() {
                    return Err(TinkvError::ParseRespValue);
                }
                let name =
                    String::from_utf8_lossy(v.remove(0).as_bulk_string().unwrap()).to_string();
                Ok(Self {
                    name: name.to_ascii_lowercase(),
                    args: v,
                })
            }
            _ => Err(TinkvError::ParseRespValue),
        }
    }
}

struct Conn<W> {
    writer: W,
}

impl<W> Conn<W>
where
    W: Write,
{
    fn new(writer: W) -> Self {
        Self { writer }
    }

    fn write_value(&mut self, value: Value) -> Result<()> {
        trace!("send value to client: {:?}", value);
        serialize_to_writer(&mut self.writer, &value)?;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
