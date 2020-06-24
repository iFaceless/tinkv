//! TinKV server listens at specific address, and serves
//! any request, returns an response to the client.
//! Communicate with client using a redis-compatible protocol.

use crate::error::{Result, TinkvError};

use crate::store::Store;
use crate::util::ByteLineReader;

use log::{debug, error, info, trace};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
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

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        info!("TinKV server is listening at '{}'", addr);
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            self.handle(stream?)?;
        }
        Ok(())
    }

    fn handle(&mut self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()?;
        debug!("got connection from {}", &peer_addr);

        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);

        macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                let data = resp.serialize();
                writer.write_all(data.as_bytes())?;
                writer.flush()?;
                debug!("Response sent to {}: {:?}", peer_addr, data);
            };};
        }

        let de = Deserializer::new(reader);
        for resp in de {
            debug!("got resp {:?}", resp);
            match resp {
                Err(e) => {
                    error!("got error when reading from {}: {}", &peer_addr, e);
                }
                Ok(r) => {
                    if let Some(req) = Request::new(r) {
                        send_resp!(self.process_request(req));
                    } else {
                        send_resp!(RespValue::Error {
                            name: "ERR".to_owned(),
                            msg: "unknown command".to_owned()
                        });
                    }
                }
            }
        }

        debug!("connection disconnected from {}", &peer_addr);

        Ok(())
    }

    fn process_request(&mut self, request: Request) -> RespValue {
        trace!("process request: {:?}", &request);
        match request.cmd.as_str() {
            "PING" => match request.args.len() {
                0 => RespValue::SimpleStr("PONG".to_owned()),
                1 => RespValue::BulkStr(request.args.get(0).unwrap().to_owned()),
                _ => {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'ping' command".to_owned(),
                    }
                }
            },
            "GET" => {
                if request.args.len() != 1 {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'get' command".to_owned(),
                    };
                }
                match self.store.get(request.args.get(0).unwrap().as_bytes()) {
                    Ok(v) => {
                        return v
                            .map(|x| RespValue::BulkStr(String::from_utf8_lossy(&x).to_string()))
                            .unwrap_or_else(|| RespValue::NullBulkStr);
                    }
                    Err(e) => RespValue::Error {
                        name: "STOREERR".to_owned(),
                        msg: format!("{}", e),
                    },
                }
            }
            "SET" => {
                if request.args.len() < 2 {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'set' command".to_owned(),
                    };
                }
                match self.store.set(
                    request.args.get(0).unwrap().as_bytes(),
                    request.args.get(1).unwrap().as_bytes(),
                ) {
                    Ok(_) => RespValue::SimpleStr("OK".to_owned()),
                    Err(e) => RespValue::Error {
                        name: "STOREERR".to_owned(),
                        msg: format!("{}", e),
                    },
                }
            }
            "DEL" => {
                if request.args.len() != 1 {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'del' command".to_owned(),
                    };
                }
                match self.store.remove(request.args.get(0).unwrap().as_bytes()) {
                    Ok(_) => RespValue::SimpleStr("OK".to_owned()),
                    Err(e) => RespValue::Error {
                        name: "STOREERR".to_owned(),
                        msg: format!("{}", e),
                    },
                }
            }
            "COMPACT" => {
                if request.args.len() != 0 {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'compact' command".to_owned(),
                    };
                }
                match self.store.compact() {
                    Ok(_) => RespValue::SimpleStr("OK".to_owned()),
                    Err(e) => RespValue::Error {
                        name: "STOREERR".to_owned(),
                        msg: format!("{}", e),
                    },
                }
            }
            "KEYS" => {
                if request.args.len() != 1 {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'keys' command".to_owned(),
                    };
                }

                let pattern: &String = request.args.get(0).unwrap();
                let mut resp: Vec<RespValue> = Vec::new();
                if pattern.ends_with("*") {
                    let prefix = pattern.trim_end_matches('*').as_bytes();
                    for k in self.store.keys() {
                        if prefix.len() == 0 || k.starts_with(prefix) {
                            // collect keys
                            resp.push(RespValue::BulkStr(String::from_utf8_lossy(k).into()));
                        }
                    }
                }
                if resp.len() == 0 {
                    return RespValue::NullArray;
                }

                return RespValue::Array(resp);
            },
            "DBSIZE" => {
                if request.args.len() != 0 {
                    return RespValue::Error {
                        name: "ERR".to_owned(),
                        msg: "wrong number of arguments for 'dbsize' command".to_owned(),
                    };
                }
                RespValue::Integer(self.store.len() as i64)
            }
            _ => RespValue::Error {
                name: "ERR".to_owned(),
                msg: format!("unsupported command '{}'", request.cmd),
            },
        }
    }
}

#[derive(Debug)]
struct Request {
    cmd: String,
    args: Vec<String>,
}

impl Request {
    fn new(rv: RespValue) -> Option<Request> {
        match rv {
            RespValue::Array(items) => {
                let mut req = Request {
                    cmd: "unknown".to_owned(),
                    args: vec![],
                };
                for item in items {
                    let s = match item {
                        RespValue::SimpleStr(s) => s,
                        RespValue::BulkStr(s) => s,
                        _ => continue,
                    };
                    if req.cmd == "unknown" {
                        req.cmd = s.to_uppercase();
                    } else {
                        req.args.push(s);
                    }
                }
                Some(req)
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
enum RespValue {
    SimpleStr(String),
    BulkStr(String),
    Error { name: String, msg: String },
    Integer(i64),
    Array(Vec<RespValue>),
    NullBulkStr,
    NullArray,
}

impl RespValue {
    fn serialize(&self) -> String {
        match self {
            RespValue::SimpleStr(s) => {
                return format!("+{}\r\n", s);
            }
            RespValue::Error { name, msg } => {
                return format!("-{} {}\r\n", name, msg);
            }
            RespValue::Integer(i) => {
                return format!(":{}\r\n", i);
            }
            RespValue::BulkStr(s) => {
                assert!(s.len() > 0, "non-empty bulk string need at least one char");
                return format!("${}\r\n{}\r\n", s.len(), s);
            }
            RespValue::Array(members) => {
                assert!(
                    members.len() > 0,
                    "non-empty array need at least one element"
                );
                let mut s: Vec<String> = Vec::new();
                for m in members {
                    s.push(m.serialize());
                }
                format!("*{}\r\n{}", members.len(), s.join(""))
            }
            RespValue::NullBulkStr => {
                return "$-1\r\n".to_owned();
            }
            RespValue::NullArray => {
                return "*-1\r\n".to_owned();
            }
        }
    }
}

#[derive(Debug)]
struct Deserializer<B: BufRead> {
    reader: ByteLineReader<B>,
}

impl<B: BufRead> Deserializer<B> {
    fn new(reader: B) -> Deserializer<B> {
        Self {
            reader: ByteLineReader::new(reader),
        }
    }

    fn deserialize(&mut self, bytes: &[u8]) -> Result<RespValue> {
        trace!("got bytes from reader: {}", String::from_utf8_lossy(bytes));
        match bytes[0] {
            b'+' => {
                // parse simple string
                let s = String::from_utf8_lossy(&bytes[1..]);
                Ok(RespValue::SimpleStr(s.into()))
            }
            b'-' => {
                // error string
                let s = String::from_utf8_lossy(&bytes[1..]).to_string();
                let err_name_end_index = s.find(char::is_whitespace).unwrap();
                Ok(RespValue::Error {
                    name: s[..err_name_end_index].to_owned(),
                    msg: s[err_name_end_index + 1..].to_owned(),
                })
            }
            b':' => {
                // integer
                let s = String::from_utf8_lossy(&bytes[1..]).to_string();
                let v = s.parse::<i64>()?;
                Ok(RespValue::Integer(v))
            }
            b'$' => {
                // bulk string
                let len: isize = String::from_utf8_lossy(&bytes[1..]).to_string().parse()?;
                if len == -1 {
                    return Ok(RespValue::NullBulkStr);
                }

                let mut buf = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut buf)?;
                Ok(RespValue::BulkStr(
                    String::from_utf8_lossy(&buf).trim().to_owned(),
                ))
            }
            b'*' => {
                // array
                trace!("array: {:?}", bytes);
                let len: isize = String::from_utf8_lossy(&bytes[1..]).to_string().parse()?;
                if len == -1 {
                    return Ok(RespValue::NullArray);
                }

                let mut members: Vec<RespValue> = vec![];
                for _ in 0..len {
                    members.push(self.next().unwrap()?);
                }
                Ok(RespValue::Array(members))
            }
            _ => Err(TinkvError::Custom(
                "Invalid request, parse failed".to_owned(),
            )),
        }
    }
}

impl<B: BufRead> Iterator for Deserializer<B> {
    type Item = Result<RespValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = &mut self.reader;
        let bytes = match reader.next() {
            None => return None,
            Some(Err(e)) => return Some(Err(e.into())),
            // TODO: avoid data copying.
            Some(Ok(chunk)) => chunk.to_vec(),
        };

        Some(self.deserialize(&bytes))
    }
}
