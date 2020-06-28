//! TinKV server is a redis-compatible key value server.

use crate::error::{Result, TinkvError};

use crate::store::Store;

use crate::resp::{deserialize_from_reader, serialize_to_writer, Value};
use lazy_static::lazy_static;
use log::{debug, info, trace};

use crate::util::to_utf8_string;
use std::convert::TryFrom;
use std::fmt;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

lazy_static! {
    static ref COMMANDS: Vec<&'static str> = vec![
        "ping", "get", "mget", "set", "mset", "del", "dbsize", "exists", "keys", "compact", "info",
        "command",
    ];
}

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
            self.handle_request(&mut conn, Request::try_from(value?)?)?;
        }

        debug!("connection disconnected from {}", &peer_addr);

        Ok(())
    }

    fn handle_request<W: Write>(&mut self, conn: &mut Conn<W>, req: Request) -> Result<()> {
        trace!("handle {}", &req);
        let argv = req.argv();

        macro_rules! send {
            () => {
                conn.write_value(Value::new_null_bulk_string())?
            };
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
            "ping" => send!(self.handle_ping(&argv)),
            "get" => send!(self.handle_get(&argv)),
            "mget" => send!(self.handle_mget(&argv)),
            "set" => send!(self.handle_set(&argv)),
            "mset" => send!(self.handle_mset(&argv)),
            "del" => send!(self.handle_del(&argv)),
            "dbsize" => send!(self.handle_dbsize(&argv)),
            "exists" => send!(self.handle_exists(&argv)),
            "keys" => send!(self.handle_keys(&argv)),
            "compact" => send!(self.handle_compact(&argv)),
            "info" => send!(self.handle_info(&argv)),
            "command" => send!(self.handle_command(&argv)),
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

    fn handle_ping(&mut self, argv: &[&[u8]]) -> Result<Value> {
        match argv.len() {
            0 => Ok(Value::new_simple_string("PONG")),
            1 => Ok(Value::new_bulk_string(argv[0].to_vec())),
            _ => Err(TinkvError::resp_wrong_num_of_args("ping")),
        }
    }

    fn handle_get(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if argv.len() != 1 {
            return Err(TinkvError::resp_wrong_num_of_args("get"));
        }

        Ok(self
            .store
            .get(argv[0])?
            .map(Value::new_bulk_string)
            .unwrap_or_else(Value::new_null_bulk_string))
    }

    fn handle_mget(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if argv.is_empty() {
            return Err(TinkvError::resp_wrong_num_of_args("mget"));
        }

        let mut values = vec![];
        for arg in argv {
            let value = self
                .store
                .get(arg)?
                .map(Value::new_bulk_string)
                .unwrap_or_else(Value::new_null_bulk_string);
            values.push(value);
        }

        Ok(Value::new_array(values))
    }

    fn handle_set(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if argv.len() < 2 {
            return Err(TinkvError::resp_wrong_num_of_args("set"));
        }

        match self.store.set(argv[0], argv[1]) {
            Ok(()) => Ok(Value::new_simple_string("OK")),
            Err(e) => Err(TinkvError::new_resp_common(
                "INTERNALERR",
                &format!("{}", e),
            )),
        }
    }

    fn handle_mset(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if argv.len() % 2 != 0 {
            return Err(TinkvError::resp_wrong_num_of_args("mset"));
        }

        let mut i = 0;
        loop {
            if i + 1 >= argv.len() {
                break;
            }

            if let Err(e) = self.store.set(argv[i], argv[i + 1]) {
                return Err(TinkvError::new_resp_common(
                    "INTERNALERR",
                    &format!("{}", e),
                ));
            }

            i += 2;
        }

        Ok(Value::new_simple_string("OK"))
    }

    fn handle_del(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if argv.len() != 1 {
            return Err(TinkvError::resp_wrong_num_of_args("del"));
        }

        match self.store.remove(argv[0]) {
            Ok(()) => Ok(Value::new_simple_string("OK")),
            Err(e) => Err(TinkvError::new_resp_common(
                "INTERNALERR",
                &format!("{}", e),
            )),
        }
    }

    fn handle_dbsize(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if !argv.is_empty() {
            return Err(TinkvError::resp_wrong_num_of_args("dbsize"));
        }

        Ok(Value::new_integer(self.store.len() as i64))
    }

    fn handle_exists(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if argv.is_empty() {
            return Err(TinkvError::resp_wrong_num_of_args("exists"));
        }

        let mut exists = 0;
        for arg in argv {
            if self.store.contains_key(arg) {
                exists += 1;
            }
        }

        Ok(Value::new_integer(exists as i64))
    }

    fn handle_keys(&mut self, argv: &[&[u8]]) -> Result<Value> {
        let pattern = match argv.len() {
            0 => glob::Pattern::new("*"),
            1 => glob::Pattern::new(to_utf8_string(argv[0]).as_ref()),
            _ => return Err(TinkvError::resp_wrong_num_of_args("exists")),
        };

        let mut keys = vec![];

        let pattern = pattern.map_err(|e| TinkvError::new_resp_common("ERR", &format!("{}", e)))?;
        for key in self.store.keys() {
            if pattern.matches(to_utf8_string(key).as_ref()) {
                keys.push(Value::new_bulk_string(key.to_vec()));
            };
        }

        Ok(Value::new_array(keys))
    }

    fn handle_compact(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if !argv.is_empty() {
            return Err(TinkvError::resp_wrong_num_of_args("compact"));
        }

        match self.store.compact() {
            Ok(_) => Ok(Value::new_simple_string("OK")),
            Err(e) => Err(TinkvError::new_resp_common(
                "INTERNALERR",
                &format!("{}", e),
            )),
        }
    }

    fn handle_info(&mut self, argv: &[&[u8]]) -> Result<Value> {
        let server_section = || {
            let mut info = String::new();
            info.push_str("# Server\n");
            info.push_str(&format!("tinkv_version: {}\n", env!("CARGO_PKG_VERSION")));

            let os = os_info::get();
            info.push_str(&format!(
                "os: {}, {}, {}\n",
                os.os_type(),
                os.version(),
                os.bitness()
            ));
            info
        };

        let stats_section = || {
            let mut info = String::new();
            info.push_str("# Stats\n");
            let stats = self.store.stats();
            info.push_str(&format!(
                "size_of_stale_entries: {}\n",
                stats.size_of_stale_entries
            ));
            info.push_str(&format!(
                "size_of_stale_entries_human: {}\n",
                bytefmt::format(stats.size_of_stale_entries)
            ));
            info.push_str(&format!(
                "total_stale_entries: {}\n",
                stats.total_stale_entries
            ));
            info.push_str(&format!(
                "total_active_entries: {}\n",
                stats.total_active_entries
            ));
            info.push_str(&format!("total_data_files: {}\n", stats.total_data_files));
            info.push_str(&format!(
                "size_of_all_data_files: {}\n",
                stats.size_of_all_data_files
            ));
            info.push_str(&format!(
                "size_of_all_data_files_human: {}\n",
                bytefmt::format(stats.size_of_all_data_files)
            ));
            info
        };

        let mut info = Vec::new();

        match argv.len() {
            0 => {
                info.push(server_section());
                info.push(stats_section());
            }
            1 => match to_utf8_string(argv[0]).to_ascii_lowercase().as_ref() {
                "server" => {
                    info.push(server_section());
                }
                "stats" => {
                    info.push(stats_section());
                }
                _ => {}
            },
            _ => return Err(TinkvError::resp_wrong_num_of_args("info")),
        }

        Ok(Value::new_bulk_string(info.join("\n").as_bytes().to_vec()))
    }

    fn handle_command(&mut self, argv: &[&[u8]]) -> Result<Value> {
        if !argv.is_empty() {
            return Err(TinkvError::resp_wrong_num_of_args("command"));
        }

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
    raw_argv: Vec<Value>,
}

impl Request {
    fn argv(&self) -> Vec<&[u8]> {
        let mut res = vec![];
        for arg in self.raw_argv.iter() {
            if let Some(v) = arg.as_bulk_string() {
                res.push(v);
            }
        }
        res
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut argv_str = vec![];
        for arg in self.raw_argv.iter() {
            argv_str.push(format!("{}", arg));
        }

        write!(
            f,
            "Request(name=\"{}\", argc={}, argv={:?})",
            &self.name,
            argv_str.len(),
            argv_str
        )
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
                let name = to_utf8_string(v.remove(0).as_bulk_string().unwrap());
                Ok(Self {
                    name: name.to_ascii_lowercase(),
                    raw_argv: v,
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
        trace!("send value to client: {}", value);
        serialize_to_writer(&mut self.writer, &value)?;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
