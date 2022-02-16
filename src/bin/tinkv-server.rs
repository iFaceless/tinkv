//! TinKV server is a redis-compatible storage server.
use chrono::Local;
use clap_verbosity_flag::Verbosity;
use env_logger;
use env_logger::Builder;
use log::LevelFilter;
use std::error::Error;
use std::io::Write;
use std::net::SocketAddr;

use log::{debug, info};
use structopt::StructOpt;
use tinkv::{config, OpenOptions, Server};

const DEFAULT_DATASTORE_PATH: &str = "/opt/tinkv";
const DEFAULT_LISTENING_ADDR: &str = "127.0.0.1:7379";

#[derive(Debug, StructOpt)]
#[structopt(
rename_all = "kebab-case",
name = "tinkv-server",
version = env ! ("CARGO_PKG_VERSION"),
author = env ! ("CARGO_PKG_AUTHORS"),
about = "TiKV is a redis-compatible key/value storage server.",
)]
struct Opt {
    #[structopt(flatten)]
    verbose: Verbosity,
    /// Set listening address.
    #[structopt(
    short = "a",
    long,
    value_name = "IP:PORT",
    default_value = DEFAULT_LISTENING_ADDR,
    parse(try_from_str),
    )]
    addr: SocketAddr,
    /// Set max key size (in bytes).
    #[structopt(long, value_name = "KEY-SIZE")]
    max_key_size: Option<u64>,
    /// Set max value size (in bytes).
    #[structopt(long, value_name = "VALUE-SIZE")]
    max_value_size: Option<u64>,
    /// Set max file size (in bytes).
    #[structopt(long, value_name = "FILE-SIZE")]
    max_data_file_size: Option<u64>,
    /// Sync all pending writes to disk after each writing operation (default to false).
    #[structopt(long, value_name = "SYNC")]
    sync: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();

    let log_level = match opt.verbose.log_level().unwrap_or_else(|| log::Level::Info) {
        log::Level::Error => LevelFilter::Error,
        log::Level::Warn => LevelFilter::Warn,
        log::Level::Info => LevelFilter::Info,
        log::Level::Debug => LevelFilter::Debug,
        log::Level::Trace => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, log_level)
        .init();

    debug!("get tinkv server config from command line: {:?}", &opt);
    let store = OpenOptions::new()
        .max_key_size(
            opt.max_key_size
                .unwrap_or_else(|| config::DEFAULT_MAX_KEY_SIZE),
        )
        .max_value_size(
            opt.max_value_size
                .unwrap_or_else(|| config::DEFAULT_MAX_VALUE_SIZE),
        )
        .max_data_file_size(
            opt.max_data_file_size
                .unwrap_or_else(|| config::DEFAULT_MAX_DATA_FILE_SIZE),
        )
        .sync(opt.sync)
        .open(DEFAULT_DATASTORE_PATH)?;

    info!("start server now");
    Server::new(store).run(opt.addr)?;

    Ok(())
}
