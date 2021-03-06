//! TinKV server is a redis-compatible storage server.
use clap_verbosity_flag::Verbosity;
use std::error::Error;
use std::net::SocketAddr;

use log::debug;
use structopt::StructOpt;
use tinkv::{config, OpenOptions, Server};

const DEFAULT_DATASTORE_PATH: &str = "/usr/local/var/tinkv";
const DEFAULT_LISTENING_ADDR: &str = "127.0.0.1:7379";

#[derive(Debug, StructOpt)]
#[structopt(
    rename_all = "kebab-case", 
    name = "tinkv-server",
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
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
    if let Some(level) = opt.verbose.log_level() {
        std::env::set_var("RUST_LOG", format!("{}", level));
    }
    pretty_env_logger::init_timed();

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

    Server::new(store).run(opt.addr)?;

    Ok(())
}
