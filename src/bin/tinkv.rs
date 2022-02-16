//! TinKV command line app.
use clap_verbosity_flag::Verbosity;
use std::path::PathBuf;
use std::process;
use structopt::{self, StructOpt};
use tinkv::{self, Store};

#[derive(Debug, StructOpt)]
enum SubCommand {
    /// Retrive value of a key, and display the value.
    Get { key: String },
    /// Store a key value pair into datastore.
    Set { key: String, value: String },
    #[structopt(name = "del")]
    /// Delete a key value pair from datastore.
    Delete { key: String },
    /// List all keys in datastore.
    Keys,
    /// Perform a prefix scanning for keys.
    Scan { prefix: String },
    /// Compact data files in datastore and reclaim disk space.
    Compact,
    /// Display statistics of the datastore.
    Stats,
}

#[derive(Debug, StructOpt)]
#[structopt(
    rename_all = "kebab-case", 
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
    about = env!("CARGO_PKG_DESCRIPTION"),
)]
struct Opt {
    #[structopt(flatten)]
    verbose: Verbosity,
    /// Path to tinkv datastore.
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(subcommand)]
    cmd: SubCommand,
}

fn main() {
    let opt = Opt::from_args();
    if let Some(level) = opt.verbose.log_level() {
        std::env::set_var("RUST_LOG", format!("{}", level));
    }

    pretty_env_logger::init_timed();
    match dispatch(&opt) {
        Ok(_) => {
            process::exit(0);
        }
        Err(e) => {
            eprintln!("operation failed: {}", e);
            process::exit(1);
        }
    }
}

fn dispatch(opt: &Opt) -> tinkv::Result<()> {
    let mut store = Store::open(&opt.path)?;

    // dispacth subcommand handler.
    match &opt.cmd {
        SubCommand::Get { key } => {
            handle_get_command(&mut store, key.as_bytes())?;
        }
        SubCommand::Set { key, value } => {
            handle_set_command(&mut store, key.as_bytes(), value.as_bytes())?;
        }
        SubCommand::Delete { key } => {
            handle_delete_command(&mut store, key.as_bytes())?;
        }
        SubCommand::Compact => {
            handle_compact_command(&mut store)?;
        }
        SubCommand::Keys => {
            handle_keys_command(&mut store)?;
        }
        SubCommand::Scan { prefix } => {
            handle_scan_command(&mut store, prefix.as_bytes())?;
        }
        SubCommand::Stats => {
            handle_stats_command(&mut store)?;
        }
    }
    Ok(())
}

fn handle_set_command(store: &mut Store, key: &[u8], value: &[u8]) -> tinkv::Result<()> {
    store.set(key, value)?;
    Ok(())
}

fn handle_get_command(store: &mut Store, key: &[u8]) -> tinkv::Result<()> {
    let value = store.get(key)?;
    match value {
        None => {
            println!(
                "key '{}' is not found in datastore",
                String::from_utf8_lossy(key)
            );
        }
        Some(value) => {
            println!("{}", String::from_utf8_lossy(&value));
        }
    }
    Ok(())
}

fn handle_delete_command(store: &mut Store, key: &[u8]) -> tinkv::Result<()> {
    store.remove(key)?;
    Ok(())
}

fn handle_compact_command(store: &mut Store) -> tinkv::Result<()> {
    store.compact()?;
    Ok(())
}

fn handle_keys_command(store: &mut Store) -> tinkv::Result<()> {
    store.keys().for_each(|key| {
        println!("{}", String::from_utf8_lossy(key));
    });
    Ok(())
}

fn handle_scan_command(store: &mut Store, prefix: &[u8]) -> tinkv::Result<()> {
    // TODO: Optimize it, prefix scanning is too slow if there are too
    // many keys in datastore. Consider using other data structure like
    // `Trie`.
    store.keys().for_each(|key| {
        if key.starts_with(prefix) {
            println!("{}", String::from_utf8_lossy(key));
        }
    });
    Ok(())
}

fn handle_stats_command(store: &mut Store) -> tinkv::Result<()> {
    let stats = store.stats();
    println!(
        "size of stale entries = {}
total stale entries = {}
total active entries = {}
total data files = {}
size of all data files = {}",
        bytefmt::format(stats.size_of_stale_entries),
        stats.total_stale_entries,
        stats.total_active_entries,
        stats.total_data_files,
        bytefmt::format(stats.size_of_all_data_files),
    );
    Ok(())
}
