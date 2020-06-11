use log;
use pretty_env_logger;
use std;
use std::path::Path;
use tinkv::{self, util, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open(".tinkv")?;
    store.set(b"key".to_vec(), b"value".to_vec())?;
    Ok(())
}
