//! TinKV command line app.
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open(".tinkv")?;
    store.set(&b"key".to_vec(), &b"value".to_vec())?;
    store.set(&b"key".to_vec(), &b"value new".to_vec())?;
    let value = store.get(&b"key".to_vec())?;
    println!("{:?}", String::from_utf8_lossy(&value.unwrap()));
    Ok(())
}
