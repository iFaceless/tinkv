//! TinKV command line app.
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open(".tinkv")?;
    // for k in vec!["a", "b", "c", "d"] {
    //     store.set(k.as_bytes(), k.as_bytes())?;
    // }
    println!("{:?}", store.get("c".as_bytes()));
    Ok(())
}
