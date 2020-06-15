//! TinKV command line app.
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open(".tinkv")?;
    
    // for key in vec!["a", "b", "c", "d", "e"] {
    //     store.set(key.as_bytes(), key.as_bytes())?;
    // }
    // store.set("b".as_bytes(), "new_b".as_bytes())?;
    // store.remove("d".as_bytes())?;

    let value = store.get("b".as_bytes())?;
    println!("{:?}", String::from_utf8_lossy(&value.unwrap()));

    let value = store.get("d".as_bytes())?;
    println!("{:?}", value);
    Ok(())
}
