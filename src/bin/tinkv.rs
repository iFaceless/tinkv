//! TinKV command line app.
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open(".tinkv")?;
    // for k in vec!["a", "b", "c", "d"] {
    //     store.set(k.as_bytes(), k.as_bytes())?;
    // }
    println!("initial: {:?}", store.stats());

    store.set("hello".as_bytes(), "tinkv".as_bytes())?;
    println!("after set 1: {:?}", store.stats());

    store.set("hello".as_bytes(), "tinkv 2".as_bytes())?;
    println!("after set 2: {:?}", store.stats());

    store.set("hello 2".as_bytes(), "tinkv".as_bytes())?;
    println!("after set 3: {:?}", store.stats());

    let value = store.get("hello".as_bytes())?;
    assert_eq!(value, Some("tinkv 2".as_bytes().to_vec()));

    store.remove("hello".as_bytes())?;
    println!("after remove: {:?}", store.stats());

    let value_not_found = store.get("hello".as_bytes())?;
    assert_eq!(value_not_found, None);
    
    store.compact()?;
    println!("after compaction: {:?}", store.stats());

    Ok(())
}
