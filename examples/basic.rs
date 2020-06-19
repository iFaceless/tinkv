use pretty_env_logger;
use std::time;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init_timed();
    let mut store = Store::open(".tinkv")?;

    let begin = time::Instant::now();

    const TOTAL_KEYS: usize = 100000;
    for i in 0..TOTAL_KEYS {
        let k = format!("key_{}", i);
        let v = format!(
            "value_{}_{}_hello_world_this_is_a_bad_day",
            i,
            tinkv::util::current_timestamp()
        );
        store.set(k.as_bytes(), v.as_bytes())?;
        store.set(k.as_bytes(), v.as_bytes())?;
    }

    let duration = time::Instant::now().duration_since(begin);
    let speed = (TOTAL_KEYS * 2) as f32 / duration.as_secs_f32();
    println!(
        "{} keys written in {} secs, {} keys/s",
        TOTAL_KEYS * 2,
        duration.as_secs_f32(),
        speed
    );

    println!("initial: {:?}", store.stats());

    let v = store.get("key_1".as_bytes())?.unwrap_or_default();
    println!("key_1 => {:?}", String::from_utf8_lossy(&v));

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

    let v = store.get("key_1".as_bytes())?.unwrap();
    println!("key_1 => {:?}", String::from_utf8_lossy(&v));

    Ok(())
}
