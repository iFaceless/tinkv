use pretty_env_logger;
use std::time;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init_timed();
    let mut store = Store::open(".tinkv")?;

    let begin = time::Instant::now();

    const TOTAL_KEYS: usize = 10;
    for i in 0..TOTAL_KEYS {
        let k = format!("hello_{}", i);
        let v = format!("world_{}", i);
        store.set(k.as_bytes(), v.as_bytes())?;
    }

    let duration = time::Instant::now().duration_since(begin);
    let speed = (TOTAL_KEYS) as f32 / duration.as_secs_f32();
    println!(
        "{} keys written in {} secs, {} keys/s",
        TOTAL_KEYS,
        duration.as_secs_f32(),
        speed
    );

    store.compact()?;

    let v = store.get("hello_1".as_bytes())?.unwrap_or_default();
    println!("{}", String::from_utf8_lossy(&v));

    Ok(())
}
