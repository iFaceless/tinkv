use pretty_env_logger;
use std::time;
use tinkv::{self};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init_timed();
    let mut store = tinkv::OpenOptions::new()
        .max_data_file_size(1024 * 100)
        .open("/usr/local/var/tinkv")?;

    let begin = time::Instant::now();

    const TOTAL_KEYS: usize = 1000;
    for i in 0..TOTAL_KEYS {
        let k = format!("hello_{}", i);
        let v = format!("world_{}", i);
        store.set(k.as_bytes(), v.as_bytes())?;
        store.set(k.as_bytes(), format!("{}_new", v).as_bytes())?;
    }

    let duration = time::Instant::now().duration_since(begin);
    let speed = (TOTAL_KEYS * 2) as f32 / duration.as_secs_f32();
    println!(
        "{} keys written in {} secs, {} keys/s",
        TOTAL_KEYS * 2,
        duration.as_secs_f32(),
        speed
    );

    let stats = store.stats();
    println!("{:?}", stats);

    store.compact()?;

    let mut index = 100;
    store.for_each(&mut |k, v| {
        index += 1;

        println!(
            "key={}, value={}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );

        if index > 5 {
            Ok(false)
        } else {
            Ok(true)
        }
    })?;

    let v = store.get("hello_1".as_bytes())?.unwrap_or_default();
    println!("{}", String::from_utf8_lossy(&v));

    let stats = store.stats();
    println!("{:?}", stats);

    Ok(())
}
