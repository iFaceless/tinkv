use tinkv::{self, TinkvStore};
use std;
fn main() -> tinkv::Result<()> {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    println!("hello, rust");
    Ok(())
}