#[macro_export]
macro_rules! to_utf8_str {
    ($value:expr) => {
        &String::from_utf8_lossy($value).to_string()
    };
}

pub use io::{BufReaderWithOffset, BufWriterWithOffset, ByteLineReader, FileWithBufWriter};
pub use misc::*;

mod io;
pub mod misc;
