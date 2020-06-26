//! Define some helper functions.
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos()
}

pub fn checksum(data: &[u8]) -> u32 {
    crc::crc32::checksum_ieee(data)
}

pub fn parse_file_id(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .split('.')
        .next()?
        .parse::<u64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_file_id() {
        let r = parse_file_id(Path::new("path/to/12345.tinkv.data"));
        assert_eq!(r, Some(12345 as u64));

        let r = parse_file_id(Path::new("path/to/.tinkv.data"));
        assert_eq!(r, None);

        let r = parse_file_id(Path::new("path/to"));
        assert_eq!(r, None);
    }
}
