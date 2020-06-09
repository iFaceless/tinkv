//! Define some helper functions.
use chrono;
use crc;

pub fn current_timestamp() -> u32 {
    chrono::Local::now().timestamp() as u32
}

pub fn checksum(data: &[u8]) -> u32 {
    crc::crc32::checksum_ieee(data)
}