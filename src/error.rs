use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;
use thiserror::Error;

/// The result of any operation.
pub type Result<T> = ::std::result::Result<T, TinkvError>;

/// The kind of error that could be produced during tinkv operation.
#[derive(Error, Debug)]
pub enum TinkvError {
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Glob(#[from] glob::GlobError),
    #[error(transparent)]
    Pattern(#[from] glob::PatternError),
    #[error(transparent)]
    Codec(#[from] Box<bincode::ErrorKind>),
    /// Custom error definitions.
    #[error("crc check failed, data entry (key='{}', file_id={}, offset={}) was corrupted", String::from_utf8_lossy(.key), .file_id, .offset)]
    DataEntryCorrupted {
        file_id: u64,
        key: Vec<u8>,
        offset: u64,
    },
    #[error("key '{}' not found", String::from_utf8_lossy(.0))]
    KeyNotFound(Vec<u8>),
    #[error("file '{}' is not writeable", .0.display())]
    FileNotWriteable(PathBuf),
    #[error("key is too large")]
    KeyIsTooLarge,
    #[error("value is too large")]
    ValueIsTooLarge,
    #[error("{}", .0)]
    Custom(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("{}", .0)]
    Protocol(String),
}
