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
    #[error("parse resp value failed")]
    ParseRespValue,
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
    #[error("{} {}", .name, .msg)]
    RespCommon { name: String, msg: String },
    #[error("wrong number of arguments for '{}' command", .0)]
    RespWrongNumOfArgs(String),
}

impl TinkvError {
    pub fn new_resp_common(name: &str, msg: &str) -> Self {
        Self::RespCommon {
            name: name.to_owned(),
            msg: msg.to_owned(),
        }
    }

    pub fn resp_wrong_num_of_args(name: &str) -> Self {
        Self::RespWrongNumOfArgs(name.to_owned())
    }
}
