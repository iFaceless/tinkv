use anyhow;

pub type TinkvError = anyhow::Error;
pub type Result<T> = std::result::Result<T, TinkvError>;