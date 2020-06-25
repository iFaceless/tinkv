//! Implementation of serializer & deserializer of REdis Serialization Protocol (RESP).
//! Ref: https://redis.io/topics/protocol
use crate::error::{Result, TinkvError};
use crate::util::ByteLineReader;
use log::trace;
use std::io::prelude::*;
use std::io::{self, BufRead, BufReader, BufWriter, Cursor};

macro_rules! repr {
    ($bs:expr) => {
        format!("{}", String::from_utf8_lossy($bs))
    };
}

/// Value type depends on the first byte.
const SIMPLE_STR_PREFIX: u8 = b'+';
const ERROR_PREFIX: u8 = b'-';
const INTEGER_PREFIX: u8 = b':';
const BULK_STR_PREFIX: u8 = b'$';
const ARRAY_PREFIX: u8 = b'*';
const CR: u8 = b'\r';
const LF: u8 = b'\n';
const CRLF: &[u8] = b"\r\n";

/// RESP value types. In RESP, different parts
/// of the protocol are always terminated with `\r\n`.
#[derive(Debug, Clone)]
pub enum Value {
    /// Simple strings are used to transmit non binary safee strings
    /// with minimal overhead.
    SimpleString(String),
    /// Error replies are set when something wrong happens.
    Error { name: String, msg: String },
    /// Integer is in the range of a signed 64 bit integer.
    Integer(i64),
    /// Bulk strings are used in order to represent a single
    /// binary safe string up to 512 MB in length.
    BulkString(Vec<u8>),
    /// Signal non-existence of a value, length is set to -1.
    NullBulkString,
    /// Client send commands to the server using RESP arrays.
    /// Commands returning collections of elements to the client
    /// use RESP arrays.
    /// Arrays can be nested of multiple RESP arrays or any other
    /// above types.
    Array(Vec<Value>),
    /// To specify a NULL value, length of `NullArray` is -1.
    NullArray,
}

impl Default for Value {
    fn default() -> Self {
        Self::NullBulkString
    }
}

impl Value {
    fn simple_string_from_slice(value: &[u8]) -> Value {
        let s = String::from_utf8_lossy(value);
        Value::SimpleString(s.into())
    }

    fn error_from_slice(value: &[u8]) -> Value {
        let s = String::from_utf8_lossy(&value[1..]).to_string();
        let mut name = "ERR".to_owned();
        let msg;
        if let Some(idx) = s.find(char::is_whitespace) {
            name = s[..idx].into();
            msg = s[idx + 1..].into();
        } else {
            msg = s
        }
        Value::Error { name, msg }
    }

    fn integer_from_slice(value: &[u8]) -> Result<Value> {
        let s = String::from_utf8_lossy(&value[1..]).to_string();
        let v = s.parse::<i64>()?;
        Ok(Value::Integer(v))
    }

    fn bulk_string_from_slice<B: BufRead>(length: i64, reader: &mut B) -> Result<Value> {
        if length == -1 {
            return Ok(Value::NullBulkString);
        }

        let mut buf = vec![0u8; length as usize + 2]; // extra 2 bytes for `\r\n`
        reader.read_exact(&mut buf)?;

        if buf.last().unwrap() == &LF {
            buf.pop();
            if buf.last().unwrap() == &CR {
                buf.pop();
            }
            Ok(Value::BulkString(buf))
        } else {
            Err(TinkvError::Protocol(
                "protocol error, expect CRLF".to_string(),
            ))
        }
    }
}

#[derive(Debug)]
pub struct Deserializer<B>
where
    B: BufRead,
{
    inner: ByteLineReader<B>,
    buf: Vec<u8>,
}

impl<B> Deserializer<B>
where
    B: BufRead,
{
    // #[allow(dead_code)]
    // pub fn from_slice(v: &[u8]) -> Self {

    // }

    // #[allow(dead_code)]
    // pub fn from_str(v: &str) -> Self {

    // }

    #[allow(dead_code)]
    pub fn from_reader(inner: B) -> Self {
        Self {
            inner: ByteLineReader::new(inner),
            buf: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn into_iter(self) -> ValueIter<B> {
        ValueIter { de: self }
    }

    pub fn next(&mut self) -> Result<Option<Value>> {
        match self.next_line()? {
            None => Ok(None),
            Some(line) => {
                let v = self.next_value(&line)?;
                Ok(Some(v))
            }
        }
    }

    fn next_line(&mut self) -> Result<Option<Vec<u8>>> {
        match self.inner.next() {
            None => return Ok(None),
            Some(Err(e)) => Err(e.into()),
            // TODO: avoid copy
            Some(Ok(v)) => Ok(Some(v.to_vec())),
        }
    }

    fn next_value(&mut self, value_line: &[u8]) -> Result<Value> {
        trace!("receive value line {}", repr!(value_line));
        match value_line[0] {
            SIMPLE_STR_PREFIX => Ok(Value::simple_string_from_slice(&value_line[1..])),
            ERROR_PREFIX => Ok(Value::error_from_slice(&value_line[1..])),
            INTEGER_PREFIX => Value::integer_from_slice(&value_line[1..]),
            BULK_STR_PREFIX => {
                Value::bulk_string_from_slice(parse_length(&value_line[1..])?, &mut self.inner)
            }
            ARRAY_PREFIX => {
                let len = parse_length(&value_line[1..])?;
                if len == -1 {
                    return Ok(Value::NullArray);
                }

                let mut elements: Vec<Value> = Vec::new();
                for _ in 0..len {
                    let v = self.next()?.ok_or_else(|| {
                        TinkvError::Protocol(
                            "protocol error, not enough array elements".to_string(),
                        )
                    })?;
                    elements.push(v);
                }

                Ok(Value::Array(elements))
            }
            _ => {
                // plain text
                Err(TinkvError::Protocol(format!(
                    "invalid data type prefix: {}",
                    repr!(&value_line[..1])
                )))
            }
        }
    }
}

fn parse_length(value: &[u8]) -> Result<i64> {
    let len = String::from_utf8_lossy(value).to_string().parse()?;
    Ok(len)
}

#[derive(Debug)]
pub struct ValueIter<B>
where
    B: BufRead,
{
    de: Deserializer<B>,
}

impl<B> Iterator for ValueIter<B>
where
    B: BufRead,
{
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.de.next() {
            Ok(None) => None,
            Ok(Some(v)) => Some(Ok(v)),
            Err(e) => Some(Err(e)),
        }
    }
}
