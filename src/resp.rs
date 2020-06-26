//! Implementation of serializer & deserializer of REdis Serialization Protocol (RESP).
//! Ref: https://redis.io/topics/protocol
use crate::error::{Result, TinkvError};
use crate::util::ByteLineReader;
use log::trace;
use std::io::{BufRead, Cursor, Write};

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

/// For writes
const WRITE_SIMPLE_STR_PREFIX: &[u8] = b"+";
const WRITE_ERROR_PREFIX: &[u8] = b"-";
const WRITE_INTEGER_PREFIX: &[u8] = b":";
const WRITE_BULK_STR_PREFIX: &[u8] = b"$";
const WRITE_ARRAY_PREFIX: &[u8] = b"*";
const WRITE_CRLF_SUFFIX: &[u8] = b"\r\n";

/// Generic RESP error.
pub(crate) struct Error<'s> {
    name: &'s str,
    msg: &'s str,
}

impl<'s> Error<'s> {
    pub fn new(name: &'s str, msg: &'s str) -> Self {
        Self { name, msg }
    }

    #[allow(dead_code)]
    pub fn as_error_value(&self) -> Value {
        Value::Error {
            name: self.name.to_owned(),
            msg: self.msg.to_owned(),
        }
    }
}

/// RESP value types. In RESP, different parts
/// of the protocol are always terminated with `\r\n`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
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
    #[allow(dead_code)]
    pub fn new_integer(value: i64) -> Self {
        Self::Integer(value)
    }

    #[allow(dead_code)]
    pub fn new_simple_string(value: &str) -> Self {
        Self::SimpleString(value.to_owned())
    }

    #[allow(dead_code)]
    pub fn new_null_bulk_string() -> Self {
        Self::NullBulkString
    }

    #[allow(dead_code)]
    pub fn new_bulk_string(value: Vec<u8>) -> Self {
        Self::BulkString(value)
    }

    #[allow(dead_code)]
    pub fn new_error(name: &str, msg: &str) -> Self {
        Self::Error {
            name: name.to_owned(),
            msg: msg.to_owned(),
        }
    }

    #[allow(dead_code)]
    pub fn new_array(elements: Vec<Value>) -> Self {
        Self::Array(elements)
    }

    #[allow(dead_code)]
    pub fn new_null_array() -> Self {
        Self::NullArray
    }

    #[allow(dead_code)]
    pub fn is_simple_string(&self) -> bool {
        self.as_simple_string().is_some()
    }

    pub fn as_simple_string(&self) -> Option<&str> {
        match self {
            Value::SimpleString(s) => Some(&s),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_error(&self) -> bool {
        self.as_error().is_some()
    }

    pub fn as_error(&self) -> Option<Error> {
        match self {
            Value::Error { name, msg } => Some(Error::new(name, msg)),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_integer(&self) -> bool {
        self.as_integer().is_some()
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_bulk_string(&self) -> bool {
        self.as_bulk_string().is_some()
    }

    pub fn as_bulk_string(&self) -> Option<&[u8]> {
        match self {
            Value::BulkString(v) => Some(v),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_array(&self) -> bool {
        self.as_array().is_some()
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(v) => Some(v),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        self.is_null_array() || self.is_null_bulk_string()
    }

    #[allow(dead_code)]
    pub fn is_null_bulk_string(&self) -> bool {
        self.as_null_bulk_string().is_some()
    }

    pub fn as_null_bulk_string(&self) -> Option<()> {
        match self {
            Value::NullBulkString => Some(()),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_null_array(&self) -> bool {
        self.as_null_array().is_some()
    }

    pub fn as_null_array(&self) -> Option<()> {
        match self {
            Value::NullArray => Some(()),
            _ => None,
        }
    }

    fn simple_string_from_slice(value: &[u8]) -> Value {
        let s = String::from_utf8_lossy(value);
        Value::SimpleString(s.into())
    }

    fn error_from_slice(value: &[u8]) -> Value {
        let s = String::from_utf8_lossy(value).to_string();
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
        let s = String::from_utf8_lossy(value).to_string();
        let v = s.parse::<i64>()?;
        Ok(Value::Integer(v))
    }

    fn bulk_string_from_slice<B: BufRead>(length: i64, reader: &mut B) -> Result<Value> {
        if length == -1 {
            return Ok(Value::NullBulkString);
        }

        let mut buf = vec![0u8; length as usize + 2]; // extra 2 bytes for `\r\n`
        reader.read_exact(&mut buf)?;

        let err = Err(TinkvError::Protocol(
            "protocol error, length of bulk string is not long enough".to_string(),
        ));

        if buf.last().unwrap() == &LF {
            buf.pop();
            if buf.last().unwrap() == &CR {
                buf.pop();
            } else {
                return err;
            }

            Ok(Value::BulkString(buf))
        } else {
            err
        }
    }
}

/// Deserialize RESP values from byte slice and return a value iterator.
/// Ref: https://github.com/rust-lang/rust/issues/51282
#[allow(dead_code)]
pub(crate) fn deserialize_from_slice(value: &[u8]) -> impl Iterator<Item = Result<Value>> + '_ {
    deserialize_from_reader(Cursor::new(value))
}

/// Deserialize RESP values from a stream reader and return a value iterator.
pub(crate) fn deserialize_from_reader<B: BufRead>(
    reader: B,
) -> impl Iterator<Item = Result<Value>> {
    Deserializer::from_reader(reader).into_iter()
}

#[derive(Debug)]
pub(crate) struct Deserializer<B> {
    inner: ByteLineReader<B>,
    buf: Vec<u8>,
}

impl<B> Deserializer<B>
where
    B: BufRead,
{
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
                if line.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(self.next_value(&line)?))
                }
            }
        }
    }

    fn next_line(&mut self) -> Result<Option<Vec<u8>>> {
        match self.inner.next_line() {
            None => Ok(None),
            Some(Err(e)) => Err(e.into()),
            // TODO: optimize later, avoid copying
            Some(Ok(v)) => Ok(Some(v.to_vec())),
        }
    }

    fn next_value(&mut self, value_line: &[u8]) -> Result<Value> {
        trace!("receive value line {}", repr!(value_line));
        let remaining = &value_line[1..];
        match value_line[0] {
            SIMPLE_STR_PREFIX => Ok(Value::simple_string_from_slice(remaining)),
            ERROR_PREFIX => Ok(Value::error_from_slice(remaining)),
            INTEGER_PREFIX => Value::integer_from_slice(remaining),
            BULK_STR_PREFIX => {
                Value::bulk_string_from_slice(parse_length(remaining)?, &mut self.inner)
            }
            ARRAY_PREFIX => {
                let len = parse_length(remaining)?;

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
                // TODO: fallback to extract from plain text
                Err(TinkvError::Protocol(format!(
                    "invalid data type prefix: {}",
                    repr!(remaining)
                )))
            }
        }
    }
}

fn parse_length(value: &[u8]) -> Result<i64> {
    let s = String::from_utf8_lossy(value).to_string();
    s.parse().map_err(|_| {
        TinkvError::Protocol(format!("protocol error, cannot parse length from {}", s))
    })
}

#[derive(Debug)]
pub(crate) struct ValueIter<B>
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

#[allow(dead_code)]
pub(crate) fn serialize_to_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut c = Cursor::new(&mut buf);
    serialize_to_writer(&mut c, value)?;
    c.flush()?;
    Ok(buf)
}

#[allow(dead_code)]
pub(crate) fn serialize_to_writer<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: Write,
{
    Serializer::new(writer).serialize(value)
}

#[derive(Debug)]
pub(crate) struct Serializer<W> {
    writer: W,
}

impl<W> Serializer<W>
where
    W: Write,
{
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn serialize(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::SimpleString(s) => self.serialize_simple_string(s.as_ref()),
            Value::Integer(i) => self.serialize_integer(i.to_owned()),
            Value::Error { name, msg } => self.serialize_error(&name, &msg),
            Value::BulkString(s) => self.serialize_bulk_string(s.as_ref()),
            Value::Array(v) => self.serialize_array(v.as_ref()),
            Value::NullArray => self.serialize_null_array(),
            Value::NullBulkString => self.serialize_null_bulk_string(),
        }
    }

    pub fn serialize_simple_string(&mut self, value: &str) -> Result<()> {
        self.writer.write_all(WRITE_SIMPLE_STR_PREFIX)?;
        self.writer.write_all(value.as_bytes())?;
        self.end()
    }

    pub fn serialize_error(&mut self, name: &str, msg: &str) -> Result<()> {
        self.writer.write_all(WRITE_ERROR_PREFIX)?;
        if name == "" {
            self.writer.write_all(b"ERR")?;
        } else {
            self.writer.write_all(name.as_bytes())?;
        }
        self.writer.write_all(b" ")?;
        self.writer.write_all(msg.as_bytes())?;
        self.end()
    }

    pub fn serialize_integer(&mut self, value: i64) -> Result<()> {
        self.writer.write_all(WRITE_INTEGER_PREFIX)?;
        self.writer.write_all(value.to_string().as_bytes())?;
        self.end()
    }

    pub fn serialize_bulk_string(&mut self, value: &[u8]) -> Result<()> {
        self.writer.write_all(WRITE_BULK_STR_PREFIX)?;
        self.writer.write_all(value.len().to_string().as_bytes())?;
        self.writer.write_all(WRITE_CRLF_SUFFIX)?;
        self.writer.write_all(value)?;
        self.end()
    }

    pub fn serialize_array(&mut self, value: &Vec<Value>) -> Result<()> {
        self.writer.write_all(WRITE_ARRAY_PREFIX)?;
        self.writer.write_all(value.len().to_string().as_bytes())?;
        self.writer.write_all(WRITE_CRLF_SUFFIX)?;
        for elem in value.iter() {
            self.serialize(elem)?;
        }
        Ok(())
    }

    pub fn serialize_null_bulk_string(&mut self) -> Result<()> {
        self.writer.write_all(WRITE_BULK_STR_PREFIX)?;
        self.writer.write_all(b"-1")?;
        self.end()
    }

    pub fn serialize_null_array(&mut self) -> Result<()> {
        self.writer.write_all(WRITE_ARRAY_PREFIX)?;
        self.writer.write_all(b"-1")?;
        self.end()
    }

    fn end(&mut self) -> Result<()> {
        self.writer.write_all(WRITE_CRLF_SUFFIX)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse_value(value: &str) -> Result<Option<Value>> {
        let reader = Cursor::new(value.as_bytes());
        let mut de = Deserializer::from_reader(reader);
        let v = de.next()?;
        Ok(v)
    }

    #[test]
    fn test_parse_length() {
        let v = parse_length("123".as_bytes());
        assert!(v.is_ok());
        assert_eq!(v.unwrap(), 123);

        let v = parse_length("123abc".as_bytes());
        assert!(v.is_err());
    }

    #[test]
    fn test_simple_string() {
        let r = parse_value("+OK\r\n");
        assert!(r.is_ok());
        let v = r.unwrap().unwrap();
        assert!(v.is_simple_string());
        assert_eq!(v.as_simple_string().unwrap(), "OK");
    }

    #[test]
    fn test_error() {
        let r = parse_value("-CUSTOMERR error message\r\n");
        assert!(r.is_ok());

        let v = r.unwrap().unwrap();
        assert!(v.is_error());

        let e = v.as_error().unwrap();
        assert_eq!(e.name, "CUSTOMERR");
        assert_eq!(e.msg, "error message");
    }

    #[test]
    fn test_integer() {
        let r = parse_value(":1234567\r\n");
        assert!(r.is_ok());

        let v = r.unwrap().unwrap();
        assert!(v.is_integer());

        let r = v.as_integer().unwrap();
        assert_eq!(r, 1234567);
    }

    #[test]
    fn test_bulk_string() {
        let r = parse_value("$5\r\nhello\r\n");
        assert!(r.is_ok());

        let v = r.unwrap().unwrap();
        assert!(v.is_bulk_string());

        let r = v.as_bulk_string().unwrap();
        assert_eq!(r, "hello".as_bytes());

        // null string
        let r = parse_value("$-1\r\n");
        assert!(r.is_ok());

        let v = r.unwrap().unwrap();
        assert!(v.is_null_bulk_string());
        assert!(v.is_null());
    }

    #[test]
    fn test_invalid_bulk_string() {
        let r = parse_value("$5\r\nhell\r\n");
        assert!(r.is_err());

        let r = parse_value("$5\r\nhello\r");
        assert!(r.is_err());

        let r = parse_value("$5\r\nhello\n\r");
        assert!(r.is_err());

        let r = parse_value("$5\rhello\n\r");
        assert!(r.is_err());
    }

    #[test]
    fn test_array() {
        let r = parse_value("*1\r\n:1\r\n");
        assert!(r.is_ok());
        let v = r.unwrap().unwrap();
        assert!(v.is_array());
        let r = v.as_array().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.get(0).unwrap().as_integer().unwrap(), 1);

        let r = parse_value("*2\r\n:1\r\n$5\r\ntinkv\r\n");
        assert!(r.is_ok());
        let v = r.unwrap().unwrap();
        assert!(v.is_array());
        let r = v.as_array().unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r.get(0).unwrap().as_integer().unwrap(), 1);
        assert_eq!(
            r.get(1).unwrap().as_bulk_string().unwrap(),
            "tinkv".as_bytes()
        );

        let r = parse_value("*3\r\n:1\r\n$5\r\ntinkv\r\n+OK\r\n");
        assert!(r.is_ok());
        let v = r.unwrap().unwrap();
        assert!(v.is_array());
        let r = v.as_array().unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r.get(0).unwrap().as_integer().unwrap(), 1);
        assert_eq!(
            r.get(1).unwrap().as_bulk_string().unwrap(),
            "tinkv".as_bytes()
        );
        assert_eq!(r.get(2).unwrap().as_simple_string().unwrap(), "OK");

        let r = parse_value("*-1\r\n");
        assert!(r.is_ok());
        let v = r.unwrap().unwrap();
        assert!(v.is_null());
        assert!(v.is_null_array());
    }

    #[test]
    fn test_invalid_array() {
        let r = parse_value("*3\r\n:1\r\n");
        assert!(r.is_err());
        let r = parse_value("*3\r\n:1\n");
        assert!(r.is_err());
    }

    #[test]
    fn test_deserialize_from_slice() {
        let values: Vec<Result<Value>> =
            deserialize_from_slice(b"+OK\r\n:100\r\n$5\r\nhello\r\n").collect();
        assert_eq!(values.len(), 3);

        let mut it = values.into_iter();
        assert_eq!(
            it.next().unwrap().unwrap().as_simple_string().unwrap(),
            "OK"
        );
        assert_eq!(it.next().unwrap().unwrap().as_integer().unwrap(), 100);
        assert_eq!(
            it.next().unwrap().unwrap().as_bulk_string().unwrap(),
            "hello".as_bytes()
        );
    }

    #[test]
    fn test_serialize_simple_string() {
        let r = serialize_to_bytes(&Value::new_simple_string("hello, world"));
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), b"+hello, world\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let r = serialize_to_bytes(&Value::new_integer(100));
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), b":100\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let r = serialize_to_bytes(&Value::new_error("MYERR", "error occurs"));
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), b"-MYERR error occurs\r\n");
    }

    #[test]
    fn test_serialize_null_bulk_string() {
        let r = serialize_to_bytes(&Value::new_null_bulk_string());
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), b"$-1\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let r = serialize_to_bytes(&Value::new_bulk_string(b"hello".to_vec()));
        assert!(r.is_ok());
        assert_eq!(repr!(&r.unwrap()), "$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_null_array() {
        let r = serialize_to_bytes(&Value::new_null_array());
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), b"*-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let r = serialize_to_bytes(&Value::new_array(vec![
            Value::new_integer(1),
            Value::new_simple_string("hello"),
            Value::new_bulk_string(b"world".to_vec()),
        ]));
        assert!(r.is_ok());
        assert_eq!(repr!(&r.unwrap()), "*3\r\n:1\r\n+hello\r\n$5\r\nworld\r\n");
    }
}
