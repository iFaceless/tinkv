//! Some io helpers.

use std::fs;
use std::io::prelude::*;
use std::io::{self, BufReader, BufWriter, SeekFrom};
#[derive(Debug)]
pub struct BufReaderWithOffset<R: Read + Seek> {
    reader: BufReader<R>,
    offset: u64,
}

impl<R: Read + Seek> BufReaderWithOffset<R> {
    pub fn new(mut r: R) -> io::Result<Self> {
        r.seek(SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(r),
            offset: 0,
        })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl<R: Read + Seek> Read for BufReaderWithOffset<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.offset += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithOffset<R> {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        self.offset = self.reader.seek(offset)?;
        Ok(self.offset)
    }
}

#[derive(Debug)]
pub struct BufWriterWithOffset<W: Write + Seek> {
    writer: BufWriter<W>,
    offset: u64,
}

impl<W: Write + Seek> BufWriterWithOffset<W> {
    pub fn new(mut w: W) -> io::Result<Self> {
        w.seek(SeekFrom::Current(0))?;
        Ok(Self {
            writer: BufWriter::new(w),
            offset: 0,
        })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl<W: Write + Seek> Write for BufWriterWithOffset<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.offset += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithOffset<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.offset = self.writer.seek(pos)?;
        Ok(self.offset)
    }
}

/// A file wrapper wraps `File` and `BufWriterWithOffset<File>`.
/// We're using `BufWriterWithOffset` here for better writting performance.
/// Also, we need to make sure `file.sync_all()` can be called manually to
/// flush all pending writes to disk.
#[derive(Debug)]
pub struct FileWithBufWriter {
    inner: fs::File,
    bw: BufWriterWithOffset<fs::File>,
}

impl FileWithBufWriter {
    pub fn from(inner: fs::File) -> io::Result<FileWithBufWriter> {
        let bw = BufWriterWithOffset::new(inner.try_clone()?)?;

        Ok(FileWithBufWriter { inner, bw })
    }

    pub fn inner(&self) -> &fs::File {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut fs::File {
        &mut self.inner
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.inner.sync_all()
    }

    pub fn offset(&self) -> u64 {
        self.bw.offset()
    }
}

impl Drop for FileWithBufWriter {
    fn drop(&mut self) {
        // ignore sync errors.
        let _r = self.sync();
    }
}

impl Write for FileWithBufWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bw.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.bw.flush()
    }
}

impl Seek for FileWithBufWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.bw.seek(pos)
    }
}

const CARRIAGE_RETURN: &[u8] = b"\r";
const LINE_FEED: &[u8] = b"\n";

/// Read byte lines from an instance of `BufRead`.
///
/// Unlike `io::Lines`, it simply return byte lines instead
/// of string lines.
///
/// Ref: https://github.com/whitfin/bytelines/blob/master/src/lib.rs
#[derive(Debug)]
pub struct ByteLineReader<B> {
    reader: B,
    buf: Vec<u8>,
}

impl<B: BufRead> ByteLineReader<B> {
    /// Create a new `ByteLineReader` instance from given instance of `BufRead`
    pub fn new(reader: B) -> Self {
        Self {
            reader,
            buf: Vec::new(),
        }
    }

    pub fn next_line(&mut self) -> Option<io::Result<&[u8]>> {
        self.buf.clear();
        match self.reader.read_until(b'\n', &mut self.buf) {
            Ok(0) => None,
            Ok(mut n) => {
                if self.buf.ends_with(LINE_FEED) {
                    self.buf.pop();
                    n -= 1;
                    if self.buf.ends_with(CARRIAGE_RETURN) {
                        self.buf.pop();
                        n -= 1;
                    }
                }
                Some(Ok(&self.buf[..n]))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<B: BufRead> Read for ByteLineReader<B> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl<B: BufRead> BufRead for ByteLineReader<B> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt);
    }
}
