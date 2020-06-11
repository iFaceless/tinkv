//! Some io helpers.
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
