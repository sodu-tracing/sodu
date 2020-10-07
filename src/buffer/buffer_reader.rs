use anyhow::{anyhow, Result};
use unsigned_varint::decode;

#[derive(Default)]
pub struct BufferReader<'a> {
    buf: &'a [u8],
    current_offset: usize,
}

impl<'a> BufferReader<'a> {
    pub fn new(buf: &[u8]) -> BufferReader {
        BufferReader {
            buf: buf,
            current_offset: 0,
        }
    }

    pub fn read_exact_length(&mut self, sz: usize) -> Option<&[u8]> {
        if self.current_offset + sz > self.buf.len() {
            return None;
        }
        self.current_offset += sz;
        Some(&self.buf[self.current_offset - sz..self.current_offset])
    }

    pub fn peek_byte(&self) -> Option<u8> {
        if self.current_offset + 1 > self.buf.len() {
            return None;
        }
        Some(self.buf[self.current_offset + 1])
    }

    pub fn consume(&mut self, sz: usize) -> Result<()> {
        if self.current_offset + sz > self.buf.len() {
            return Err(anyhow!("end of file"));
        }
        self.current_offset += sz;
        Ok(())
    }

    pub fn read_slice(&mut self) -> Result<Option<&[u8]>> {
        let (sz, rem) = decode::u32(&self.buf[self.current_offset..]).unwrap();
        // Advance the size offset.
        self.current_offset += self.buf[self.current_offset..].len() - rem.len();
        let sz = sz as usize;
        if self.current_offset + sz > self.buf.len() {
            return Ok(None);
        }
        self.current_offset += sz;
        Ok(Some(
            &self.buf[self.current_offset - sz..self.current_offset],
        ))
    }

    pub fn skip_bytes(&mut self, sz: usize) {
        self.current_offset += sz;
    }

    pub fn read_byte(&mut self) -> Option<u8> {
        if self.current_offset + 1 > self.buf.len() {
            return None;
        }
        self.current_offset += 1;
        Some(self.buf[self.current_offset - 1])
    }

    pub fn reset(&mut self) {
        self.current_offset = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer::Buffer;

    #[test]
    fn test_buffer_reader() {
        let mut buffer = Buffer::with_size(2 << 20);
        buffer.write_byte(1);
        buffer.write_raw_slice(&vec![2; 16]);
        buffer.write_slice(&vec![1; 4]);
        let mut reader = BufferReader::new(buffer.bytes_ref());
        let id: Vec<u8> = vec![1; 1];
        assert_eq!(&id[..], reader.read_exact_length(1).unwrap());
        let id: Vec<u8> = vec![2; 16];
        assert_eq!(reader.read_exact_length(16).unwrap(), &id[..]);
        let id: Vec<u8> = vec![1; 4];
        assert_eq!(reader.read_slice().unwrap().unwrap(), &id[..]);
    }
}
