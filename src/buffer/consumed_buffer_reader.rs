// Copyright [2020] [Balaji Rajendran]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use anyhow::{anyhow, Result};
use unsigned_varint::decode;

/// ConsumedBufferReader is used to read bytes buffer. It contains helper function
/// to read slice and peak byte and stuff...
/// TODO: use generics to fix this. Otherwise lot of duplicate code and hard to manage.
#[derive(Default)]
pub struct ConsumedBufferReader {
    /// buf contains the bytes slice.
    buf: Vec<u8>,
    /// current_offset is the current offset of the buffer.
    current_offset: usize,
}

impl ConsumedBufferReader {
    /// new returns ConsumedBufferReader.
    pub fn new(buf: Vec<u8>) -> ConsumedBufferReader {
        ConsumedBufferReader {
            buf: buf,
            current_offset: 0,
        }
    }

    /// read_exact_length reads bytes of the given size and returns.
    pub fn read_exact_length(&mut self, sz: usize) -> Option<&[u8]> {
        if self.current_offset + sz > self.buf.len() {
            return None;
        }
        self.current_offset += sz;
        Some(&self.buf[self.current_offset - sz..self.current_offset])
    }

    /// peak_byte peaks a byte without consuming the offset.
    pub fn peek_byte(&self) -> Option<u8> {
        if self.current_offset + 1 > self.buf.len() {
            return None;
        }
        Some(self.buf[self.current_offset])
    }

    /// consume increase the current offset by the given size.
    pub fn consume(&mut self, sz: usize) -> Result<()> {
        if self.current_offset + sz > self.buf.len() {
            return Err(anyhow!("end of file"));
        }
        self.current_offset += sz;
        Ok(())
    }

    /// read_slice reads the byte slice where the current offset gives the size
    /// of upcoming byte slice.
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

    /// read_byte reads exactly one byte.
    pub fn read_byte(&mut self) -> Option<u8> {
        if self.current_offset + 1 > self.buf.len() {
            return None;
        }
        self.current_offset += 1;
        Some(self.buf[self.current_offset - 1])
    }

    /// resest reset's the offset.
    pub fn reset(&mut self) {
        self.current_offset = 0;
    }

    /// is_end tells whether we read the entire buffer or not.
    pub fn is_end(&self) -> bool {
        self.buf.len() - self.current_offset == 1
    }
}
