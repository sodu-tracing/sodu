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
use unsigned_varint::{decode, encode};

/// Buffer encloses Vec<u8> and gives utility functions which allows to
/// write slice of bytes and helps in encoding. For eg, it contains helper
/// function to write size of bytes before writing the slice bytes to buffer.
pub struct Buffer {
    // inner is the underlying the storage vector.
    inner: Vec<u8>, // position determine the current position of cursor. Basically, offset to
    // write the upcoming bytes.
    position: usize,
    var_int_buffer: [u8; 5],
}

// TODO: handle regrowing the slice. Not doing now. We should be able to handle such stuff.
impl Buffer {
    /// with size reserves the given size and return the buffer.
    pub fn with_size(size: usize) -> Buffer {
        let mut inner = Vec::with_capacity(size);
        unsafe {
            inner.set_len(size);
        }
        Buffer {
            inner: inner,
            position: 0,
            var_int_buffer: [0; 5],
        }
    }

    /// write_slice write the slice length in the beginning and write the slice as well.
    pub fn write_slice(&mut self, buf: &[u8]) -> usize {
        let offset = self.position;
        // Write the len of the buffer first.
        self.write_size(buf.len() as u32);
        if self.inner.len() - self.position < buf.len() {
            self.inner.reserve(1);
            unsafe {
                self.inner.set_len(self.inner.capacity());
            }
            return self.write_slice(buf);
        }
        self.write_raw_slice(buf);
        offset
    }

    /// write_raw_slice write the given slice to the underlying buffer.
    pub fn write_raw_slice(&mut self, buf: &[u8]) -> usize {
        if self.inner.len() - self.position < buf.len() {
            self.inner.reserve(1);
            unsafe {
                self.inner.set_len(self.inner.capacity());
            }
            return self.write_raw_slice(buf);
        }
        // Instead of extend use copy from slice. extend iterates and pushes. Which
        // leads to poor performance.
        self.inner[self.position..self.position + buf.len()].copy_from_slice(buf);
        // Increment the position to the new position.

        self.position = self.position + buf.len();
        self.position - buf.len()
    }

    /// write_size writes the given size as varint32 to the buffer.
    pub fn write_size(&mut self, size: u32) {
        if self.inner.len() - self.position < 5 {
            self.inner.reserve(1);
            unsafe {
                self.inner.set_len(self.inner.capacity());
            }
            return self.write_size(size);
        }
        // TODO: Instead of having separate buf, we need a way directly write var int
        // to the inner itself.
        let buf = encode::u32(size, &mut self.var_int_buffer);
        self.inner[self.position..self.position + buf.len()].copy_from_slice(buf);
        self.position = self.position + buf.len();
    }

    /// write byte writes byte to the buffer.
    pub fn write_byte(&mut self, b: u8) {
        self.inner[self.position] = b;
        self.position = self.position + 1;
    }

    /// returns slice at the given offset.
    pub fn slice_at(&self, offset: usize) -> &[u8] {
        if (offset + 5 as usize) > self.inner.len() {
            panic!("invalid offset at slice_at");
        }
        let (size, buf) = decode::u32(&self.inner[offset..offset + 5]).unwrap();
        // advance the offset
        let offset = offset + (5 - buf.len());
        // Check whether we have required buf in the space.
        if (offset + size as usize) > self.inner.len() {
            panic!("invalid size at slice_at");
        }
        return &self.inner[offset..offset + (size as usize)];
    }

    /// bytes_ref returns the entire
    pub fn bytes_ref(&self) -> &[u8] {
        &self.inner[..self.position]
    }

    /// size returns the size of the buffer.
    pub fn size(&self) -> usize {
        self.position
    }

    /// Clear reset the position to the initial state.
    pub fn clear(&mut self) {
        self.position = 0;
    }

    pub fn bytes(mut self) -> Vec<u8> {
        unsafe {
            self.inner.set_len(self.position);
        }
        self.inner
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn get_buffer(size: usize) -> Buffer {
        let mut buffer = Buffer::with_size(10);
        buffer.position = size;
        buffer
    }
    #[test]
    fn test_buffer() {
        // Testing with write_slice
        let mut buffer = Buffer::with_size(1024);
        let offset = buffer.write_slice(&[1, 2, 3, 5, 12]);
        assert_eq!(buffer.bytes_ref(), &[5, 1, 2, 3, 5, 12]);
        assert_eq!(buffer.slice_at(offset), &[1, 2, 3, 5, 12]);
        // Let's test raw slice write.
        let mut buffer = Buffer::with_size(1024);
        buffer.write_raw_slice(&[1, 2, 3, 5, 12]);
        assert_eq!(buffer.bytes_ref(), &[1, 2, 3, 5, 12]);
    }
}
