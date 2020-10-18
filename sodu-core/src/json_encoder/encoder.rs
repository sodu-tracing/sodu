use crate::buffer::buffer::Buffer;
use std::convert::TryInto;
pub fn encode_trace(buf: &mut Buffer, trace: Vec<u8>) {
    buf.write_byte(b'{');
}
