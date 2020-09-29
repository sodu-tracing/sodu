use crate::buffer::buffer::Buffer;

pub struct WalBufferedIterator<'a> {
    buffer: &'a Buffer,
}
