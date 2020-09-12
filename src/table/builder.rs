use crate::buffer::buffer::Buffer;
pub struct TableBuilder {
    buffer:  Buffer,
}
impl TableBuilder{
    pub fn from_buffer(buffer:Buffer) -> TableBuilder{
        TableBuilder{
            buffer: buffer,
        }
    }

    pub fn reserve_size(&self, size: usize){

    }
    pub fn add_trace(&mut self, trace_id: Vec<u8>, spans: &[u8]){
        self.buffer.write_raw_slice(&trace_id);
        self.buffer.write_raw_slice(spans);
    }

    pub fn finish(self) -> Buffer{
        self.buffer
    }
}