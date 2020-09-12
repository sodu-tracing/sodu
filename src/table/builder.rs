use crate::buffer::buffer::Buffer;
use std::collections::HashSet;
use std::collections::btree_map::BTreeMap;

pub struct TableBuilder {
    buffer:  Buffer,
    index_store: BTreeMap<String, Vec<usize>>
}
impl TableBuilder{
    pub fn from_buffer(mut buffer:Buffer) -> TableBuilder{
        buffer.clear();
        TableBuilder{
            buffer: buffer,
            index_store: Default::default(),
        }
    }

    pub fn add_trace(&mut self, trace_id: Vec<u8>, spans: &[u8], indices: HashSet<String>){
        let offset = self.buffer.size();
        self.buffer.write_raw_slice(&trace_id);
        self.buffer.write_slice(spans);
        for index in indices{
            if let Some(posting_list) = self.index_store.get_mut(&index){
                posting_list.push(offset);
                continue
            }
            let mut posting_list = Vec::new();
            posting_list.push(offset);
            self.index_store.insert(index, posting_list);
        }
    }


    pub fn finish(mut self) -> Buffer {
        let mut posting_list_buffer = Buffer::with_size(400);
        let mut offsets = Vec::with_capacity(2*self.index_store.len());
        for (index, posting_list) in self.index_store{
            let offset = self.buffer.write_slice(index.as_bytes());
            posting_list_buffer.clear();
            for trace_offset in posting_list{
                posting_list_buffer.write_raw_slice(&trace_offset.to_be_bytes());
            }
            self.buffer.write_slice(posting_list_buffer.bytes_ref());
            offsets.push(offset);
        }
        // Convert all the index offsets to buffer.
        posting_list_buffer.clear();
        for offset in offsets{
            posting_list_buffer.write_raw_slice(&offset.to_be_bytes());
        }
        let offset = self.buffer.write_slice(posting_list_buffer.bytes_ref());
        self.buffer.write_raw_slice(&offset.to_be_bytes());
        self.buffer
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn test_table_builder(){

    }
}