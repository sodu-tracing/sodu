use crate::buffer::buffer::Buffer;
use std::collections::btree_map::BTreeMap;
use std::collections::HashSet;

/// TableBuilder is used to build file format table. It puts all the spans of the trace id close
/// to each other and flushes the built indices at the end of the file.
pub struct TableBuilder {
    /// buffer holds all the memory of file format table.
    buffer: Buffer,
    /// index_store store all the indices and there respective offsets in an sorted order.
    /// So, it's easy to find the index using simple binary search.
    index_store: BTreeMap<String, Vec<usize>>,
}
impl TableBuilder {
    /// from_buffer borrow the given buffer and build the table builder.
    pub fn from_buffer(mut buffer: Buffer) -> TableBuilder {
        buffer.clear();
        TableBuilder {
            buffer: buffer,
            index_store: Default::default(),
        }
    }

    /// add_trace writes the given trace to the buffer.
    pub fn add_trace(&mut self, trace_id: Vec<u8>, spans: Vec<&[u8]>, indices: HashSet<&String>) {
        let offset = self.buffer.size();
        self.buffer.write_raw_slice(&trace_id);
        for span in spans {
            self.buffer.write_slice(span);
        }
        for index in indices {
            if let Some(posting_list) = self.index_store.get_mut(index) {
                posting_list.push(offset);
                continue;
            }
            let mut posting_list = Vec::new();
            posting_list.push(offset);
            self.index_store.insert(index.clone(), posting_list);
        }
    }

    /// finish writes the inmemory index also to the file and returns the buffer
    /// which has file formatted span and indices.
    pub fn finish(mut self) -> Buffer {
        let mut posting_list_buffer = Buffer::with_size(400);
        let mut offsets = Vec::with_capacity(2 * self.index_store.len());
        for (index, posting_list) in self.index_store {
            let offset = self.buffer.write_slice(index.as_bytes());
            posting_list_buffer.clear();
            for trace_offset in posting_list {
                posting_list_buffer.write_raw_slice(&trace_offset.to_be_bytes());
            }
            self.buffer.write_slice(posting_list_buffer.bytes_ref());
            offsets.push(offset);
        }
        // Convert all the index offsets to buffer.
        posting_list_buffer.clear();
        for offset in offsets {
            posting_list_buffer.write_raw_slice(&offset.to_be_bytes());
        }
        let offset = self.buffer.write_slice(posting_list_buffer.bytes_ref());
        self.buffer.write_raw_slice(&offset.to_be_bytes());
        self.buffer
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::memtable::tests::gen_span;
    use crate::memtable::memtable::MemTable;

    fn gen_table() -> MemTable {
        let mut table = MemTable::new();
        let mut trace_id: [u8; 16] = [0; 16];
        trace_id[0] = 1;
        // Let's insert trace 1;
        let mut span1 = gen_span();
        span1.trace_id = trace_id.clone().to_vec();
        span1.start_time_unix_nano = 1;
        table.put_span(span1.clone());
        let mut span2 = gen_span();
        span2.trace_id = trace_id.clone().to_vec();
        span2.start_time_unix_nano = 2;
        table.put_span(span2.clone());
        table
    }
    #[test]
    fn test_table_builder() {
        let mut table = gen_table();
        let mut itr = table.iter();
        let (trace_id, spans, indices) = itr.next().unwrap();
        assert_eq!(indices.len(), 1);

        // Let's build the table for file format.
        let mut builder = TableBuilder::from_buffer(Buffer::with_size(64 << 20));
        builder.add_trace(trace_id, spans, indices);
        let mut buffer = builder.finish();
    }
}
