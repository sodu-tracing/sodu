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

    pub fn build_with(
        mut buffer: Buffer,
        mut index_store: BTreeMap<String, Vec<usize>>,
    ) -> TableBuilder {
        buffer.clear();
        index_store.clear();
        TableBuilder {
            buffer,
            index_store,
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
    pub fn finish(mut self) -> (Buffer, BTreeMap<String, Vec<usize>>) {
        let mut posting_list_buffer = Buffer::with_size(400);
        let mut start_offset: u32 = 0;
        for (index, posting_list) in &self.index_store {
            let offset = self.buffer.write_slice(index.as_bytes());
            if start_offset == 0 {
                // Store the offset of index store start. From there we can simply iterate to the
                // end to build the index store.
                start_offset = offset as u32;
            }
            posting_list_buffer.clear();
            for trace_offset in posting_list {
                let trace_offset = *trace_offset as u32;
                posting_list_buffer.write_raw_slice(&trace_offset.to_be_bytes());
            }
            self.buffer.write_slice(posting_list_buffer.bytes_ref());
        }
        self.buffer.write_raw_slice(&start_offset.to_be_bytes());
        (self.buffer, self.index_store)
    }
}
#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::memtable::memtable::tests::gen_span;
    use crate::memtable::memtable::MemTable;
    use crate::table::table::Table;
    use crate::utils::utils::create_index_key;
    use std::io::Write;
    use tempfile::tempfile;

    pub fn gen_table() -> MemTable {
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
        let (mut buffer, _) = builder.finish();
        let mut tf = tempfile().unwrap();
        tf.write_all(buffer.bytes_ref()).unwrap();
        tf.flush();
        let table = Table::from_file(tf);
        assert_eq!(table.indices.len(), 1);
        assert_eq!(
            table.indices[0].0,
            create_index_key(
                &"sup magic man".to_string(),
                "let's make it right".to_string()
            )
        );
        assert_eq!(table.indices[0].1, vec![0; 1]);
    }
}
