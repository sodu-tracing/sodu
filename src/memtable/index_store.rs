use std::collections::btree_map::BTreeMap;
use std::collections::HashSet;

#[derive(Default)]
pub struct IndexStore {
    inner: BTreeMap<String, HashSet<Vec<u8>>>,
    index_key_size: usize,
    no_of_trace_indexed: usize,
    current_trace_id: Vec<u8>,
}

impl IndexStore {
    pub fn add_index(&mut self, index: String){
        if let Some(posting_list) = self.inner.get_mut(&index){
            if !posting_list.insert(self.current_trace_id.clone()){
                return;
            }
            self.no_of_trace_indexed = self.no_of_trace_indexed + 1;
            return;
        }
        self.no_of_trace_indexed = self.no_of_trace_indexed + 1;
        self.index_key_size = self.index_key_size + index.len();
        let mut posting_list = HashSet::default();
        posting_list.insert(self.current_trace_id.clone());
        self.inner.insert(index, posting_list);
    }

    pub fn set_current_trace(&mut self, trace_id: Vec<u8>){
        self.current_trace_id = trace_id;
    }

    pub fn calculate_index_size(&self) -> usize{
        // calculate size for offset of key and value.
        let mut size = self.no_of_trace_indexed * 8;
        // Add size for all the trace_id.
        size = size + (16 * self.no_of_trace_indexed);
        // Add size of all the index key.
        size = size + self.index_key_size;
        size
    }
}