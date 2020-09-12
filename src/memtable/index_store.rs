use std::collections::btree_map::BTreeMap;
use std::collections::HashSet;

#[derive(Default)]
pub struct IndexStore {
    inner: BTreeMap<String, HashSet<usize>>,
    index_key_size: usize,
    no_of_trace_indexed: usize,
}

impl IndexStore {
    pub fn add_index(&mut self, index: String, offset: usize){
        if let Some(posting_list) = self.inner.get_mut(&index){
            if !posting_list.insert(offset){
                return;
            }
            self.no_of_trace_indexed = self.no_of_trace_indexed + 1;
            return;
        }
        self.no_of_trace_indexed = self.no_of_trace_indexed + 1;
        self.index_key_size = self.index_key_size + index.len();
        let mut posting_list = HashSet::default();
        posting_list.insert(offset);
        self.inner.insert(index, posting_list);
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

    pub fn len(&self) -> usize{
        self.inner.len()
    }

    pub fn inner_ref(&self) -> &BTreeMap<String, HashSet<usize>>{
        &self.inner
    }
}