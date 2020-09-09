use crate::buffer::buffer::Buffer;
use crate::memtable::types::SpanPointer;
use skiplist::OrderedSkipList;
use std::cmp::Ordering;

/// MemtableIterator is used to iterate all the traces in the table in
/// a sorted order. It gives all the traces in sorted order and spans in
/// traces are sorted by start time span. It is upto the client to order the
/// dependency between spans.
pub struct MemtableIterator<'a> {
    /// ordered_spans keeps the span ptr in an sorted order. SpanPointer is used to
    /// reference the span in the continuous buffer.
    pub ordered_spans: &'a OrderedSkipList<SpanPointer>,
    /// buffer holds all the span.
    pub buffer: &'a Buffer,
    /// next_trace_idx hold the index of the next trace in the ordered_spans skiplist.
    pub next_trace_idx: usize,
}

impl<'a> Iterator for MemtableIterator<'a> {
    type Item = (Vec<u8>, Vec<&'a [u8]>);
    /// next iterates and gives the next trace.
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_trace_idx >= self.ordered_spans.len() {
            return None;
        }
        // Now collect all spans of same traces.
        let mut trace_id = Vec::with_capacity(16);
        let mut spans = Vec::new();
        for i in self.next_trace_idx..self.ordered_spans.len() {
            let span_ptr = &self.ordered_spans[i];
            if trace_id.is_empty() {
                unsafe {
                    trace_id.set_len(16);
                }
                trace_id.copy_from_slice(&span_ptr.trace_id);
                spans.push(self.buffer.slice_at(span_ptr.index));
                self.next_trace_idx = self.next_trace_idx + 1;
                continue;
            }
            if trace_id.cmp(&span_ptr.trace_id) != Ordering::Equal {
                break;
            }
            spans.push(self.buffer.slice_at(span_ptr.index));
            self.next_trace_idx = self.next_trace_idx + 1;
        }
        Some((trace_id, spans))
    }
}
