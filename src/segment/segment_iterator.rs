use crate::buffer::buffer::Buffer;
use std::collections::HashMap;

/// SegmentIterator is used to iterate over inmemory segment.
pub struct SegmentIterator<'a> {
    /// trace_offsets is the offset of all the traces in time sorted order.
    trace_offsets: Vec<(u64, Vec<u32>)>,
    /// next_index is used to tell the next trace index in trace_offsets.
    next_index: usize,
    /// buffer contains the spans.
    buffer: &'a Buffer,
}

impl<'a> SegmentIterator<'a> {
    /// new return a SegmentIterator.
    pub fn new(offsets: Vec<(u64,Vec<u32>)>, buffer: &Buffer) -> SegmentIterator {
        SegmentIterator {
            trace_offsets: offsets,
            next_index: 0,
            buffer: buffer,
        }
    }
}

impl<'a> Iterator for SegmentIterator<'a> {
    type Item = (u64,Vec<&'a [u8]>);

    /// next gives the next trace in the iterator.
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.trace_offsets.len() {
            None
        }
        let (start_ts,span_offsets) = &self.trace_offsets[self.next_index];
        let mut spans = Vec::with_capacity(span_offsets.len());
        for offset in span_offsets {
            spans.push(self.buffer.slice_at(offset as usize));
        }
        self.next_index += 1;
        Some((*start_ts,spans))
    }
}
