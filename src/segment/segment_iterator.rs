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
    pub fn new(offsets: Vec<(u64, Vec<u32>)>, buffer: &Buffer) -> SegmentIterator {
        SegmentIterator {
            trace_offsets: offsets,
            next_index: 0,
            buffer: buffer,
        }
    }
}

impl<'a> Iterator for SegmentIterator<'a> {
    type Item = (u64, Vec<&'a [u8]>);

    /// next gives the next trace in the iterator.
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.trace_offsets.len() {
            return None;
        }
        let (start_ts, span_offsets) = &self.trace_offsets[self.next_index];
        let mut spans = Vec::with_capacity(span_offsets.len());
        for offset in span_offsets {
            spans.push(self.buffer.slice_at(*offset as usize));
        }
        self.next_index += 1;
        Some((*start_ts, spans))
    }
}
