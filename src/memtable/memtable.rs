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
use crate::encoder::span::encode_span;
use crate::memtable::iterator::MemtableIterator;
use crate::memtable::types::SpanPointer;
use crate::proto::trace::Span;
use skiplist::ordered_skiplist::OrderedSkipList;

/// MAX_MEMTABLE_SPAN_ENTRIES is an estimate that memtable can hold ablest 50k spans.
const MAX_MEMTABLE_SPAN_ENTRIES: usize = 50_000;
/// Memtable is the in-memory representation of incoming spans. Once we hit the
/// Threshold. We'll flush the span to the disk. Later, it's used for querying.
pub struct MemTable {
    /// container of encoded spans.
    spans: Buffer,
    /// sorted_span_pointer contains sorted span pointer. Which is
    /// user to find the span in the spans vector.
    sorted_span_pointer: OrderedSkipList<SpanPointer>,
    /// span_freelist contains free buffer. That can be reused. Ideally, we fill this
    /// free list while flushing the existing memory to the disk. This freelist can be
    /// use when we encode on the next run.
    span_freelist: Buffer,
}

impl MemTable {
    /// new returns the memtable struct.
    pub fn new() -> MemTable {
        MemTable {
            spans: Buffer::with_size(64 << 20),
            sorted_span_pointer: OrderedSkipList::with_capacity(MAX_MEMTABLE_SPAN_ENTRIES),
            span_freelist: Buffer::with_size(3_000),
        }
    }

    /// put_span puts the span into the memtable.
    pub fn put_span(&mut self, span: Span) {
        self.span_freelist.clear();
        let indices = encode_span(&span, &mut self.span_freelist);
        // TODO: check whether can it be done without freelist later. Whether worth to do the
        //  optimization.
        let offset = self.spans.write_slice(self.span_freelist.bytes_ref());
        let ptr = SpanPointer {
            trace_id: span.trace_id,
            start_ts: span.start_time_unix_nano,
            index: offset,
            indices,
        };
        self.sorted_span_pointer.insert(ptr);
    }

    /// span_size returns all the span size.
    pub fn span_size(&self) -> usize {
        self.spans.size()
    }

    /// iter returns memtable iterator. It's used for iterate over traces in the
    /// memtable.
    pub fn iter(&mut self) -> MemtableIterator {
        MemtableIterator {
            ordered_spans: &self.sorted_span_pointer,
            buffer: &self.spans,
            next_trace_idx: 0,
        }
    }

    /// clear clears all the state and hold the allocated memory. which can be used to
    /// clear the state.
    pub fn clear(&mut self) {
        self.spans.clear();
        self.span_freelist.clear();
        self.sorted_span_pointer.clear();
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::proto::common::{AnyValue, AnyValue_oneof_value, KeyValue};
    use crate::proto::trace::{Span_Event, Span_Link};
    use protobuf::{Message, RepeatedField, SingularPtrField};
    use rand::Rng;
    use std::convert::TryInto;
    pub fn gen_span() -> Span {
        let mut span = Span::default();
        span.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.parent_span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let mut event = Span_Event::default();
        let mut kv = KeyValue::default();
        kv.key = String::from("sup magic man");
        let mut val = AnyValue::default();
        val.value = Some(AnyValue_oneof_value::string_value(String::from(
            "let's make it right",
        )));
        kv.value = SingularPtrField::from(Some(val));
        let mut attributes = vec![kv.clone()];
        // Let's add more event.
        attributes.push(kv.clone());
        attributes.push(kv.clone());
        attributes.push(kv.clone());
        attributes.push(kv.clone());
        span.attributes = RepeatedField::from(attributes.clone());
        event.attributes = RepeatedField::from(attributes.clone());
        span.events = RepeatedField::from(vec![event.clone()]);
        span.events.push(event.clone());
        span.events.push(event.clone());
        span.events.push(event.clone());
        span.events.push(event.clone());
        span.events.push(event.clone());
        let mut link = Span_Link::default();
        link.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        link.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        link.trace_state = String::from("supl loadded state");
        link.attributes = RepeatedField::from(attributes);
        let mut links = vec![link.clone()];
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        span.links = RepeatedField::from(links);
        span
    }
    #[test]
    fn test_iterator() {
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

        // Let's insert trace 2
        trace_id[0] = 2;
        let mut span3 = gen_span();
        span3.start_time_unix_nano = 3;
        span3.trace_id = trace_id.clone().to_vec();
        table.put_span(span3.clone());
        let mut span4 = gen_span();
        span4.start_time_unix_nano = 4;
        span4.trace_id = trace_id.clone().to_vec();
        table.put_span(span4.clone());

        let mut itr = table.iter();
        let (span_trace_id, spans, _) = itr.next().unwrap();
        trace_id[0] = 1;
        assert_eq!(&span_trace_id[..], &trace_id);
        assert_eq!(spans.len(), 2);
        let mut buffer = Buffer::with_size(64 << 20);
        encode_span(&span1, &mut buffer);
        assert_eq!(spans[0], buffer.bytes_ref());
        let mut buffer = Buffer::with_size(64 << 20);
        encode_span(&span2, &mut buffer);
        assert_eq!(spans[1], buffer.bytes_ref());

        let (span_trace_id, spans, _) = itr.next().unwrap();
        trace_id[0] = 2;
        assert_eq!(&span_trace_id[..], &trace_id);
        assert_eq!(spans.len(), 2);
        let mut buffer = Buffer::with_size(64 << 20);
        encode_span(&span3, &mut buffer);
        assert_eq!(spans[0], buffer.bytes_ref());
        buffer.clear();
        encode_span(&span4, &mut buffer);
        assert_eq!(spans[1], buffer.bytes_ref());
    }
}
