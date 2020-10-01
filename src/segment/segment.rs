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
use crate::segment::segment_iterator::SegmentIterator;
use crate::wal::wal::EncodedRequest;
use std::collections::{HashMap, HashSet};
use std::u64;

/// Segment holds the inmemory representation of incoming traces.
pub struct Segment {
    /// buffer hold all the incoming spans.
    buffer: Buffer,
    /// index is the relationship between index and traces.
    index: HashMap<String, HashSet<u64>>,
    /// trace_offsets gives the mapping between trace and buffer offsets.
    trace_offsets: HashMap<u64, (u64, Vec<u32>)>,
    /// min_start_ts gives the minimum start_ts of this segment.
    min_start_ts: u64,
    /// max_start_ts gives the maximum start_ts of this segment.
    max_start_ts: u64,
    /// num_of_spans gives the number of span in the segment.
    num_of_spans: u64,
    /// max_wal_id is the maximum wal id of this segment.  Any thing higher than this
    /// is not persisted.
    max_wal_id: u64,
    /// max_wal_offset is the last offset of the the wal file that has been persisted in this
    /// segment.
    max_wal_offset: u64,
    /// delayed_wal_span_offsets contains delayed span wal offsets. This is used to skip this
    /// offset when we replaying the WAL.
    delayed_wal_span_offsets: HashMap<u64, Vec<u64>>,
}

impl Segment {
    /// new returns a new segment.
    pub fn new() -> Segment {
        Segment {
            buffer: Buffer::with_size(64 << 20),
            index: HashMap::default(),
            trace_offsets: HashMap::default(),
            min_start_ts: u64::MAX,
            max_start_ts: 0,
            num_of_spans: 0,
            max_wal_id: 0,
            max_wal_offset: 0,
            delayed_wal_span_offsets: HashMap::default(),
        }
    }

    pub fn from_buffer(buffer: Buffer) -> Segment {
        Segment {
            buffer: buffer,
            index: HashMap::default(),
            trace_offsets: HashMap::default(),
            min_start_ts: u64::MAX,
            max_start_ts: 0,
            num_of_spans: 0,
            max_wal_id: 0,
            max_wal_offset: 0,
            delayed_wal_span_offsets: HashMap::default(),
        }
    }
    fn insert_span(&mut self, hashed_trace_id: u64, req: EncodedRequest) {
        self.num_of_spans += 1;
        // Update the timestamp of the current segment.
        if self.min_start_ts > req.start_ts {
            self.min_start_ts = req.start_ts;
        }
        if self.max_start_ts < req.start_ts {
            self.max_start_ts = req.start_ts;
        }
        // write the span to the inmemory buffer.
        let offset = self.buffer.write_slice(req.encoded_span);
        // Update the trace offsets.
        if let Some(ptr) = self.trace_offsets.get_mut(&hashed_trace_id) {
            // update the start ts.
            if ptr.0 > req.start_ts {
                ptr.0 = req.start_ts;
            }
            // Update the offsets.
            ptr.1.push(offset as u32);
        } else {
            self.trace_offsets
                .insert(hashed_trace_id, (req.start_ts, vec![offset as u32]));
        }
        // Update the index.
        for index in req.indices {
            if let Some(traces) = self.index.get_mut(&index) {
                traces.insert(hashed_trace_id);
                continue;
            }
            let mut traces = HashSet::new();
            traces.insert(hashed_trace_id);
            self.index.insert(index, traces);
        }
    }

    /// put_span add span to the in-memory buffer and index the spans based on
    /// the given indices.
    pub fn put_span(&mut self, hashed_trace_id: u64, wal_id: u64, req: EncodedRequest) {
        self.max_wal_id = wal_id;
        self.max_wal_offset = req.wal_offset;
        self.insert_span(hashed_trace_id, req);
    }

    pub fn put_delayed_span(&mut self, hashed_trace_id: u64, wal_id: u64, req: EncodedRequest) {
        let wal_offset = req.wal_offset;
        self.insert_span(hashed_trace_id, req);
        if let Some(offsets) = self.delayed_wal_span_offsets.get_mut(&wal_id) {
            offsets.push(wal_offset);
            return;
        }
        self.delayed_wal_span_offsets
            .insert(wal_id, vec![wal_offset]);
    }

    pub fn contain_trace(&self, hashed_trace_id: &u64) -> bool {
        self.trace_offsets.contains_key(hashed_trace_id)
    }
    /// segment_size returns the current in-memory buffer size.
    /// This is used to cut the current segment.
    pub fn segment_size(&self) -> usize {
        self.buffer.size()
    }

    pub fn num_spans(&self) -> u64 {
        self.num_of_spans
    }

    pub fn max_wal_id(&self) -> u64 {
        self.max_wal_id
    }

    pub fn max_wal_offset(&self) -> u64 {
        self.max_wal_offset
    }

    pub fn max_trace_start_ts(&self) -> u64 {
        self.max_start_ts
    }
    pub fn index(&self) -> &HashMap<String, HashSet<u64>> {
        &self.index
    }
    pub fn delayed_wal_offsets(&self) -> &HashMap<u64, Vec<u64>> {
        &self.delayed_wal_span_offsets
    }

    pub fn iter(&self) -> SegmentIterator {
        let mut trace_offsets = Vec::with_capacity(self.trace_offsets.len());
        for (_, trace_offset) in &self.trace_offsets {
            trace_offsets.push(trace_offset.clone());
        }
        // sort the start_ts in descending order.
        trace_offsets.sort_by(|a, b| b.0.cmp(&a.0));
        SegmentIterator::new(trace_offsets, &self.buffer)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::encoder::span::encode_span;
    use crate::proto::common::{AnyValue, AnyValue_oneof_value, KeyValue};
    use crate::proto::trace::{Span_Event, Span_Link};
    use protobuf::{Message, RepeatedField, SingularPtrField};
    use rand::Rng;

    fn gen_trace(mut start_ts: u64) -> Vec<Span> {
        let mut span = Span::default();
        span.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.start_time_unix_nano = start_ts;
        span.end_time_unix_nano = start_ts + 1;
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
        let mut spans = Vec::new();
        spans.push(span.clone());
        start_ts += 2;
        span.start_time_unix_nano = start_ts;
        span.end_time_unix_nano = start_ts + 1;
        span.parent_span_id = span.span_id.clone();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        spans.push(span.clone());
        start_ts += 2;
        span.start_time_unix_nano = start_ts;
        span.end_time_unix_nano = start_ts + 1;
        span.parent_span_id = span.span_id.clone();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        spans.push(span.clone());
        start_ts += 2;
        span.start_time_unix_nano = start_ts;
        span.end_time_unix_nano = start_ts + 1;
        span.parent_span_id = span.span_id.clone();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        spans.push(span.clone());
        start_ts += 2;
        span.start_time_unix_nano = start_ts;
        span.end_time_unix_nano = start_ts + 1;
        span.parent_span_id = span.span_id.clone();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        spans.push(span.clone());
        spans
    }

    pub fn gen_traces(mut start_ts: u64) -> Vec<Vec<Span>> {
        let mut traces = Vec::new();
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        start_ts += 10;
        traces.push(gen_trace(start_ts));
        traces
    }
    #[test]
    fn test_memory_segment() {
        let start_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut traces = gen_traces(start_ts);
        let mut segment = Segment::new();
        let mut buffer = Buffer::with_size(3 << 20);
        for trace in traces.clone().into_iter() {
            for span in trace.into_iter() {
                let mut hasher = DefaultHasher::new();
                span.trace_id.hash(&mut hasher);
                let hashed_span_id = hasher.finish();
                buffer.clear();
                let indices = encode_span(&span, &mut buffer);
                segment.put_span(
                    hashed_span_id,
                    buffer.bytes_ref(),
                    span.start_time_unix_nano,
                    indices,
                );
            }
        }
        assert_eq!(
            segment.max_start_ts,
            traces[traces.len() - 1][traces[0].len() - 1].start_time_unix_nano
        );
        assert_eq!(segment.min_start_ts, traces[0][0].start_time_unix_nano);
        // Check the iterator whether traces are coming is same order.
        let mut iterator = segment.iter();
        traces.reverse();
        for trace in traces.into_iter() {
            let (start_ts, spans) = iterator.next().unwrap();
            assert_eq!(start_ts, trace[0].start_time_unix_nano);
            assert_eq!(spans.len(), trace.len());
        }
    }
}
