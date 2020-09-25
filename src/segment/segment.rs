use crate::buffer::buffer::Buffer;
use crate::encoder::decoder::InplaceSpanDecoder;
use crate::proto::trace::Span;
use crate::segment::segment_iterator::SegmentIterator;
use std::cmp::Reverse;
use std::collections::btree_map::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::SystemTime;
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
        }
    }

    /// put_span add span to the in-memory buffer and index the spans based on
    /// the given indices.
    pub fn put_span(
        &mut self,
        hashed_trace_id: u64,
        span: &[u8],
        start_ts: u64,
        indices: HashSet<String>,
    ) {
        // Update the timestamp of the current segment.
        if self.min_start_ts > start_ts {
            self.min_start_ts = start_ts;
        }
        if self.max_start_ts < start_ts {
            self.max_start_ts = start_ts;
        }
        // write the span to the inmemory buffer.
        let offset = self.buffer.write_slice(span);
        // Update the trace offsets.
        if let Some(ptr) = self.trace_offsets.get_mut(&hashed_trace_id) {
            // update the start ts.
            if ptr.0 > start_ts {
                ptr.0 = start_ts;
            }
            // Update the offsets.
            ptr.1.push(offset as u32);
        } else {
            self.trace_offsets
                .insert(hashed_trace_id, (start_ts, vec![offset as u32]));
        }
        // Update the index.
        for index in indices {
            if let Some(traces) = self.index.get_mut(&index) {
                traces.insert(hashed_trace_id);
                continue;
            }
            let mut traces = HashSet::new();
            traces.insert(hashed_trace_id);
            self.index.insert(index, traces);
        }
    }

    pub fn contain_trace(&self, hashed_trace_id: &u64) -> bool {
        self.trace_offsets.contains_key(hashed_trace_id)
    }
    /// segment_size returns the current in-memory buffer size.
    /// This is used to cut the current segment.
    pub fn segment_size(&self) -> usize {
        self.buffer.size()
    }

    pub fn max_trace_start_ts(&self) -> u64 {
        self.max_start_ts
    }
    pub fn index(&self) -> &HashMap<String, HashSet<u64>> {
        &self.index
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
mod tests {
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

    fn gen_traces() -> Vec<Vec<Span>> {
        let mut start_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
        let mut traces = gen_traces();
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
