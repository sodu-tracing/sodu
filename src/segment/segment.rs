use crate::buffer::buffer::Buffer;
use crate::encoder::decoder::InplaceSpanDecoder;
use crate::proto::trace::Span;
use crate::segment::segment_iterator::SegmentIterator;
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
        SegmentIterator::new(trace_offsets, &self.buffer)
    }
}
