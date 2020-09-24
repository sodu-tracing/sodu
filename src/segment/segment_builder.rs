use crate::buffer::buffer::Buffer;
use crate::proto::types::ChuckMetadata;
use crate::proto::types::{SegmentMetadata, TraceIDOffset, TraceIds};
use protobuf::{Message, RepeatedField};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::mem;
use std::u64;
use unsigned_varint::encode;

pub struct SegmentBuilder {
    buffer: Buffer,
    trace_offsets: Vec<TraceIDOffset>,
    chunks: Vec<ChuckMetadata>,
    current_chunk_offset: usize,
    current_chunk_min_start_ts: u64,
    current_chunk_max_start_ts: u64,
    trace_size_buffer: [u8; 5],
}

impl SegmentBuilder {
    pub fn new() -> SegmentBuilder {
        SegmentBuilder {
            buffer: Buffer::with_size(64 << 20),
            trace_offsets: Vec::new(),
            chunks: Vec::new(),
            current_chunk_offset: 0,
            current_chunk_max_start_ts: u64::MIN,
            current_chunk_min_start_ts: u64::MAX,
            trace_size_buffer: [0; 5],
        }
    }

    pub fn add_trace(&mut self, start_ts: u64, trace: Vec<&[u8]>) {
        // Cut the chunk if the incoming trace can't fit into current
        // chunk.
        let trace_size = self.calculate_trace_size(&trace);
        if self.should_finish_chunk(trace_size) {
            self.finish_chunk();
        }
        // Update the min and max start_ts
        if self.current_chunk_min_start_ts > start_ts {
            self.current_chunk_min_start_ts = start_ts;
        }
        if self.current_chunk_max_start_ts < start_ts {
            self.current_chunk_max_start_ts = start_ts;
        }
        // Calculate the hash of the trace_id.
        let mut hasher = DefaultHasher::new();
        trace[0][..16].hash(&mut hasher);
        let trace_hash_id = hasher.finish();
        // Write the all the spans of this trace.
        let offset = self.buffer.size();
        // Write the trace size.
        let trace_size_buf = encode::u32(trace_size as u32, &mut self.trace_size_buffer);
        self.buffer.write_raw_slice(trace_size_buf);
        for (idx, span) in trace.into_iter().enumerate() {
            // Write the first span with trace_id. This will give us
            // little bit of compression.
            if idx == 0 {
                self.buffer.write_raw_slice(span);
                continue;
            }
            self.buffer.write_raw_slice(&span[16..]);
        }
        // Update the offset index for the current trace.
        let mut trace_offset = TraceIDOffset::default();
        trace_offset.set_offset(offset as u32);
        trace_offset.set_hashed_trace_id(trace_hash_id);
        self.trace_offsets.push(trace_offset);
    }

    fn should_finish_chunk(&mut self, size: usize) -> bool {
        let chunk_size = self.buffer.size() - self.current_chunk_offset;
        chunk_size + size >= 2 << 20
    }

    fn finish_chunk(&mut self) {
        // Create chunk metadata.
        let mut chunk_metadata = ChuckMetadata::default();
        chunk_metadata.set_max_start_ts(self.current_chunk_max_start_ts);
        chunk_metadata.set_min_start_ts(self.current_chunk_min_start_ts);
        chunk_metadata.set_offset(self.current_chunk_offset as u32);
        chunk_metadata.set_length((self.buffer.size() - self.current_chunk_offset) as u32);
        self.chunks.push(chunk_metadata);
        // Update the tmp metadata for the new chunk.
        self.current_chunk_offset = self.buffer.size();
        self.current_chunk_min_start_ts = u64::MAX;
        self.current_chunk_max_start_ts = u64::MIN;
    }

    fn calculate_trace_size(&self, trace: &Vec<&[u8]>) -> usize {
        let mut size: usize = 0;
        // iter collect would have done this job simple why to allocate here :(
        for (idx, span) in trace.iter().enumerate() {
            if idx == 0 {
                size += span.len();
                continue;
            }
            size += span.len() - 16;
        }
        size
    }

    pub fn finish_segment(&mut self, index: &HashMap<String, HashSet<u64>>) -> &Buffer {
        let mut segment_metadata = SegmentMetadata::default();
        // build segment index.
        let mut segment_index = HashMap::default();
        for (index_key, hashed_trace_ids) in index {
            let mut trace_ids = Vec::with_capacity(hashed_trace_ids.len());
            trace_ids.extend(hashed_trace_ids.iter());
            /// There should be way to embed rust types.
            let mut segment_trace_ids = TraceIds::default();
            segment_trace_ids.set_trace_ids(trace_ids);
            segment_index.insert(index_key.clone(), segment_trace_ids);
        }
        // update the metadata
        segment_metadata.set_index(segment_index);
        segment_metadata.set_min_start_ts(self.chunks[0].get_min_start_ts());
        segment_metadata.set_max_start_ts(self.chunks[self.chunks.len() - 1].get_max_start_ts());
        let chunks = mem::replace(&mut self.chunks, Vec::new());
        segment_metadata.set_chunks(RepeatedField::from(chunks));
        // write the metadata to the buffer.
        let metadata_buf = segment_metadata.write_to_bytes().unwrap();
        self.buffer.write_raw_slice(&metadata_buf[..]);
        // write the metadata size.
        let metadata_size = metadata_buf.len() as u32;
        self.buffer.write_raw_slice(&metadata_size.to_be_bytes());
        // replace the chunk back.
        let chunks = mem::replace(
            &mut segment_metadata.chunks,
            RepeatedField::from(Vec::new()),
        );
        self.chunks = chunks.to_vec();
        &self.buffer
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.trace_offsets.clear();
        self.chunks.clear();
        self.current_chunk_offset = 0;
        self.current_chunk_max_start_ts = u64::MIN;
        self.current_chunk_min_start_ts = u64::MAX;
    }
}
