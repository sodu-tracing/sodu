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
use crate::proto::types::{ChunkMetadata, WalOffsets};
use crate::proto::types::{SegmentMetadata, TraceIDOffset, TraceIds};
use crate::utils::utils::calculate_trace_size;
use protobuf::{Message, RepeatedField};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::mem;
use std::u64;
use unsigned_varint::encode;

/// SegmentBuilder is used to build segment files.
pub struct SegmentBuilder {
    /// buffer holds the memory of the segment.
    buffer: Buffer,
    /// trace_offsets cotains the offsets of all traces.
    trace_offsets: Vec<TraceIDOffset>,
    /// chucks contains metadata of chunks.
    chunks: Vec<ChunkMetadata>,
    /// current_chunk_offset is the offset of current chunk in the buffer.
    current_chunk_offset: usize,
    /// current_chunk_min_start_ts is the min ts of the current chunk.
    current_chunk_min_start_ts: u64,
    /// current_chunk_max_start_ts is the max ts of the current chunk.
    current_chunk_max_start_ts: u64,
    /// trace_size_buffer is a temporary buffer used for encoding trace buffer.
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
        let trace_size = calculate_trace_size(&trace);
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
        self.buffer.write_size(trace_size as u32);
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
        let mut chunk_metadata = ChunkMetadata::default();
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

    pub fn finish_segment(
        &mut self,
        index: &HashMap<String, HashSet<u64>>,
        max_wal_id: u64,
        max_wal_offset: u64,
        delayed_wal_offsets: &HashMap<u64, Vec<u64>>,
    ) -> &Buffer {
        // finish the current chunk.
        if self.current_chunk_offset != self.buffer.size() {
            self.finish_chunk();
        }
        let mut segment_metadata = SegmentMetadata::default();
        // build segment index.
        let mut segment_index = HashMap::default();
        for (index_key, hashed_trace_ids) in index {
            let mut trace_ids = Vec::with_capacity(hashed_trace_ids.len());
            trace_ids.extend(hashed_trace_ids.iter());
            // There should be way to embed rust types.
            let mut segment_trace_ids = TraceIds::default();
            segment_trace_ids.set_trace_ids(trace_ids);
            segment_index.insert(index_key.clone(), segment_trace_ids);
        }
        // update the metadata
        segment_metadata.set_index(segment_index);
        segment_metadata.set_min_start_ts(self.chunks[0].get_min_start_ts());
        segment_metadata.set_max_start_ts(self.chunks[self.chunks.len() - 1].get_max_start_ts());
        segment_metadata.set_max_wal_id(max_wal_id);
        segment_metadata.set_max_wal_offset(max_wal_offset);
        let mut delayed_span_wal_offsets = HashMap::default();
        for (wal_id, offsets) in delayed_wal_offsets {
            let mut wal_offsets = WalOffsets::default();
            wal_offsets.set_offsets(offsets.clone());
            delayed_span_wal_offsets.insert(wal_id.clone(), wal_offsets);
        }
        segment_metadata.set_delayed_span_wal_offsets(delayed_span_wal_offsets);
        self.trace_offsets
            .sort_by(|a, b| a.get_hashed_trace_id().cmp(&b.get_hashed_trace_id()));
        let trace_offsets = mem::replace(&mut self.trace_offsets, Vec::new());
        segment_metadata.set_sorted_trace_ids(RepeatedField::from(trace_offsets));
        let chunks = mem::replace(&mut self.chunks, Vec::new());
        segment_metadata.set_chunks(RepeatedField::from(chunks));
        // write the metadata to the buffer.
        let metadata_buf = segment_metadata.write_to_bytes().unwrap();
        let offset = self.buffer.write_raw_slice(&metadata_buf[..]);
        // write the metadata size.
        let metadata_offset = offset as u32;
        self.buffer.write_raw_slice(&metadata_offset.to_be_bytes());
        // replace the chunk back.
        let chunks = mem::replace(
            &mut segment_metadata.chunks,
            RepeatedField::from(Vec::new()),
        );
        let trace_offsets =
            mem::replace(&mut segment_metadata.sorted_trace_ids, RepeatedField::new());
        self.trace_offsets = trace_offsets.to_vec();
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
