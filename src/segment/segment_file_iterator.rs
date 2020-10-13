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
use crate::buffer::consumed_buffer_reader::ConsumedBufferReader;
use crate::encoder::decoder::InplaceSpanDecoder;
use crate::proto::service::InternalTrace;
use crate::proto::types::ChunkMetadata;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct SegmentFileIterator {
    file: File,
    filtered_trace_ids: HashSet<u64>,
    chunks: Vec<ChunkMetadata>,
    current_chunk_reader: Option<ConsumedBufferReader>,
}

impl SegmentFileIterator {
    pub fn new(
        file: File,
        filtered_trace_ids: HashSet<u64>,
        mut chunks: Vec<ChunkMetadata>,
    ) -> SegmentFileIterator {
        // First sort the chunks based on the offsets.
        chunks.sort_by(|a, b| a.offset.as_ref().unwrap().cmp(b.offset.as_ref().unwrap()));
        SegmentFileIterator {
            file: file,
            filtered_trace_ids: filtered_trace_ids,
            chunks: chunks,
            current_chunk_reader: None,
        }
    }
}

impl Iterator for SegmentFileIterator {
    type Item = InternalTrace;
    fn next(&mut self) -> Option<Self::Item> {
        // try to read from the current chunk reader.
        if let Some(reader) = &mut self.current_chunk_reader {
            if !reader.is_end() {
                let trace = reader.read_slice().unwrap().unwrap();
                let decoder = InplaceSpanDecoder(trace);
                // skip if the current trace if the trace id is not part of filtered
                // traces.
                if self.filtered_trace_ids.len() != 0
                    && !self.filtered_trace_ids.contains(&decoder.hashed_trace_id())
                {
                    return self.next();
                }
                let start_ts = decoder.start_ts();
                let mut internal_trace = InternalTrace::default();
                internal_trace.set_start_ts(start_ts);
                internal_trace.set_trace(trace.to_vec());
                return Some(internal_trace);
            }
        }
        // We read the previous chunk let's move to the next chunk.
        if let Some(chunk) = self.chunks.pop() {
            let chunk_offset = chunk.offset.unwrap();
            self.file
                .seek(SeekFrom::Start(chunk_offset as u64))
                .unwrap();
            let mut buf = vec![0; chunk.length.unwrap() as usize];
            self.file.read_exact(&mut buf).unwrap();
            let buffer = ConsumedBufferReader::new(buf);
            self.current_chunk_reader = Some(buffer);
            // let's read the next chunk.
            return self.next();
        }
        // we don't have any more chunk to read. So, just return None.
        None
    }
}
