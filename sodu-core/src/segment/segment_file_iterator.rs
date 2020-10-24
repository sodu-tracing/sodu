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
use crate::proto::service::{ChunkMetadata, TimeRange};
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct SegmentFileIterator {
    pub file: File,
    pub filtered_trace_ids: HashSet<u64>,
    pub chunks: Vec<ChunkMetadata>,
    pub current_chunk_reader: Option<ConsumedBufferReader>,
    pub max_start_ts: u64,
    pub min_start_ts: u64,
}

impl Iterator for SegmentFileIterator {
    type Item = (u64, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        // try to read from the current chunk reader.
        if let Some(reader) = &mut self.current_chunk_reader {
            if !reader.is_end() {
                let trace = reader.read_slice().unwrap().unwrap();
                let decoder = InplaceSpanDecoder(trace);
                let trace_start_ts = decoder.start_ts();
                // skip this trace if the trace is not falling in iterator range.
                if (trace_start_ts > self.max_start_ts || trace_start_ts < self.min_start_ts)
                    && (self.max_start_ts != 0 && self.min_start_ts != 0)
                {
                    return self.next();
                }
                // skip if the current trace if the trace id is not part of filtered
                // traces.
                let hashed_trace_id = decoder.hashed_trace_id();
                if self.filtered_trace_ids.len() != 0
                    && !self.filtered_trace_ids.contains(&hashed_trace_id)
                {
                    return self.next();
                }
                return Some((trace_start_ts, trace.to_vec()));
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::buffer::buffer::Buffer;
    use crate::json_encoder::encoder::encode_trace;
    use crate::proto::service::QueryRequest;
    use crate::segment::segment::tests::gen_traces;
    use crate::segment::segment::Segment;
    use crate::segment::segment_builder::SegmentBuilder;
    use crate::segment::segment_file::SegmentFile;
    use crate::utils::utils::tests::get_encoded_req;
    use std::collections::HashMap;
    use std::fs::{create_dir_all, File};
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_segment_file_iterator() {
        // create segment file.
        let mut segment = Segment::new();
        let traces = gen_traces(100, 10000);
        let mut buffer = Buffer::with_size(3 << 20);
        for trace in &traces {
            for span in trace {
                let (encoded_req, hashed_trace_id) = get_encoded_req(span, &mut buffer);
                segment.put_span(hashed_trace_id, 0, encoded_req);
            }
        }
        // let's build the segment file.
        let mut builder = SegmentBuilder::new(0);
        for (start_ts, trace) in segment.iter() {
            builder.add_trace(start_ts, trace);
        }
        let segment_buffer = builder.finish_segment(segment.index(), 0, 0, &HashMap::default());

        // write the segment buffer to file.
        let tmp_dir = tempdir().unwrap();
        create_dir_all(tmp_dir.path()).unwrap();
        let segment_file_path = tmp_dir.path().join(format!("{}.segment", 1));
        let mut file = File::create(&segment_file_path).unwrap();
        file.write_all(segment_buffer.bytes_ref()).unwrap();
        drop(file);
        let file = File::open(&segment_file_path).unwrap();
        let segment_file = SegmentFile::new(file).unwrap();
        let req = QueryRequest::default();
        let mut segment_itr = segment.iter();
        for (start_ts, trace) in segment_file.get_iter_for_query(&req).unwrap() {
            let (in_memory_start_ts, in_memory_trace) = segment_itr.next().unwrap();
            let mut extended_trace = Vec::new();
            for (idx, span) in in_memory_trace.into_iter().enumerate() {
                if idx == 0 {
                    extended_trace.extend(span);
                    continue;
                }
                extended_trace.extend(&span[16..]);
            }
            assert_eq!(start_ts, in_memory_start_ts);
            assert_eq!(&trace[..], &extended_trace[..]);
        }
    }
}
