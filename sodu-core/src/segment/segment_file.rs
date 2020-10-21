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
use crate::proto::service::QueryRequest;
use crate::proto::service::{ChunkMetadata, SegmentMetadata, TimeRange, WalOffsets};
use crate::segment::segment_file_iterator::SegmentFileIterator;
use crate::utils::utils::{create_index_key, is_over_lapping_range};
use anyhow::{anyhow, Context, Result};
use protobuf::parse_from_bytes;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// SegmentFile is the persisted segment file, which contains traces.
pub struct SegmentFile {
    /// file is the segment file's fd.
    file: File,
    /// metadata is the segment file metadata.
    metadata: SegmentMetadata,
}

impl SegmentFile {
    /// new returns the SegmentFile.
    pub fn new(mut file: File) -> Result<SegmentFile> {
        let file_length = file
            .metadata()
            .context("unable to read file metadata")
            .unwrap()
            .len() as usize;
        // If the file size is zero then the segment is not yet flushed.
        if file_length == 0 {
            return Err(anyhow!("file size is zero"));
        }
        // let's read the last 4 bytes to read the
        file.seek(SeekFrom::Start((file_length - 4) as u64))
            .unwrap();
        let mut buf: [u8; 4] = [0; 4];
        file.read_exact(&mut buf).unwrap();
        let metadata_offset = u32::from_be_bytes(buf);
        // let's decode the metadata.
        file.seek(SeekFrom::Start(metadata_offset as u64)).unwrap();
        let mut metadata_buf = vec![0; file_length - (metadata_offset as usize) - 4];
        file.read_exact(&mut metadata_buf[..])?;
        let metadata = parse_from_bytes(&metadata_buf).unwrap();
        Ok(SegmentFile { file, metadata })
    }

    /// get_iter_for_query returns segment iterator for the given query request.
    pub fn get_iter_for_query(mut self, req: &QueryRequest) -> Option<SegmentFileIterator> {
        // Filter trace_ids for tags.
        let mut filtered_trace_ids = HashSet::new();
        for (key, val) in &req.tags {
            let index_key = create_index_key(key, val);
            if let Some(trace_ids) = self.metadata.index.remove(&index_key) {
                filtered_trace_ids.extend(trace_ids.trace_ids);
            }
        }
        if req.tags.len() != 0 && filtered_trace_ids.len() == 0 {
            return None;
        }
        // Filter all the chunks that falls in the specified time range.
        let chunks = self.metadata.chunks.to_vec();
        let chunks: Vec<ChunkMetadata> = chunks
            .into_iter()
            .filter(|chunk| is_over_lapping_range(req.get_time_range(), chunk.get_time_range()))
            .collect();
        // return the iterator.
        Some(SegmentFileIterator {
            file: self.file,
            filtered_trace_ids: filtered_trace_ids,
            chunks: chunks,
            current_chunk_reader: None,
            max_start_ts: req.get_time_range().get_max_start_ts(),
            min_start_ts: req.get_time_range().get_min_start_ts(),
        })
    }

    pub fn get_wal_offset(&self) -> (u64, u64, HashMap<u64, WalOffsets>) {
        let wal_offset = self.metadata.max_wal_offset.unwrap();
        let wal_id = self.metadata.max_wal_id.unwrap();
        println!("wal id {:?} wal offset {:?}", wal_id, wal_offset);
        let delayed_offsets = self.metadata.delayed_span_wal_offsets.clone();
        (wal_id, wal_offset, delayed_offsets)
    }

    /// get_delayed_offsets returns delayed offsets.
    pub fn get_delayed_offsets(&self) -> HashMap<u64, WalOffsets> {
        self.metadata.delayed_span_wal_offsets.clone()
    }

    /// get_time_range returns time range of the segment file.
    pub fn get_time_range(&self) -> &TimeRange {
        self.metadata.get_time_range()
    }
}
