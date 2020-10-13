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
use crate::proto::types::ChunkMetadata;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct SegmentFileIterator {
    file: File,
    filtered_trace_ids: HashSet<u64>,
    chunks: Vec<ChunkMetadata>,
    current_chunk: Option<ChunkMetadata>,
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
            current_chunk: None,
        }
    }
}

impl Iterator for SegmentFileIterator {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(chunk) = self.chunks.pop() {
            let chunk_offset = chunk.offset.unwrap();
            self.file
                .seek(SeekFrom::Start(chunk_offset as u64))
                .unwrap();
            let mut buf = vec![0; chunk.length.unwrap() as usize];
            self.file.read_exact(&mut buf).unwrap();
        }
        None
    }
}
