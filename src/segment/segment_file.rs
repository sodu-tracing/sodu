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
use crate::proto::types::{SegmentMetadata, WalOffsets};
use anyhow::{anyhow, Context, Result};
use protobuf::parse_from_bytes;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct SegmentFile {
    file: File,
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

    pub fn get_wal_offset(&self) -> (u64, u64, HashMap<u64, WalOffsets>) {
        let wal_offset = self.metadata.max_wal_offset.unwrap();
        let wal_id = self.metadata.max_wal_id.unwrap();
        let delayed_offsets = self.metadata.delayed_span_wal_offsets.clone();
    }
}
