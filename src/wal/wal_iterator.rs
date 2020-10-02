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
use crate::proto::types::WalOffsets;
use crate::utils::utils::{get_file_ids, read_files_in_dir};
use anyhow::Context;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::path::PathBuf;
use unsigned_varint::decode;

pub struct WalIterator {
    next_wal_ids: Vec<u64>,
    current_wal_reader: BufReader<File>,
    current_wal_id: u64,
    current_wal_offset: u64,
    offsets_to_be_skipped: HashMap<u64, WalOffsets>,
    wal_dir_path: PathBuf,
}

impl WalIterator {
    fn new(
        head_wal_id: u64,
        head_wal_offset: u64,
        offsets_to_be_skipped: HashMap<u64, WalOffsets>,
        wal_path: PathBuf,
    ) -> Option<WalIterator> {
        // Let's get all the wal files.
        let wal_files = read_files_in_dir(&wal_path, "wal")
            .context("unable to read files in wal directory")
            .unwrap();
        // If there is no wal files to replay. Just end
        // here.
        if wal_files.len() == 0 {
            return None;
        }
        let mut wal_ids = get_file_ids(&wal_files);
        // Remove all the wal id which are lesser than head wal id. Since those wal files
        // are persisted in the disk.
        let mut wal_ids: Vec<u64> = wal_ids
            .into_iter()
            .filter(|id| *id >= head_wal_id)
            .collect();
        wal_ids.sort();
        // take the first wal file.
        let wal_id = wal_ids.pop().unwrap();
        let mut wal_file = File::open(&wal_path.join(format!("{:?}", wal_id))).unwrap();
        // Seek to the correct offset.
        if head_wal_offset != 0 {
            wal_file.seek(SeekFrom::Start(head_wal_offset)).unwrap();
        }
        // Do we need to skip this offset since it's persisted on segment file.
        Some(WalIterator {
            next_wal_ids: wal_ids,
            current_wal_id: wal_id,
            current_wal_offset: head_wal_offset,
            offsets_to_be_skipped: offsets_to_be_skipped,
            current_wal_reader: BufReader::new(wal_file),
            wal_dir_path: wal_path,
        })
    }
}

impl Iterator for WalIterator {
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.current_wal_offset;
        let mut size_buf: [u8; 5] = [0; 5];
        if let Err(e) = self.current_wal_reader.read_exact(&mut size_buf) {
            if e.kind() != ErrorKind::UnexpectedEof {
                panic!(format!(
                    "unexpected error. Looks like wal file {:?} is corrupted",
                    self.current_wal_id
                ));
            }
            // Looks like we reached the end of the file. Let's advance the wal file.
            if let Some(next_wal_id) = self.next_wal_ids.pop() {
                let next_wal_file =
                    File::open(&self.wal_dir_path.join(format!("{:?}.wal", next_wal_id)))
                        .context(format!("unable to open wal file {:?}", next_wal_id))
                        .unwrap();
                self.current_wal_id = next_wal_id;
                self.current_wal_offset = 0;
                self.current_wal_reader = BufReader::new(next_wal_file);
                return self.next();
            }
            return None;
        }
        let (size, rem) = decode::u32(&size_buf).unwrap();
        // Go back to the remaining place.
        self.current_wal_reader.seek_relative(-(rem.len() as i64));
        let mut buf = vec![0; size as usize];
        self.current_wal_reader.read_exact(&mut buf);
        // advance the offset.
        self.current_wal_offset == (5 - rem.len() + size as usize) as u64;
        None
    }
}
