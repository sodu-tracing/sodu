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
use std::path::PathBuf;

pub struct WalIterator {
    filtered_ids: Vec<u64>,
    file: File,
}

impl WalIterator {
    fn new(
        head_wal_id: u64,
        head_wal_offset: u64,
        offsets_to_be_skipped: HashMap<u64, WalOffsets>,
        wal_path: PathBuf,
    ) {
        // Let's get all the wal files.
        let wal_files = read_files_in_dir(&wal_path, "wal")
            .context("unable to read files in wal directory")
            .unwrap();
        let mut wal_ids = get_file_ids(&wal_files);
        // Remove all the wal id which are lesser than head wal id. Since those wal files
        // are persisted in the disk.
        let mut wal_ids: Vec<u64> = wal_ids
            .into_iter()
            .filter(|id| *id >= head_wal_id)
            .collect();
        wal_ids.sort();
    }
}
