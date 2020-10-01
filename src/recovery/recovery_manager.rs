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
use crate::options::options::Options;
use crate::proto::types::WalOffsets;
use crate::segment::segment_file::SegmentFile;
use crate::utils::utils::{get_file_ids, read_files_in_dir};
use anyhow::Context;
use std::collections::HashMap;
use std::fs::{remove_file, File};

pub struct RecoveryManager {
    opt: Options,
}

impl RecoveryManager {
    fn repair(&mut self) {
        // repair the segment files first. Because io_uring file flush is not sequential, so
        // we need to delete some flushed files as well.
        self.repair_segment_files();
        // now get the wal offset from the last segment file.
        let mut segment_file_ids = self.get_segment_file_ids();
        segment_file_ids.reverse();
        let mut wal_id = u64::MAX;
        let mut wal_offset = u64::MAX;
        let mut delayed_wal_offsets: HashMap<u64, WalOffsets> = HashMap::default();
        if segment_file_ids.len() != 0 {
            let file = File::open(
                &self
                    .opt
                    .shard_path
                    .join(format!("{:?}.segment", segment_file_ids[0])),
            )
            .context(format!(
                "unable to open segment file {:?}",
                segment_file_ids[0]
            ))
            .unwrap();
            let segment_file = SegmentFile::new(file)
                .context(format!(
                    "unable to open segment file {:?} to read metadata",
                    segment_file_ids[0]
                ))
                .unwrap();
            (wal_id, wal_offset, delayed_wal_offsets) = segment_file.get_wal_offset();
        }
        // Now it's time to find delayed offset that is greater than the current wal_id.
        for idx in 1..segment_file_ids.len() {
            // Filter all the delayed wal id which is greater than current wal id.
            let file = File::open(
                &self
                    .opt
                    .shard_path
                    .join(format!("{:?}.segment", segment_file_ids[idx])),
            )
            .context(format!(
                "unable to open segment file {:?}",
                segment_file_ids[idx]
            ))
            .unwrap();
            let segment_file = SegmentFile::new(file)
                .context(format!(
                    "unable to open segment file {:?} to read metadata",
                    segment_file_ids[idx]
                ))
                .unwrap();
            let (_, _, delayed_offsets) = segment_file.get_wal_offset();
            for (segment_wal_id, offsets) in delayed_offsets {
                // If it's lesser than our replay wal id then just skip. Because, we are
                // not going to replay it.
                if segment_wal_id < wal_id {
                    continue;
                }
                for offset in offsets.offsets {
                    // It's the same wal but the offset is lesser that repayable offset. So
                    // let's ignore this.
                    if segment_wal_id == wal_id && offset < wal_offset {
                        continue;
                    }
                    if let Some(wal_delayed_offset) = delayed_wal_offsets.get_mut(&segment_wal_id) {
                        wal_delayed_offset.offsets.push(offset);
                        continue;
                    }
                    let mut tmp_offsets = WalOffsets::default();
                    tmp_offsets.offsets = vec![offset];
                    delayed_wal_offsets.insert(segment_wal_id, tmp_offsets);
                }
            }
        }

        // Now we have from which wal and wal offset that needs to replayed. Also, we
        // got offset that needs to be skipped because, those entries are already persisted
        // in the previous segment files.
    }

    /// repair_segment_files remove all the invalid segment files. This is a dangerous function.
    /// considering our segment flush method. We can call this function confidently. But, if
    /// the user delete some file in the segment. Then this system will crap out. So, it's
    /// upto the user to use it responsibly without doing any stupid chaos testing.
    fn repair_segment_files(&mut self) {
        // First we need to check that all the segment files are in healthy
        // state.
        let segment_file_ids = self.get_segment_file_ids();
        // no segment file to repair.
        if segment_file_ids.len() == 0 {
            return;
        }

        // Let's verify files one by one
        for segment_file_id in segment_file_ids {
            let file = File::open(
                &self
                    .opt
                    .shard_path
                    .join(format!("{:?}.segment", segment_file_id)),
            )
            .context(format!("unable to open segment file {:?}", segment_file_id))
            .unwrap();
            // Delete the segment file it's is corrupted.
            if let Err(_) = SegmentFile::new(file) {
                remove_file(
                    &self
                        .opt
                        .shard_path
                        .join(format!("{:?}.segment", segment_file_id)),
                )
                .context(format!(
                    "unable to remove segment file {:?}",
                    segment_file_id
                ))
                .unwrap();
            }
        }

        let mut segment_file_ids = self.get_segment_file_ids();
        // We have successfully removed all the unnecessary files.
        // Now, remove all the disjoint files.
        let mut disjointed_idx: usize = usize::MAX;
        for (idx, segment_file_id) in segment_file_ids.iter().enumerate() {
            if idx + 1 >= segment_file_ids.len() {
                continue;
            }
            if segment_file_id + 1 != segment_file_ids[idx + 1] {
                // We found the disjoint point. Let' break it so that we can
                // delete files which greater than this.
                disjointed_idx = idx;
                break;
            }
        }

        // Looks like all segment files are fine. Let's just stop here.
        if disjointed_idx == usize::MAX {
            return;
        }
        // delete all the segment file which are higher than the healthy segment.
        for idx in disjointed_idx + 1..segment_file_ids.len() {
            remove_file(
                &self
                    .opt
                    .shard_path
                    .join(format!("{:?}.segment", segment_file_ids[idx])),
            )
            .context(format!(
                "unable to delete unhealthy segment file {:?}",
                segment_file_ids[idx + 1]
            ))
            .unwrap();
        }
    }

    fn get_segment_file_ids(&self) -> Vec<u64> {
        let segment_file_paths = read_files_in_dir(&self.opt.shard_path, "segment").unwrap();
        let mut segment_file_ids = get_file_ids(&segment_file_paths);
        segment_file_ids
    }
}
