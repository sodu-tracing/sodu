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
use crate::encoder::decoder::decode_span;
use crate::ingester::segment_ingester::SegmentIngester;
use crate::options::options::Options;
use crate::proto::service::WalOffsets;
use crate::segment::segment_file::SegmentFile;
use crate::utils::utils::{extract_indices_from_span, get_file_ids, read_files_in_dir};
use crate::wal::wal::EncodedRequest;
use crate::wal::wal_iterator::WalIterator;
use anyhow::Context;
use log::{debug, info};
use std::collections::HashMap;
use std::fs::{remove_file, File};
use std::time::SystemTime;

/// RecoveryManager is used to repair crashed sodu.
pub struct RecoveryManager {
    /// opt holds the options of sodu.
    opt: Options,
}

impl RecoveryManager {
    /// new returns the RecoveryManager.
    pub fn new(opt: Options) -> RecoveryManager {
        RecoveryManager { opt }
    }

    /// repair repairs the sodu if it's crashed and replays log and flushes the segments.
    pub fn repair(&self) {
        // repair the segment files first. Because io_uring file flush is not sequential, so
        // we need to delete some flushed files as well.
        self.repair_segment_files();
        self.replay_wal();
    }

    fn replay_wal(&self) {
        debug!("replaying wal");
        // now get the wal offset from the last segment file.
        let mut segment_file_ids = self.get_segment_file_ids();
        segment_file_ids.sort_by(|a, b| b.cmp(&a));
        let mut wal_id = u64::MIN;
        let mut wal_offset = u64::MIN;
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
            let offset_metadata = segment_file.get_wal_offset();
            wal_id = offset_metadata.0;
            wal_offset = offset_metadata.1;
            delayed_wal_offsets = offset_metadata.2;
        }
        let start_time = SystemTime::now();
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
        info!(
            "time taken to generate delayed wal offset to replay wal {:?}",
            start_time.elapsed().unwrap().as_millis()
        );

        // Now we have from which wal and wal offset that needs to replayed. Also, we
        // got offset that needs to be skipped because, those entries are already persisted
        // in the previous segment files.
        let wal_itr = WalIterator::new(
            wal_id,
            wal_offset,
            delayed_wal_offsets,
            self.opt.wal_path.clone(),
        );
        if let None = wal_itr {
            return;
        }
        let wal_itr = wal_itr.unwrap();
        let mut ingester = SegmentIngester::new(self.opt.shard_path.clone());
        let mut ingested = false;
        for (encoded_buf, wal_id, offset) in wal_itr {
            ingested = true;
            // TODO: may be we do want to skip the first iteration because,
            // that is request is already persisted.
            let span = decode_span(&encoded_buf);
            let indices = extract_indices_from_span(&span);
            let req = EncodedRequest {
                indices: indices,
                start_ts: span.start_time_unix_nano,
                wal_offset: offset,
                encoded_span: &encoded_buf[..],
            };
            ingester.push_span(wal_id, req);
        }
        // Nothing ingested to flush. Just return here.
        if !ingested {
            return;
        }
        // flush all segments to the disk.
        ingester.flush_segment_if_necessary(true);
        ingester.reclaim_submitted_builder_buffer();
    }

    /// repair_segment_files remove all the invalid segment files. This is a dangerous function.
    /// Considering our segment flush method, We can call this function confidently. But, if
    /// the user delete some file in the segment. Then this system will crap out. So, it's
    /// upto the user to use it responsibly without doing any stupid chaos testing.
    fn repair_segment_files(&self) {
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
        segment_file_ids.sort();
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
                segment_file_ids[idx]
            ))
            .unwrap();
        }
    }

    fn get_segment_file_ids(&self) -> Vec<u64> {
        let segment_file_paths = read_files_in_dir(&self.opt.shard_path, "segment").unwrap();
        get_file_ids(&segment_file_paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::wal::tests::{generate_span, get_tmp_option};
    use crate::wal::wal::Wal;
    use std::fs::{create_dir_all, remove_file};
    #[test]
    fn test_segment_repair() {
        let opt = get_tmp_option();
        create_dir_all(&opt.wal_path).unwrap();
        let mut ingester = SegmentIngester::new(opt.shard_path.clone());
        let mut wal = Wal::new(opt.clone()).unwrap();
        let wal_id = wal.current_wal_id();
        let span = generate_span(1);
        let req = wal.write_spans(span);
        ingester.push_span(wal_id, req);
        let span = generate_span(2);
        let req = wal.write_spans(span);
        ingester.push_span(wal_id, req);
        ingester.flush_segment_if_necessary(true);
        ingester.reclaim_submitted_builder_buffer();
        // we need to see one segment file on the disk now.
        let segment_files_path = read_files_in_dir(&opt.shard_path, "segment").unwrap();
        assert_eq!(segment_files_path.len(), 1);
        let segment_file_ids = get_file_ids(&segment_files_path);
        assert_eq!(segment_file_ids[0], 1);
        // Let's ingest few more entries.
        let span = generate_span(3);
        let req = wal.write_spans(span);
        ingester.push_span(wal_id, req);
        let span = generate_span(4);
        let req = wal.write_spans(span);
        ingester.push_span(wal_id, req);
        ingester.flush_segment_if_necessary(true);
        ingester.reclaim_submitted_builder_buffer();

        let span = generate_span(5);
        let req = wal.write_spans(span);
        ingester.push_span(wal_id, req);
        let span = generate_span(6);
        let req = wal.write_spans(span);
        ingester.push_span(wal_id, req);
        ingester.flush_segment_if_necessary(true);
        ingester.reclaim_submitted_builder_buffer();
        wal.submit_buffer_to_iou();
        wal.wait_for_submitted_wal_span();
        // now we should have 3 segment files.
        let segment_files_path = read_files_in_dir(&opt.shard_path, "segment").unwrap();
        assert_eq!(segment_files_path.len(), 3);

        // Let's remove segment file.
        remove_file(&segment_files_path[1]).unwrap();
        // Since we removed the 2nd file. recovery manager
        // should delete the 3rd file as well. So, that
        // it can replay from the begining.
        let recovery_manager = RecoveryManager::new(opt.clone());
        recovery_manager.repair_segment_files();
        let segment_files_path = read_files_in_dir(&opt.shard_path, "segment").unwrap();
        assert_eq!(segment_files_path.len(), 1);
        let segment_file_ids = get_file_ids(&segment_files_path);
        assert_eq!(segment_file_ids[0], 1);
        recovery_manager.replay_wal();
        // Now we should be able to new segment file.
        let segment_files_path = read_files_in_dir(&opt.shard_path, "segment").unwrap();
        assert_eq!(segment_files_path.len(), 2);
    }
}
