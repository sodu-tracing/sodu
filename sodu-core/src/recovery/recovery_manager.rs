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
use crate::meta_store::sodu_meta_store::SoduMetaStore;
use crate::options::options::Options;
use crate::proto::service::WalOffsets;
use crate::segment::segment_file::SegmentFile;
use crate::utils::types::WalCheckPoint;
use crate::utils::utils::{extract_indices_from_span, get_file_ids, read_files_in_dir};
use crate::wal::wal::EncodedRequest;
use crate::wal::wal_iterator::WalIterator;
use anyhow::Context;
use log::{debug, info};
use std::collections::HashMap;
use std::fs::{remove_file, File};
use std::sync::Arc;
use std::time::SystemTime;

/// RecoveryManager is used to repair crashed sodu.
pub struct RecoveryManager {
    /// opt holds the options of sodu.
    opt: Options,
    meta_store: Arc<SoduMetaStore>,
}

impl RecoveryManager {
    /// new returns the RecoveryManager.
    pub fn new(opt: Options, meta_store: Arc<SoduMetaStore>) -> RecoveryManager {
        RecoveryManager { opt, meta_store }
    }

    /// repair repairs the sodu if it's crashed and replays log and flushes the segments.
    pub fn repair(&self) {
        // repair the segment files first. Because io_uring file flush is not sequential, so
        // we need to delete some flushed files as well.
        let wal_check_point = self.repair_segment_files();
        self.replay_wal(wal_check_point);
    }

    fn replay_wal(&self, checkpoint: WalCheckPoint) {
        debug!("replaying wal");
        // now get the wal offset from the last segment file.
        let mut segment_file_ids = self.get_segment_file_ids();
        segment_file_ids.sort_by(|a, b| b.cmp(&a));
        let mut iterated_segments = 0;
        let mut delayed_wal_offsets: HashMap<u64, WalOffsets> = HashMap::default();
        for segment_id in segment_file_ids {
            // Just go only 10 segments past to calculate the delayed offsets.
            // TODO: there should be a logic to calculate the checkpoint to calculate
            // till what point we need to check. But, I'm keeping a hard limit. It may
            // go wrong. Have to fix it later. Since, it's a telemtry data. So, it's fine.
            if iterated_segments == 10 {
                break;
            }
            let file = File::open(
                &self
                    .opt
                    .shard_path
                    .join(format!("{:?}.segment", segment_id)),
            )
            .context(format!("unable to open segment file {:?}", segment_id))
            .unwrap();
            let segment_file = SegmentFile::new(file)
                .context(format!(
                    "unable to open segment file {:?} to read metadata",
                    segment_id
                ))
                .unwrap();
            iterated_segments += 1;
            let delayed_offsets = segment_file.get_delayed_offsets();
            for (wal_id, wal_offsets) in delayed_offsets {
                // If the wal id is lesser than checkpoint. Then no need to capture the offsets.
                // since we are not going to replay those wals.
                if wal_id < checkpoint.wal_id {
                    continue;
                }
                for offset in wal_offsets.offsets.clone() {
                    // It's the same wal offset, which is lesser than the checkpoint offset. So,
                    // ignoring this offset.
                    if wal_id == checkpoint.wal_id && offset < checkpoint.wal_offset {
                        continue;
                    }
                    if let Some(captured_delayed_offsets) = delayed_wal_offsets.get_mut(&wal_id) {
                        captured_delayed_offsets.offsets.push(offset);
                        continue;
                    }
                    let mut captured_delayed_offsets = WalOffsets::default();
                    captured_delayed_offsets.offsets = vec![offset];
                    delayed_wal_offsets.insert(wal_id, captured_delayed_offsets);
                }
            }
        }
        // Now we have from which wal and wal offset that needs to replayed. Also, we
        // got offset that needs to be skipped because, those entries are already persisted
        // in the previous segment files.
        let wal_itr = WalIterator::new(
            checkpoint.wal_id,
            checkpoint.wal_offset,
            delayed_wal_offsets,
            self.opt.wal_path.clone(),
        );
        if let None = wal_itr {
            return;
        }
        let wal_itr = wal_itr.unwrap();
        let mut ingester =
            SegmentIngester::new(self.opt.shard_path.clone(), self.meta_store.clone());
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
    fn repair_segment_files(&self) -> WalCheckPoint {
        // First we need to check that all the segment files are in healthy
        // state.
        let mut segment_file_ids = self.get_segment_file_ids();
        // no segment file to repair.
        if segment_file_ids.len() == 0 {
            return WalCheckPoint::default();
        }

        if let Some(wal_check_point) = self.meta_store.get_wal_check_point() {
            // delete all the segment files which is greater than the given checkpoint.
            segment_file_ids.sort_by(|a, b| b.cmp(&a));
            for segment_id in segment_file_ids {
                if segment_id > wal_check_point.segment_id {
                    debug!("deleting segment id while repairing wal {:?}", segment_id);
                    remove_file(
                        &self
                            .opt
                            .shard_path
                            .join(format!("{:?}.segment", segment_id)),
                    )
                    .context(format!("unable to remove segment file {:?}", segment_id))
                    .unwrap();
                    continue;
                }
                // Since we iterating in an sorted order. So, it's safe to break the loop
                // here. Because, the upcoming segment id are lesser than the checkpoint.
                break;
            }
            return wal_check_point;
        }

        // delete all the segment files. Since no wal checkpoint is persisted. we can replay
        // the wal so it's fine to delete.
        for segment_file_id in segment_file_ids {
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
        WalCheckPoint::default()
    }

    fn get_segment_file_ids(&self) -> Vec<u64> {
        let segment_file_paths = read_files_in_dir(&self.opt.shard_path, "segment").unwrap();
        get_file_ids(&segment_file_paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::utils::init_all_utils;
    use crate::wal::wal::tests::{generate_span, get_tmp_option};
    use crate::wal::wal::Wal;
    use std::fs::{create_dir_all, remove_file};
    #[test]
    fn test_segment_repair() {
        init_all_utils();
        let opt = get_tmp_option();
        create_dir_all(&opt.wal_path).unwrap();
        let meta_store = Arc::new(SoduMetaStore::new(&opt));
        let mut ingester = SegmentIngester::new(opt.shard_path.clone(), meta_store.clone());
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
        remove_file(opt.shard_path.join(format!("{:?}.segment", 2))).unwrap();
        remove_file(opt.shard_path.join(format!("{:?}.segment", 3))).unwrap();
        // let's  update the wal checkpoint.
        let file = File::open(opt.shard_path.join(format!("{:?}.segment", 1))).unwrap();
        let segment_file = SegmentFile::new(file).unwrap();
        let (wal_id, wal_offset, _) = segment_file.get_wal_offset();
        let checkpoint = WalCheckPoint {
            wal_id: wal_id,
            wal_offset: wal_offset,
            segment_id: 1,
        };
        meta_store.save_wal_check_point(checkpoint.clone());
        let recovery_manager = RecoveryManager::new(opt.clone(), meta_store);
        recovery_manager.repair_segment_files();
        let segment_files_path = read_files_in_dir(&opt.shard_path, "segment").unwrap();
        assert_eq!(segment_files_path.len(), 1);
        let segment_file_ids = get_file_ids(&segment_files_path);
        assert_eq!(segment_file_ids[0], 1);
        recovery_manager.replay_wal(checkpoint);
        // Now we should be able to new segment file.
        let segment_files_path = read_files_in_dir(&opt.shard_path, "segment").unwrap();
        assert_eq!(segment_files_path.len(), 2);
    }
}
