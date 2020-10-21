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
use crate::buffer::buffer::Buffer;
use crate::encoder::decoder::InplaceSpanDecoder;
use crate::meta_store::sodu_meta_store::SoduMetaStore;
use crate::proto::service::{QueryRequest, TimeRange};
use crate::segment::segment::Segment;
use crate::segment::segment_builder::SegmentBuilder;
use crate::utils::utils::{get_file_ids, is_over_lapping_range, read_files_in_dir};
use crate::wal::wal::EncodedRequest;
use futures::io::IoSlice;
use iou::IoUring;
use log::debug;
use std::arc::Arc;
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::fs::File;
use std::mem;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};

/// SegmentIngester is used for ingesting incoming spans. It is responsible for handling the
/// life cycle of ingestion.
pub struct SegmentIngester {
    /// current_segment is current segment where incoming span goes.
    current_segment: Segment,
    /// buffered_segment contains filled segments. Once segment reached it's size threshold, then
    /// it's been pushed to buffered_segment. Then it's been lazily flushed to disk once it
    /// completes it's grace period.
    buffered_segment: VecDeque<Segment>,
    /// span_buffer is the tmp buffer for encoding span.
    span_buffer: Buffer,
    /// iou is the io_uring instance. This used to flush segments in an async fashion.
    iou: IoUring,
    /// next_segment_id is the next segment id.
    next_segment_id: u64,
    /// segments_path is the segment file directory.
    segments_path: PathBuf,
    /// submitted_builders holds the segment build which is been submitted to the io_uring. This
    /// is used to maintain the life time of the builder. So, that we don't drop the buffer before
    /// the segment making into a segment file.
    submitted_builders: Vec<SegmentBuilder>,
    /// submitted_iou_ids contains the set of io_uring submitted table builder segment's id. It helps
    /// to verify theirs id on completion.
    submitted_iou_ids: HashSet<u64>,
    /// builder_freelist contains iouring completed table builder. It can be reused for further
    /// building segment.
    builder_freelist: Vec<SegmentBuilder>,
    /// meta_store is used to store wal check point.
    meta_store: Arc<SoduMetaStore>,
}

impl SegmentIngester {
    /// new returns SegmentIngester instance.
    pub fn new(segments_path: PathBuf, meta_store: Arc<SoduMetaStore>) -> SegmentIngester {
        fs::create_dir_all(&segments_path).unwrap();
        // Get all the start id of the current segment.
        let segment_file_path = read_files_in_dir(&segments_path, "segment").unwrap();
        let mut segment_file_ids = get_file_ids(&segment_file_path);
        segment_file_ids.sort();
        let mut next_segment_id = 1;
        if let Some(segment_id) = segment_file_ids.pop() {
            next_segment_id = segment_id + 1;
        }

        let iou = IoUring::new(50).unwrap();
        SegmentIngester {
            iou: iou,
            segments_path: segments_path,
            current_segment: Segment::new(),
            span_buffer: Buffer::with_size(1 << 20),
            next_segment_id: next_segment_id,
            submitted_builders: Vec::new(),
            submitted_iou_ids: HashSet::new(),
            builder_freelist: Vec::new(),
            buffered_segment: VecDeque::new(),
            meta_store,
        }
    }

    /// push span insert span to the ingester.
    pub fn push_span(&mut self, wal_id: u64, req: EncodedRequest) {
        self.flush_segment_if_necessary(false);
        self.span_buffer.clear();
        let inplace_decoder = InplaceSpanDecoder(req.encoded_span);
        let hashed_trace_id = inplace_decoder.hashed_trace_id();
        // Find that is the incoming span is part of
        // previous segment trace.
        for segment in &mut self.buffered_segment {
            if !segment.contain_trace(&hashed_trace_id) {
                continue;
            }
            // Incoming trace is part of previous trace.
            // So, let's update it.
            segment.put_delayed_span(hashed_trace_id, wal_id, req);
            return;
        }
        // Incoming span is part of current segment. So, let's just add there.
        self.current_segment.put_span(hashed_trace_id, wal_id, req);
    }

    /// flush_segment_if_necessary flushes the buffered segment. If it's crosses the grace period
    /// time.
    pub fn flush_segment_if_necessary(&mut self, force: bool) {
        // Flush segments if necessary. Calculate whether the current segment reached the threshold
        // size
        if self.current_segment.segment_size() < 62 << 20 && !force {
            return;
        }
        debug!(
            "current segment finished with {} spans. moving to buffer",
            self.current_segment.num_spans()
        );
        let segment = Segment::new();
        let prev_segment = mem::replace(&mut self.current_segment, segment);
        // Add the previous segment to the buffered segment.
        self.buffered_segment.push_back(prev_segment);
        // Find is there any buffered segment reached the elapsed time to
        // flush to the disk.
        loop {
            if let Some(past_segment) = self.buffered_segment.pop_front() {
                // calculate grace period if it's not force flush.
                if !force {
                    // Check whether the past_segment older than five minutes. If's older than five minutes
                    // then flush to the disk.
                    let segment_finish_time =
                        UNIX_EPOCH + Duration::from_nanos(past_segment.max_trace_start_ts());
                    let elapsed = segment_finish_time.elapsed().unwrap();
                    if elapsed.as_secs() / 60 < 1 {
                        debug!("skipping the segment flush since the it it's in the grace period. elapsed time {:?}", elapsed.as_secs());
                        break;
                    }
                }
                // TODO: This may run as if we are running in a sync way. We need some thorttling while
                // flushing series of segments.
                self.reclaim_submitted_builder_buffer();
                // This segment is older than 5 minutes. So, let's flush this.
                let mut builder = self.get_segment_builder();
                for (start_ts, spans) in past_segment.iter() {
                    builder.add_trace(start_ts, spans);
                }
                let buf = builder.finish_segment(
                    past_segment.index(),
                    past_segment.max_wal_id(),
                    past_segment.max_wal_offset(),
                    past_segment.delayed_wal_offsets(),
                );
                let segment_file = self.get_next_segment_file();

                unsafe {
                    let mut sq = self.iou.sq();
                    // TODO: Needs to handled carefully. We really need some throttling saying
                    //  how many buffer can be flushed at a time.
                    let mut sqe = sq
                        .prepare_sqe()
                        .expect("unable to get the next submission queue entry");
                    // Verify whether is it necessary to hold the reference of slice.
                    let slice = [IoSlice::new(buf.bytes_ref())];
                    sqe.prep_write_vectored(segment_file.as_raw_fd(), &slice, 0);
                    sqe.set_user_data(self.next_segment_id - 1);
                    self.submitted_iou_ids.insert(self.next_segment_id - 1);
                    self.iou
                        .sq()
                        .submit()
                        .expect("unable to submit submission entry");
                    debug!(
                        "flushing segment {} with size {}",
                        &self.next_segment_id - 1,
                        buf.size()
                    );
                }
                self.submitted_builders.push(builder);
            }
            break;
        }
    }

    /// reclaim_submitted_builder_buffer reclaims the submitted builder buffer after it's completion.
    pub fn reclaim_submitted_builder_buffer(&mut self) {
        if self.submitted_iou_ids.len() == 0 {
            return;
        }
        // Wait for the previous submitted request before building new builder. This
        // allow us to throttle the builder memory and number of async request.
        // Let's wait for all the submitted events. Actually we can do in one call,
        // but still iou doesn't support io_uring_peek_batch_cqe.
        let mut completed_segment_id = HashSet::with_capacity(self.submitted_iou_ids.len());
        for _ in 0..self.submitted_iou_ids.len() {
            let mut cq = self.iou.cq();
            let cqe = cq.wait_for_cqe().expect("unable to wait for ceq event");
            // Assert all the pre committed conditions.
            // assert_eq!(cqe.is_timeout(), false);
            assert_eq!(self.submitted_iou_ids.contains(&cqe.user_data()), true);
            completed_segment_id.insert(cqe.user_data());
        }
        assert_eq!(completed_segment_id.len(), self.submitted_iou_ids.len());
        self.submitted_iou_ids.clear();
        loop {
            if let Some(mut builder) = self.submitted_builders.pop() {
                builder.clear();
                self.builder_freelist.push(builder);
                continue;
            }
            break;
        }
    }

    /// get_segment_builder returns segment builder if it's in the freelist. Otherwise, it creates
    /// new SegmentBuilder.
    fn get_segment_builder(&mut self) -> SegmentBuilder {
        if let Some(builder) = self.builder_freelist.pop() {
            return builder;
        }
        SegmentBuilder::new()
    }

    /// get_next_segment file return's next segment file.
    fn get_next_segment_file(&mut self) -> File {
        let segment_path = self
            .segments_path
            .join(format!("{:?}.segment", self.next_segment_id));
        self.next_segment_id += 1;
        File::create(&segment_path)
            .expect(&format!("unable to create segment file {:?}", segment_path))
    }

    /// get_segments_for_query
    pub fn get_segments_for_query(&self, req: &QueryRequest) -> Vec<&Segment> {
        let mut segments = Vec::new();

        // Check whether the current segment falls in the time range.
        if is_over_lapping_range(req.get_time_range(), &self.current_segment.get_time_range()) {
            segments.push(&self.current_segment);
        }

        // Now, iterate over buffered segments.
        for bufferd_segment in &self.buffered_segment {
            if is_over_lapping_range(req.get_time_range(), &bufferd_segment.get_time_range()) {
                segments.push(bufferd_segment);
            }
        }
        segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer::tests::get_buffer;
    use crate::segment::segment::tests::gen_traces;
    use std::fs::metadata;
    use tempfile::tempdir;

    // #[test]
    // fn test_segment_ingester() {
    //     let dir = tempdir().unwrap();
    //     let tmp_path = dir.path();
    //     let mut ingester = SegmentIngester::new(tmp_path.clone().to_path_buf());
    //     // Generate start_ts as 1 hour back.
    //     let start_ts = SystemTime::now()
    //         .sub(Duration::from_secs(60 * 60))
    //         .elapsed()
    //         .unwrap()
    //         .as_secs();
    //     let traces = gen_traces(start_ts);
    //     for trace in traces {
    //         for span in trace {
    //             ingester.push_span(span);
    //         }
    //     }
    //
    //     let mut dummy_buffer = get_buffer(66 << 20);
    //     let dummy_segment = Segment::from_buffer(dummy_buffer);
    //     let segment = mem::replace(&mut ingester.current_segment, dummy_segment);
    //     ingester.buffered_segment.push_back(segment);
    //     // flush the past segment.
    //     ingester.flush_segment_if_necessary();
    //     assert_eq!(ingester.submitted_iou_ids.len(), 1);
    //     // reclaim the submitted buffer.
    //     ingester.reclaim_submitted_builder_buffer();
    //     assert_eq!(ingester.builder_freelist.len(), 1);
    //     // file should have some data.
    //     let metadata = metadata(tmp_path.join("1.segment")).unwrap();
    //     assert!(metadata.len() > 70);
    // }
}
