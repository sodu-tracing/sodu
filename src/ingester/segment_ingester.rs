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
use crate::encoder::span::encode_span;
use crate::proto::trace::Span;
use crate::segment::segment::Segment;
use crate::segment::segment_builder::SegmentBuilder;
use crossbeam::atomic::AtomicCell;
use futures::io::IoSlice;
use iou::IoUring;
use log::{debug, info};
use parking_lot::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Sub;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::fs;
pub struct SegmentIngester {
    current_segment: Segment,
    buffered_segment: VecDeque<Segment>,
    span_buffer: Buffer,
    iou: IoUring,
    next_segment_id: u64,
    segments_path: PathBuf,
    submitted_builders: Vec<SegmentBuilder>,
    submitted_iou_ids: HashSet<u64>,
    builder_freelist: Vec<SegmentBuilder>,
}

impl SegmentIngester {

    pub fn new(segments_path: PathBuf) -> SegmentIngester{
        fs::create_dir_all(&segments_path).unwrap();
        let iou = IoUring::new(50).unwrap();
        SegmentIngester{
            iou:iou,
            segments_path:segments_path,
            current_segment: Segment::new(),
            span_buffer: Buffer::with_size(1<<20),
            next_segment_id: 1,
            submitted_builders: Vec::new(),
            submitted_iou_ids: HashSet::new(),
            builder_freelist: Vec::new(),
            buffered_segment: VecDeque::new(),
        }
    }
    
    pub fn push_span(&mut self, span: Span) {
        self.span_buffer.clear();
        let indices = encode_span(&span, &mut self.span_buffer);
        // Calculate hash_id for the given span
        let mut hasher = DefaultHasher::new();
        span.trace_id.hash(&mut hasher);
        let hashed_trace_id = hasher.finish();
        // Find that is the incoming span is part of
        // previous segment trace.
        for segment in &mut self.buffered_segment {
            if !segment.contain_trace(&hashed_trace_id) {
                continue;
            }
            // Incoming trace is part of previous trace.
            // So, let's update it.
            segment.put_span(
                hashed_trace_id,
                self.span_buffer.bytes_ref(),
                span.start_time_unix_nano,
                indices,
            );
            return;
        }
        // Incoming span is part of current segment. So, let's just add there.
        self.current_segment.put_span(
            hashed_trace_id,
            self.span_buffer.bytes_ref(),
            span.start_time_unix_nano,
            indices,
        );
    }

    pub fn flush_segment_if_necessary(&mut self) {
        // Flush segments if necessary. Calculate whether the current segment reached the threshold
        // size
        if self.current_segment.segment_size() >= 64 << 20 {
            return;
        }
        let segment = Segment::new();
        let prev_segment = mem::replace(&mut self.current_segment, segment);
        // Add the previous segment to the buffered segment.
        self.buffered_segment.push_back(prev_segment);
        // Find is there any buffered segment reached the elapsed time to
        // flush to the disk.
        loop {
            if let Some(past_segment) = self.buffered_segment.pop_front() {
                // Check whether the past_segment older than five minutes. If's older than five minutes
                // then flush to the disk.
                let max_start_time = Duration::from_nanos(past_segment.max_trace_start_ts());
                let elapsed = SystemTime::now().sub(max_start_time).elapsed().unwrap();
                if elapsed.as_secs() / 60 < 5 {
                    break;
                }
                self.reclaim_submitted_builder_buffer();
                // This segment is older than 5 minutes. So, let's flush this.
                let mut builder = self.get_segment_builder();
                for (start_ts, spans) in past_segment.iter() {
                    builder.add_trace(start_ts, spans);
                }
                let buf = builder.finish_segment(past_segment.index());
                let segment_file = self.get_next_segment_file();

                unsafe {
                    let mut sq = self.iou.sq();
                    // TODO: Needs to handled carefully. We really need some throttling saying
                    //  how many buffer can be flushed at a time.
                    let mut sqe = sq
                        .next_sqe()
                        .expect("unable to get the next submission queue entry");
                    let slice = [IoSlice::new(buf.bytes_ref())];
                    sqe.prep_write_vectored(segment_file.as_raw_fd(), &slice, 0);
                    sqe.set_user_data(self.next_segment_id - 1);
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

    fn reclaim_submitted_builder_buffer(&mut self) {
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
            assert_eq!(cqe.is_timeout(), false);
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

    fn get_segment_builder(&mut self) -> SegmentBuilder {
        if let Some(builder) = self.builder_freelist.pop() {
            return builder;
        }
        SegmentBuilder::new()
    }

    fn get_next_segment_file(&mut self) -> File {
        let segment_path = self
            .segments_path
            .join(format!("{:?}.segment", self.next_segment_id));
        self.next_segment_id += 1;
        File::create(&segment_path)
            .expect(&format!("unable to create segment file {:?}", segment_path))
    }
}
