use crate::buffer::buffer::Buffer;
use crate::encoder::decoder::InplaceSpanDecoder;
use crate::encoder::span::encode_span;
use crate::proto::trace::Span;
use crate::segment::segment::Segment;
use crossbeam::atomic::AtomicCell;
use iou::IoUring;
use parking_lot::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

pub struct SegmentIngester {
    current_segment: Segment,
    buffered_segment: VecDeque<Segment>,
    span_buffer: Buffer,
    iou: IoUring,
}

impl SegmentIngester {
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
        self.buffered_segment.push(prev_segment);
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
                // This segment is older than 5 minutes. So, let's flush this.
            }
            break;
        }
    }
}
