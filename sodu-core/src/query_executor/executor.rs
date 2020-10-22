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
use crate::ingester::segment_ingester::SegmentIngester;
use crate::meta_store::sodu_meta_store::SoduMetaStore;
use crate::proto::service::{InternalTrace, QueryRequest, QueryResponse, TagResponse};
use crate::segment::segment_file::SegmentFile;
use crate::utils::utils::{
    calculate_trace_size, get_file_ids, is_over_lapping_range, read_files_in_dir,
};
use anyhow::Context;
use parking_lot::Mutex;
use protobuf::RepeatedField;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// QueryExecutor is used to execute sodu quries.
#[derive(Clone)]
pub struct QueryExecutor {
    /// ingester is the segment ingester. In QueryExecutor we'll be using it for filtering traces from
    /// the in-memory segments.
    ingester: Arc<Mutex<SegmentIngester>>,
    /// segment_path is the path of segment files.
    segment_path: PathBuf,
    meta_store: Arc<SoduMetaStore>,
}

impl QueryExecutor {
    pub fn new(segment_path: PathBuf, ingester: Arc<Mutex<SegmentIngester>>) -> QueryExecutor {
        let meta_store = ingester.lock().get_meta_store();
        QueryExecutor {
            ingester,
            segment_path,
            meta_store,
        }
    }
    /// query is used to query the sodu instance and return back the filtered traces for the given
    /// time span.
    pub fn query(&self, req: QueryRequest) -> QueryResponse {
        let mut num_of_traces = 0;
        let mut internal_traces = Vec::new();
        // find all the segments for the given ts.
        let ingester = self.ingester.lock();
        let in_memeory_segments = ingester.get_segments_for_query(&req);
        // iterate over over filtered in-memory segment and gather traces till
        // the given limit.
        for segment in in_memeory_segments {
            if let Some(itr) = segment.get_iter_for_query(&req) {
                for (start_ts, spans) in itr {
                    let mut internal_trace = InternalTrace::default();
                    internal_trace.set_trace(spans_to_trace(spans));
                    internal_trace.set_start_ts(start_ts);
                    num_of_traces += 1;
                    internal_traces.push(internal_trace);
                    if num_of_traces >= 1000 {
                        let mut res = QueryResponse::default();
                        res.set_traces(RepeatedField::from(internal_traces));
                        return res;
                    }
                }
            }
        }
        // Let's iterate over segment files and retrive all the traces.
        let segment_files = read_files_in_dir(&self.segment_path, "segment").unwrap();
        let mut segment_file_ids = get_file_ids(&segment_files);
        // sort in reverse order.
        segment_file_ids.sort_by(|a, b| b.cmp(&a));
        // TODO: This is stupidest iteration. I'm hanging this here, beacuse for index
        // I'm thinking to use rocksdb itself. Based on the use case let see.
        // Just reading the meta data of 20 files took 30 secs. pahhh.
        // Let's go over segement files one by one.

        // TODO: bug we may get partial segment file id. So, we need checkpoint to detect the
        // right starting point for the current snapshot.
        for segment_file_id in segment_file_ids {
            let file = File::open(
                &self
                    .segment_path
                    .join(format!("{:?}.segment", segment_file_id)),
            )
            .unwrap();
            let segment_file = SegmentFile::new(file)
                .context(format!(
                    "error opening segment file {:?} while querying",
                    segment_file_id
                ))
                .unwrap();
            // filter the segment file based on the requested time range.
            if !is_over_lapping_range(req.get_time_range(), segment_file.get_time_range()) {
                continue;
            }

            // iterate over the filtered segment to collect all the traces.
            if let Some(itr) = segment_file.get_iter_for_query(&req) {
                for (start_ts, trace) in itr {
                    let mut internal_trace = InternalTrace::default();
                    internal_trace.set_trace(trace);
                    internal_trace.set_start_ts(start_ts);
                    num_of_traces += 1;
                    internal_traces.push(internal_trace);
                    if num_of_traces >= 1000 {
                        let mut res = QueryResponse::default();
                        res.set_traces(RepeatedField::from(internal_traces));
                        return res;
                    }
                }
            }
        }
        let mut res = QueryResponse::default();
        res.set_traces(RepeatedField::from(internal_traces));
        return res;
    }

    /// get_tags returns the all the tags.
    pub fn get_tags(&self) -> TagResponse {
        TagResponse {
            service_names: RepeatedField::from(self.meta_store.get_service_names()),
            instance_ids: RepeatedField::from(self.meta_store.get_instance_ids()),
            operation_names: RepeatedField::from(self.meta_store.get_operation_names()),
            ..Default::default()
        }
    }
}

/// spans_to_trace is used to convert list of spans to trace.
fn spans_to_trace(spans: Vec<&[u8]>) -> Vec<u8> {
    let size = calculate_trace_size(&spans);
    let mut buffer = Buffer::with_size(size);
    for (idx, span) in spans.into_iter().enumerate() {
        if idx == 0 {
            buffer.write_raw_slice(span);
            continue;
        }
        buffer.write_raw_slice(&span[16..]);
    }
    buffer.bytes()
}
