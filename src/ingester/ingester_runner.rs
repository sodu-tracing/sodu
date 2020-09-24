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

use std::sync::Arc;
use parking_lot::Mutex;
use crate::ingester::segment_ingester::SegmentIngester;
use std::thread;
use crossbeam::crossbeam_channel::Receiver;

use log::{info};
use crate::proto::trace::Span;

/// IngesterRunner is responsible for ingesting spans to the segment.
pub struct IngesterRunner {
    /// segment_ingester is used to ingest spans. Now, it's protected by mutex. Considering
    /// there won't much on lock contention. Since, it's write heavy workload. Later this
    /// needs to be changed to lock free and per core per thread architecture. For simplicity
    /// sake. Just bear with this for now.
    segment_ingester: Arc<Mutex<SegmentIngester>>
}

impl IngesterRunner {
    /// new returns IngesterRunner.
    pub fn new(segment_ingester: Arc<Mutex<SegmentIngester>>) -> IngesterRunner{
        IngesterRunner{
            segment_ingester
        }
    }

    /// run starts the ingester runner and get ready to start accepting spans from
    /// the collector.
    pub fn run(self, receiver: Receiver<Vec<Span>>) {
        thread::spawn(move ||{
            info!("spinning ingester runner");
            loop{
                // TODO: smart batching.
                let spans = receiver.recv().unwrap();
                let mut ingester = &mut self.segment_ingester.lock();
                for span in spans{
                    ingester.push_span(span);
                }
            }
        });
    }
}