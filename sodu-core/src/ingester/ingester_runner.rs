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

use crate::ingester::segment_ingester::SegmentIngester;
use crate::meta_store::sodu_meta_store::SoduMetaStore;
use crate::proto::trace::Span;
use crate::wal::wal::Wal;
use crossbeam::crossbeam_channel::Receiver;
use log::{info, warn};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

/// IngesterRunnerRequest contains required parameter to ingest spans.
pub struct IngesterRunnerRequest {
    /// spans that needs to be ingested.
    pub spans: Vec<Span>,
    /// done is an one shot channel.
    pub done: mpsc::Sender<u8>,
    /// instace_names is the list of incoming instance names.
    pub instance_names: Vec<String>,
    /// service_names is the list of incoming service names.
    pub service_names: Vec<String>,
}
/// IngesterRunner is responsible for ingesting spans to the segment.
pub struct IngesterRunner {
    /// segment_ingester is used to ingest spans. Now, it's protected by mutex. Considering
    /// there won't much on lock contention. Since, it's write heavy workload. Later this
    /// needs to be changed to lock free and per core per thread architecture. For simplicity
    /// sake. Just bear with this for now.
    segment_ingester: Arc<Mutex<SegmentIngester>>,
    /// wal is use to presist all the incoming request.
    wal: Wal,
    /// meta_store is used to store all the metadata of segements and wal checkpoint.
    meta_store: Arc<SoduMetaStore>,
    /// service_names is the list of service name that this storage instace serves.
    service_names: HashSet<String>,
    /// instance_name is the list of instance name that this storage instance serves.
    instance_names: HashSet<String>,
    /// operations_names is the list of operation names that this storage instance serves.
    operation_names: HashSet<String>,
}

impl IngesterRunner {
    /// new returns IngesterRunner.
    pub fn new(
        segment_ingester: Arc<Mutex<SegmentIngester>>,
        wal: Wal,
        meta_store: Arc<SoduMetaStore>,
    ) -> IngesterRunner {
        IngesterRunner {
            segment_ingester,
            wal,
            meta_store,
            service_names: HashSet::default(),
            instance_names: HashSet::default(),
            operation_names: HashSet::default(),
        }
    }

    /// run starts the ingester runner and get ready to start accepting spans from
    /// the collector.
    pub fn run(mut self, receiver: Receiver<IngesterRunnerRequest>) {
        thread::spawn(move || {
            info!("spinning ingester runner");
            loop {
                let req = receiver.recv().unwrap();
                // update the metadata.
                self.update_service_names(req.service_names);
                self.update_instance_id(req.instance_names);
                // encode and ingest span.
                self.wal.wait_for_submitted_wal_span();
                self.wal.change_wal_if_neccessary();
                let current_wal_id = self.wal.current_wal_id();
                let ingester = &mut self.segment_ingester.lock();
                for span in &req.spans {
                    // Update operation names if it's not updated earlier.
                    if !self.operation_names.contains(&span.name) {
                        self.meta_store.save_operation_name(&span.name);
                        self.operation_names.insert(span.name.clone());
                    }
                    let encoded_req = self.wal.write_spans(span);
                    ingester.push_span(current_wal_id, encoded_req);
                }
                if let Err(e) = req.done.send(0) {
                    warn!("unable to send done to the ingester request {:?}", e);
                }
                self.wal.submit_buffer_to_iou();
            }
        });
    }

    /// update_service_names update the service name.
    pub fn update_service_names(&mut self, service_names: Vec<String>) {
        for service_name in service_names {
            if self.service_names.contains(&service_name) {
                continue;
            }
            self.meta_store.save_service_name(&service_name);
            self.service_names.insert(service_name);
        }
    }

    /// update_instance_id updates the instance id.
    pub fn update_instance_id(&mut self, instance_ids: Vec<String>) {
        for instance_id in instance_ids {
            if self.instance_names.contains(&instance_id) {
                continue;
            }
            self.meta_store.save_instance_id(&instance_id);
            self.instance_names.insert(instance_id);
        }
    }
}
