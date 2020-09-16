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

use crate::ingester::ingester::Ingester;
use crate::options::options::Options;
use crate::proto::trace::{ResourceSpans, Span};
use crossbeam_channel::{bounded, Sender};
use crate::utils::placement::{CoreId, get_core_ids};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use log::info;


/// IngesterCoordinator is response for spinning multiple ingester according to the
/// core count.
#[derive(Clone)]
pub struct IngesterCoordinator {
    /// transport of the ingester.
    transport: Vec<Sender<Vec<Span>>>,
    /// no of cpu.
    core_ids: Vec<CoreId>,
}

impl IngesterCoordinator {
    /// start_ingesters start ingester for every core and pins the ingester to the
    /// given core.
    pub fn start_ingesters(opt: &Options) -> IngesterCoordinator {
        let core_ids = get_core_ids().unwrap();
        info!("total number of cpu {:?}", core_ids.len());
        let mut transport = Vec::new();
        // START ingester for all the core.
        for core_id in core_ids.clone() {
            let ingester_path = opt.shard_path.join(format!("{:?}", &core_id));
            fs::create_dir_all(&ingester_path).expect(&format!(
                "unable to create ingester dir {:?}",
                &ingester_path
            ));
            // TODO: find the last table id.
            let ingester = Ingester::new(ingester_path, 0);
            let (sender, receiver) = bounded(20);
            ingester.start(receiver, core_id);
            transport.push(sender);
        }
        IngesterCoordinator { transport, core_ids }
    }

    /// send_spans send the spans to the respective ingester.
    pub fn send_spans(&self, resource_spans: Vec<ResourceSpans>) {
        // batch all the spans for the required shard.
        let mut batches: Vec<Vec<Span>> = vec![Vec::new(); self.core_ids.len()];
        // Batch the spans according to the hash.
        for resource_span in resource_spans {
            for instrumental_library_span in resource_span.instrumentation_library_spans.into_vec()
            {
                for span in instrumental_library_span.spans.into_vec() {
                    let mut hasher = DefaultHasher::new();
                    span.trace_id.hash(&mut hasher);
                    let batch = batches
                        .get_mut(hasher.finish() as usize / self.core_ids.len())
                        .unwrap();
                    batch.push(span);
                }
            }
        }
        // Send the the batched spans to the respective ingester.
        for (i, batch) in batches.into_iter().enumerate() {
            if batch.len() == 0 {
                continue;
            }
            // TODO: This may block.So, find a retry later mechanism.
            self.transport[i].send(batch).unwrap();
        }
    }
}
