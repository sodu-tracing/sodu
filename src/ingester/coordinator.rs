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
use crate::utils::placement::{get_core_ids, CoreId};
use crossbeam_channel::{bounded, Receiver, Sender};
use log::info;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};

/// IngesterCoordinator is response for spinning multiple ingester according to the
/// core count.
#[derive(Clone)]
pub struct IngesterCoordinator {
    /// transport of the ingester.
    transport: Sender<Vec<Span>>,
}

impl IngesterCoordinator {
    pub fn new() -> (IngesterCoordinator, Receiver<Vec<Span>>) {
        let (sender, receiver) = bounded(20);
        (IngesterCoordinator { transport: sender }, receiver)
    }

    /// send_spans send the spans to the respective ingester.
    pub fn send_spans(&self, resource_spans: Vec<ResourceSpans>) {
        // batch all the spans for the required shard.
        let mut spans: Vec<Span> = Vec::new();
        // Batch the spans according to the hash.
        for resource_span in resource_spans {
            for instrumental_library_span in resource_span.instrumentation_library_spans.into_vec()
            {
                for mut span in instrumental_library_span.spans.into_vec() {
                    // Add the resource attributes because, it contains attributes like
                    // resource name and instance name.
                    for resource_attribute in
                        resource_span.resource.clone().unwrap().attributes.to_vec()
                    {
                        span.attributes.push(resource_attribute);
                    }
                    spans.push(span);
                }
            }
        }
        // Send the the batched spans to the respective ingester.
        self.transport.send(spans).unwrap();
    }
}
