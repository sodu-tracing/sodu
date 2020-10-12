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
use crate::proto::service::QueryRequest;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub struct QueryExecutor {
    ingester: Arc<Mutex<SegmentIngester>>,
}

impl QueryExecutor {
    pub fn query(&self, req: QueryRequest) {
        // Find all the segments for the given ts.
        let ingester = self.ingester.lock();
        let in_memeory_segments = ingester.get_segments_for_query(&req);
    }
}
