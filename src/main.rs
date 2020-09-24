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
#![allow(dead_code)]
#![feature(test)]
extern crate test;

mod buffer;
mod encoder;
mod ingester;
mod memtable;
mod options;
mod proto;
mod segment;
mod server;
mod table;
mod utils;

use log::debug;
use parking_lot::Mutex;
use std::sync::Arc;

fn main() {
    let opt = options::options::Options::init();
    // Initialize all the global helper utils.
    utils::utils::init_all_utils();
    debug!("running in debug mode");
    // Create the ingester instance.
    let ingester = ingester::segment_ingester::SegmentIngester::new(opt.shard_path);
    let protected_ingester = Arc::new(Mutex::new(ingester));
    // run the the ingester.
    let (coordinator, receiver) = ingester::coordinator::IngesterCoordinator::new();
    let runner = ingester::ingester_runner::IngesterRunner::new(protected_ingester.clone());
    runner.run(receiver);
    // Start the grpc server.
    server::grpc::start_server(coordinator);
}
