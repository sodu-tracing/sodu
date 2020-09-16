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
use flexi_logger::Logger;
use std::fmt::Display;

pub fn init_all_utils() {
    Logger::with_env_or_str("info").start().unwrap();
}

/// create_index_key creates index key. It's used as a primary key to store posting list.
pub fn create_index_key<T: Display>(k: &String, v: T) -> String {
    format!("{}-{}", k, v)
}
