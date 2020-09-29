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
use std::fs;
use std::path::PathBuf;
use structopt::StructOpt;
#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "akkal")]
pub struct Options {
    /// Output directory
    #[structopt(short, long, parse(from_os_str), default_value = "./akkal")]
    pub dir: PathBuf,
    /// shard_path contains the path for all the shard ingested files.
    #[structopt(skip)]
    pub shard_path: PathBuf,
    /// wal_path contains all the wal related files.
    #[structopt(skip)]
    pub wal_path: PathBuf,
}

impl Options {
    pub fn init() -> Options {
        let mut opt = Options::from_args();
        // Create the shard path.
        opt.shard_path = opt.dir.join("shard");
        fs::create_dir_all(&opt.shard_path).expect("unable to create shard path");
        // Create the wal path.
        opt.wal_path = opt.dir.joun("wal");
        fs::create_dir_all(&opt.wal_path).expect("unable to create shard path");
        opt
    }
}
