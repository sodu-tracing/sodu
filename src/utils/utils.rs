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
use anyhow::{Context, Result};
use flexi_logger::Logger;
use std::fmt::Display;
use std::fs::read_dir;
use std::path::PathBuf;
pub fn init_all_utils() {
    Logger::with_env_or_str("debug").start().unwrap();
}

/// create_index_key creates index key. It's used as a primary key to store posting list.
pub fn create_index_key<T: Display>(k: &String, v: T) -> String {
    format!("{}-{}", k, v)
}

/// read_files_in_dir gives the list of files in a directory with the given extension.
pub fn read_files_in_dir(path: &PathBuf, ext: &str) -> Result<Vec<PathBuf>> {
    let entries = read_dir(path).context(format!("unable to read file in dir {:?}", path))?;
    let mut entries_path = Vec::new();
    for entry in entries {
        let entry = entry.context(format!("unable to get file entry at {:?}", path))?;
        let path = entry.path();
        // Skip all the child directories.
        if path.is_dir() {
            continue;
        }
        if path
            .extension()
            .context("unable to get the file extension")?
            != ext
        {
            continue;
        }
        entries_path.push(path);
    }
    Ok(entries_path)
}

/// get_file_ids returns all the file ids of the given path vector.
pub fn get_file_ids(paths: &Vec<PathBuf>) -> Vec<u64> {
    let mut file_ids = Vec::with_capacity(paths.len());
    for path in paths {
        let file_name = path.file_stem().unwrap();
        let file_id = file_name.to_str().unwrap().parse::<u64>().unwrap();
        file_ids.push(file_id);
    }
    file_ids.sort();
    file_ids
}
