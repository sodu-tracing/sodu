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
use crate::proto::common::{AnyValue_oneof_value, KeyValue};
use crate::proto::service::TimeRange;
use crate::proto::trace::Span;
use anyhow::{Context, Result};
use flexi_logger::Logger;
use protobuf::RepeatedField;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fmt::Display;
use std::fs::read_dir;
use std::hash::{Hash, Hasher};
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
    file_ids
}

/// extract_indices_from_span returns indices for the given span.
pub fn extract_indices_from_span(span: &Span) -> HashSet<String> {
    let mut indices = HashSet::new();
    extract_indices_from_attributes(&span.attributes, &mut indices);
    // generate indices for events.
    for event in &span.events {
        extract_indices_from_attributes(&event.attributes, &mut indices);
    }
    // generate indices for links.
    for link in &span.links {
        extract_indices_from_attributes(&link.attributes, &mut indices);
    }
    indices
}

/// extract_indices_from_attributes extract indices for the given attributes.
fn extract_indices_from_attributes(
    attributes: &RepeatedField<KeyValue>,
    indices: &mut HashSet<String>,
) {
    for attribute in attributes {
        let value = attribute.value.as_ref().unwrap().value.as_ref().unwrap();
        match value {
            AnyValue_oneof_value::bool_value(val) => {
                indices.insert(create_index_key(&attribute.key, val));
            }
            AnyValue_oneof_value::string_value(val) => {
                indices.insert(create_index_key(&attribute.key, val));
            }
            AnyValue_oneof_value::int_value(val) => {
                indices.insert(create_index_key(&attribute.key, val));
            }
            AnyValue_oneof_value::double_value(val) => {
                indices.insert(create_index_key(&attribute.key, val));
            }
            _ => {}
        }
    }
}

/// is_over_lapping tells that whether the requesting time range is in block time range or not.
pub fn is_over_lapping_range(req: &TimeRange, block: &TimeRange) -> bool {
    // get all the required data.
    let req_min_start_ts = req.get_min_start_ts();
    let req_max_start_ts = req.get_max_start_ts();
    if req_max_start_ts == 0 && req_min_start_ts == 0 {
        return true;
    }
    let block_min_start_ts = block.get_min_start_ts();
    let block_max_start_ts = block.get_max_start_ts();
    (req_min_start_ts <= block_min_start_ts && block_min_start_ts <= req_max_start_ts)
        || (req_min_start_ts <= block_max_start_ts && block_max_start_ts <= req_max_start_ts)
}

/// this mod contains test helper functions for the whole project level.
#[cfg(test)]
pub mod tests {
    use crate::buffer::buffer::Buffer;
    use crate::encoder::decoder::InplaceSpanDecoder;
    use crate::encoder::span::encode_span;
    use crate::proto::trace::Span;
    use crate::wal::wal::EncodedRequest;

    /// get_encoded_req returns encoded request and hashed trace id.
    pub fn get_encoded_req<'a>(
        span: &Span,
        mut buffer: &'a mut Buffer,
    ) -> (EncodedRequest<'a>, u64) {
        buffer.clear();
        let indices = encode_span(&span, &mut buffer);
        let req = EncodedRequest {
            encoded_span: buffer.bytes_ref(),
            indices: indices,
            start_ts: span.start_time_unix_nano,
            wal_offset: 0,
        };
        let decoder = InplaceSpanDecoder(req.encoded_span);
        let hashed_id = decoder.hashed_trace_id();
        (req, hashed_id)
    }
}

/// calculate_trace_size is used to calculate trace size from the list of spans.
pub fn calculate_trace_size(trace: &Vec<&[u8]>) -> usize {
    let mut size: usize = 0;
    // iter collect would have done this job simple why to allocate here :(
    for (idx, span) in trace.iter().enumerate() {
        if idx == 0 {
            size += span.len();
            continue;
        }
        size += span.len() - 16;
    }
    size
}

/// hash_bytes return the hashed output of the given input.
pub fn hash_bytes(input: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}
