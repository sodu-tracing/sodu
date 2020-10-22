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
use crate::buffer::buffer::Buffer;
use crate::options::options::Options;
use crate::utils::types::WalCheckPoint;
use rocksdb::{DBOptions, Writable, DB};
use std::convert::TryInto;
use std::u64;

/// WAL_CHECK_POINT is the wal check point key. This gives us the offset where we have to replay
/// the wal.
const WAL_CHECK_POINT: &str = "!sodu!pathukapu!ellai!";
/// SERVICE_NAME_PREFIX is the service name key prefix. This is used to get the service name. By doing
/// prefix iteration.
const SERVICE_NAME_PREFIX: &str = "!sodu!sevai!peyar";
/// OPERATION_NAME_PREFIX is the operation name key prefix. This is used to get the operation name, by doing
/// prefix key iteration.
const OPERATION_NAME_PREFIX: &str = "!sodu!seyal!peyar";
/// INSTANCE_NAME_PREFIX is the instance name key prefix. This is used to get the operation name, by doing
/// prefix key iteration.
const INSTANCE_NAME_PREFIX: &str = "!sodu!mega!kanini!peyar";
/// SoduMetaStore is used to store meta data releated to sodu-storage.
pub struct SoduMetaStore {
    /// db is the rocks db instance.
    db: DB,
    empty_val: [u8; 2],
}

impl SoduMetaStore {
    /// new returns SoduMetaStore.
    pub fn new(opt: &Options) -> SoduMetaStore {
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        let db = DB::open(
            opts,
            &opt.meta_store_path
                .clone()
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .unwrap();
        SoduMetaStore {
            db,
            empty_val: [0; 2],
        }
    }

    /// save_wal_check_point saves wal checkpoint.
    pub fn save_wal_check_point(&self, check_point: WalCheckPoint) {
        // encode wal id and wal offset.
        let mut buffer = Buffer::with_size(24);
        buffer.write_raw_slice(&check_point.wal_id.to_be_bytes());
        buffer.write_raw_slice(&check_point.wal_offset.to_be_bytes());
        buffer.write_raw_slice(&check_point.segment_id.to_be_bytes());
        self.db
            .put(WAL_CHECK_POINT.as_bytes(), buffer.bytes_ref())
            .map_err(|e| {
                format!(
                    "error while saving checkpoint for wal id {:?} and wal offset {:?}. err_msg: {:?}",
                    check_point.wal_id, check_point.wal_offset,e
                )
            })
            .unwrap();
    }

    /// get_wal_check_point retrives wal check point.
    pub fn get_wal_check_point(&self) -> Option<WalCheckPoint> {
        let check_point = self
            .db
            .get(WAL_CHECK_POINT.as_bytes())
            .map_err(|e| format!("error while retriving wal check point. err_msg: {:?}", e))
            .unwrap();
        // return the checkpoint if we persisted checkpoint.
        if let Some(val) = check_point {
            // always check point should be of length of 16.
            assert_eq!(val.len(), 24);
            let wal_id = u64::from_be_bytes(val[..8].try_into().unwrap());
            let wal_offset = u64::from_be_bytes(val[8..16].try_into().unwrap());
            let segment_id = u64::from_be_bytes(val[16..].try_into().unwrap());
            return Some(WalCheckPoint {
                wal_id,
                wal_offset,
                segment_id,
            });
        }
        None
    }

    /// save_service_name saves service names.
    pub fn save_service_name(&self, service_name: &String) {
        self.db
            .put(
                &self.create_prefix_index(SERVICE_NAME_PREFIX, service_name),
                &self.empty_val,
            )
            .map_err(|e| {
                format!(
                    "error while saving service name {:?}. err_msg {:?}",
                    service_name, e
                )
            })
            .unwrap();
    }

    /// save_instance_id saves instance id.
    pub fn save_instance_id(&self, instance_id: &String) {
        self.db
            .put(
                &self.create_prefix_index(INSTANCE_NAME_PREFIX, instance_id),
                &self.empty_val,
            )
            .map_err(|e| {
                format!(
                    "error while saving service name {:?}. err_msg {:?}",
                    instance_id, e
                )
            })
            .unwrap();
    }

    /// save_operation_name saves operation name.
    pub fn save_operation_name(&self, operation_name: &String) {
        self.db
            .put(
                &self.create_prefix_index(OPERATION_NAME_PREFIX, operation_name),
                &self.empty_val,
            )
            .map_err(|e| {
                format!(
                    "error while saving operation name {:?}. err_msg {:?}",
                    operation_name, e
                )
            })
            .unwrap();
    }

    /// create_prefix_index creates index by prefixing the prefix key.
    fn create_prefix_index(&self, prefix: &str, val: &String) -> Vec<u8> {
        format!("{}-{}", prefix, val).into_bytes()
    }
}
