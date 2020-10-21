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
use parking_lot::Mutex;
use rocksdb::{DBOptions, Writable, DB};
use std::collections::HashSet;
use std::convert::TryInto;
use std::default::Default;
use std::u64;

/// WAL_CHECK_POINT is the wal check point key. This gives us the offset where we have to replay
/// the wal.
const WAL_CHECK_POINT: &[u8] = b"!sodu!pathukapu!ellai!";
/// SoduMetaStore is used to store meta data releated to sodu-storage.
pub struct SoduMetaStore {
    /// db is the rocks db instance.
    db: DB,
    /// services is the list of services where we have ingested traces from.
    services: Mutex<HashSet<String>>,
    /// operations is the list of operations where we have ingested spans from.
    operations: Mutex<HashSet<String>>,
}

impl SoduMetaStore {
    /// new returns SoduMetaStore.
    pub fn new(opt: &Options) -> SoduMetaStore {
        let db = DB::open(
            DBOptions::default(),
            &opt.meta_store_path
                .clone()
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .unwrap();
        SoduMetaStore {
            db,
            services: Mutex::new(HashSet::default()),
            operations: Mutex::new(HashSet::default()),
        }
    }

    /// save_wal_check_point saves wal checkpoint.
    pub fn save_wal_check_point(&mut self, wal_id: u64, wal_offset: u64) {
        // encode wal id and wal offset.
        let mut buffer = Buffer::with_size(16);
        buffer.write_raw_slice(&wal_id.to_be_bytes());
        buffer.write_raw_slice(&wal_offset.to_be_bytes());
        self.db
            .put(WAL_CHECK_POINT, buffer.bytes_ref())
            .map_err(|e| {
                format!(
                    "error while saving checkpoint for wal id {:?} and wal offset {:?}. err_msg: {:?}",
                    wal_id, wal_offset,e
                )
            })
            .unwrap();
    }

    /// get_wal_check_point retrives wal check point.
    pub fn get_wal_check_point(&mut self) -> Option<WalCheckPoint> {
        let check_point = self
            .db
            .get(WAL_CHECK_POINT)
            .map_err(|e| format!("error while retriving wal check point. err_msg: {:?}", e))
            .unwrap();
        // return the checkpoint if we persisted checkpoint.
        if let Some(val) = check_point {
            // always check point should be of length of 16.
            assert_eq!(val.len(), 16);
            let wal_id = u64::from_be_bytes(val[..8].try_into().unwrap());
            let wal_offset = u64::from_be_bytes(val[8..].try_into().unwrap());
            return Some(WalCheckPoint { wal_id, wal_offset });
        }
        None
    }
}
