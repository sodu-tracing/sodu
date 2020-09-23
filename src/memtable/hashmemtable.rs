use crate::buffer::buffer::Buffer;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub struct OffsetPtr {
    offsets: Vec<u8>,
    start_ts: u64,
}

pub struct HashMemtable {
    valueStore: Buffer,
    pub valuePtrStore: HashMap<u64, OffsetPtr>,
}

impl HashMemtable {
    fn new() -> HashMemtable {
        HashMemtable {
            valueStore: Buffer::with_size(64 << 20),
            valuePtrStore: HashMap::default(),
        }
    }

    fn put_span(&mut self, trace_id: Vec<u8>, span: &[u8], start_ts: u64) {
        let mut hasher = DefaultHasher::new();
        trace_id.hash(&mut hasher);
        let hash_id = hasher.finish();
        let offset = self.valueStore.write_slice(span) as u32;
        if let Some(offsetptr) = self.valuePtrStore.get_mut(&hash_id) {
            if offsetptr.start_ts > start_ts {
                offsetptr.start_ts = start_ts;
            }
            offsetptr.offsets.extend_from_slice(&offset.to_be_bytes());
            return;
        }
        self.valuePtrStore.insert(
            hash_id,
            OffsetPtr {
                offsets: offset.to_be_bytes().to_vec(),
                start_ts: start_ts,
            },
        );
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use rand::Rng;
    use std::sync::Mutex;
    use std::time::SystemTime;

    #[test]
    fn test_time_calculation() {
        let mut memtable = HashMemtable::new();
        let start_time = SystemTime::now();
        let locked = Mutex::new(memtable);
        for i in 0..64_000 {
            let trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
            let buf = vec![0; 1000];
            let mut table = locked.lock().unwrap();
            table.put_span(trace_id, &buf[..], i);
        }
        let table = locked.lock().unwrap();
        let mut items: Vec<_> = table.valuePtrStore.iter().collect();
        items.sort_by(|a, b| b.1.start_ts.cmp(&a.1.start_ts));
        println!("elapsed {}", start_time.elapsed().unwrap().as_millis());
    }
}
