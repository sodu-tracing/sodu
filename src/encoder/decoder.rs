use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct InplaceSpanDecoder<'a>(pub &'a [u8]);

impl<'a> InplaceSpanDecoder<'a> {
    pub fn decode_trace_id(&self) -> &[u8] {
        &self.0[..16]
    }

    /// hashed_trace_id returns the hash of trace_id.
    pub fn hashed_trace_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        &self.0[..16].hash(&mut hasher);
        hasher.finish()
    }
}
