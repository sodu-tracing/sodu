use std::cmp::Ordering;
use std::collections::HashSet;

/// SpanPointer is used to navigate the index of the the given span in the
/// the vector array. It's used only in the memtable.
pub struct SpanPointer {
    // Trace id of the span.
    pub trace_id: Vec<u8>,
    // Start time stamp of the span.
    pub start_ts: u64,
    // Index of the span.
    pub index: usize,
    pub indices: HashSet<String>,
}

impl Ord for SpanPointer {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.trace_id.cmp(&other.trace_id);
        match cmp {
            Ordering::Equal => self.start_ts.cmp(&other.start_ts),
            _ => cmp,
        }
    }
}

impl Eq for SpanPointer {}

impl PartialOrd for SpanPointer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SpanPointer {
    fn eq(&self, other: &Self) -> bool {
        if self.trace_id != other.trace_id {
            return false;
        }
        return self.start_ts == other.start_ts;
    }
}
