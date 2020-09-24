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
use crate::memtable::memtable::MemTable;
use crate::proto::trace::Span;
use crate::table::builder::TableBuilder;
use crate::utils::placement::{set_for_current, CoreId};
use crossbeam_channel::Receiver;
use iou::IoUring;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::IoSlice;
use std::mem;
use std::os::unix::io::AsRawFd;
use std::path;
const TABLE_USER_DATA: u64 = 0xDEADBEEF;
use log::{debug, info};
use std::fs;
use std::thread;
use std::thread::JoinHandle;
/// Ingester is used for ingesting spans from the collector. It's responsible for building memtable
/// and flushing to the disk when the memtable is filled.
pub struct Ingester {
    /// in-memory memtable. used for storing all the spans in sorted order.
    memtable: MemTable,
    /// next_table_id tells the next id for the table.
    next_table_id: u64,
    /// shard_path represents the path where we can store all the table file.
    shard_path: path::PathBuf,
    /// iou is the wrapper around io_uring. It helps us to write the table without event blocking
    /// the thread.
    iou: IoUring,
    /// submitted_builder_buffer is used hold the reference of the buffer that we submitted to the
    /// io_uring. we can reuse it for building table again.
    submitted_builder_buffer: Option<Buffer>,
    /// builder_index_store is the index store for table building. It's a freelist it's meant
    /// to be reused by the table builder.
    builder_index_store: Option<BTreeMap<String, Vec<usize>>>,
    shard_id: usize,
    current_span_count: usize,
}

impl Ingester {
    /// new returns new ingester. which helps to ingest data to the disk.
    pub fn new(shard_path: path::PathBuf, last_table_id: u64, shard_id: usize) -> Ingester {
        Ingester {
            memtable: MemTable::new(),
            next_table_id: last_table_id + 1,
            iou: IoUring::new(20).expect("unable to create iouring for the ingester"),
            submitted_builder_buffer: None,
            builder_index_store: Some(BTreeMap::default()),
            current_span_count: 0,
            shard_path,
            shard_id,
        }
    }

    /// write_spans is used to write span to the memtable.
    pub fn write_spans(&mut self, spans: Vec<Span>) {
        // Our memtable is full. So, let's flush this memtable before getting new
        // writes. This is actually blocking this thread. We should create some scheduling mechanism
        // to do. Typically, our own scheduler. Idea is not to share memory across the thread.
        // Anyways for the blocking calls, we are using io_uring.
        if self.memtable.span_size() >= 10 << 20 {
            self.flush_memtable();
        }
        self.current_span_count = self.current_span_count + &spans.len();
        if self.current_span_count % 50 == 0 {
            debug!(
                "{} spans inserted for the memtable {} in shard {}",
                &self.current_span_count, self.next_table_id, &self.shard_id
            );
        }
        // Write all the incoming span to the memtable.
        for span in spans {
            self.memtable.put_span(span)
        }
    }

    /// flush_memtable flushes the sorted memtable to the disk.
    fn flush_memtable(&mut self) {
        // we'll always have index store let's just unwrap
        let index_store = mem::replace(&mut self.builder_index_store, None).unwrap();
        let buffer: Buffer;
        // Verify whether existing buffer is submitted to the iouring queue. If it's already
        // submitted let's just wait for that submitted process to complete to reuse the existing
        // buffer. Generally, we'll get the buffer as soon as we request for. Because, this
        // job is the submitted by us long back.
        if self.submitted_builder_buffer.is_some() {
            // Looks like we submitted a event. Let's just wait for the buffer to get free
            // to reuse it.
            buffer = mem::replace(&mut self.submitted_builder_buffer, None).unwrap();
            let mut cq = self.iou.cq();
            let cqe = cq
                .wait_for_cqe()
                .expect("error while waiting on completion queue");
            assert_eq!(cqe.user_data(), TABLE_USER_DATA);
            assert_eq!(buffer.size(), cqe.result().unwrap())
        } else {
            // We're building the table for the first time. Let's just create a buffer.
            buffer = Buffer::with_size(10 << 20)
        }
        // Build the sorted table for the current memtable.
        let mut builder = TableBuilder::build_with(buffer, index_store);
        for (trace_id, spans, indices) in self.memtable.iter() {
            builder.add_trace(trace_id, spans, indices);
        }
        // Flush the sorted table to the disk.
        let (builder_buffer, index_store) = builder.finish();
        let table_file = self.get_next_table_file();
        // Submit as async event. No need to wait now. We can continue writing spans to the
        // current memtable.
        unsafe {
            let mut sq = self.iou.sq();
            let mut sqe = sq
                .next_sqe()
                .expect("unable to get the next submission queue entry");
            let slice = [IoSlice::new(builder_buffer.bytes_ref())];
            sqe.prep_write_vectored(table_file.as_raw_fd(), &slice, 0);
            sqe.set_user_data(TABLE_USER_DATA);
            self.iou
                .sq()
                .submit()
                .expect("unable to submit submission entry");
            debug!(
                "flushing memtable {} with spans {} for the shard {}.",
                &self.next_table_id - 1,
                &self.current_span_count,
                &self.shard_id
            );
            self.current_span_count = 0;
        }
        // Hold the reference of submitted buffer.
        self.submitted_builder_buffer = Some(builder_buffer);
        // Hold the index store as well because. We can use it for the next time table building.
        self.builder_index_store = Some(index_store);
        self.memtable.clear();
    }

    /// get_next_table_file creates next file for the memtable to be flushed.
    /// Need to be carefully read when the server restarting. Because, we might have created the
    /// file and crashed before writing.
    fn get_next_table_file(&mut self) -> File {
        let table_path = self
            .shard_path
            .join(format!("{:?}.table", self.next_table_id));
        // Update the id for the next segment file.
        self.next_table_id = self.next_table_id + 1;
        File::create(&table_path).expect(&format!("unable to create segment file {:?}", table_path))
    }

    /// start starts the ingester and receive all the spans from the server and
    /// start writing to the memtable.
    pub fn start(mut self, recv: Receiver<Vec<Span>>, core_id: CoreId) -> JoinHandle<()> {
        thread::spawn(move || {
            info!("starting ingester for the cpu {:?}", &core_id);
            // bind the thead to the given cpu.
            set_for_current(core_id);
            // keep reading the spans and write it to the memtable.
            loop {
                let spans = recv.recv().unwrap();
                self.write_spans(spans);
            }
        })
    }
}
