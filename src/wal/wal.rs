use crate::buffer::buffer::Buffer;
use crate::encoder::span::encode_span;
use crate::options::options::Options;
use crate::proto::trace::Span;
use crate::utils::utils::{get_file_ids, read_files_in_dir};
use anyhow::Context;
use anyhow::Result;
use futures::io::IoSlice;
use iou::IoUring;
use log::info;
use std::collections::HashSet;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

/// WAL_USER_DATA is the io_uring user data for wal files.
const WAL_USER_DATA: u64 = 10;
/// Wal is used for write all the request in an sequential order in a log files. So, that
/// it can used to recover the spans if the sodu crashes.
pub struct Wal {
    /// last_wal_id is the last created wal file id.
    last_wal_id: u64,
    /// tmp_span_buffer is used to hold all the encoded buffer. It's used to hold the reference of
    /// buffer that goes to the wal file in a io_uring fashion.
    tmp_span_buffer: Buffer,
    /// written_offset is the last persisted wal file offset.
    written_offset: u64,
    /// current_wal_file is the current file where our incoming span goes.
    current_wal_file: File,
    /// iou is the io_uring instance.
    iou: IoUring,
    /// tmp_encoding_buffer. It's used for encoding the span.
    tmp_encoding_buffer: Buffer,
    /// wal_path is the wal file directory.
    wal_path: PathBuf,
    /// pending_io_submission tells that we have submitted block io to io_uring but it's
    /// reclaimed.
    pending_io_submission: bool,
}

pub struct EncodedRequest<'a> {
    pub start_ts: u64,
    pub wal_offset: u64,
    pub encoded_span: &'a [u8],
    pub indices: HashSet<String>,
}

impl Wal {
    /// new return Wal. It is used for writing the incomming spans.
    pub fn new(opt: Options) -> Result<Wal> {
        let wal_files_path = read_files_in_dir(&opt.wal_path, "wal")
            .context(format!("unable to read files at {:?}", &opt.wal_path))
            .unwrap();
        let file_ids = get_file_ids(&wal_files_path);
        // looks like we don't have any wal files. Let's create a new  file and just return it.
        if file_ids.len() == 0 {
            info!("opened wal for the first time so opening new wal file with id 1");
            let wal_file = File::create(opt.wal_path.join("1.wal")).unwrap();
            return Ok(Wal {
                last_wal_id: 1,
                tmp_span_buffer: Buffer::with_size(4 << 20),
                written_offset: 0,
                current_wal_file: wal_file,
                iou: IoUring::new(5).context("unable to open io_uring instance at wal")?,
                tmp_encoding_buffer: Buffer::with_size(2 << 20),
                wal_path: opt.wal_path,
                pending_io_submission: false,
            });
        }
        // Find the last file and decide whether to create a new wal file or you can use the existing
        // file.
        let last_file_id = file_ids[file_ids.len() - 1];
        let wal_file = File::with_options()
            .read(true)
            .write(true)
            .open(opt.wal_path.join(format!("{:?}.wal", last_file_id)))
            .context(format!("unable to open wal file {:?}", last_file_id))?;
        // Check the size.
        let metadata = wal_file.metadata().context(format!(
            "unable to read the metadata of wal file {:?}",
            last_file_id
        ))?;
        if metadata.len() < 1024 << 20 {
            // Last wal file is lesser than 1 gb so let's just use this file itself.
            return Ok(Wal {
                last_wal_id: last_file_id,
                tmp_span_buffer: Buffer::with_size(4 << 20),
                written_offset: metadata.len(),
                current_wal_file: wal_file,
                iou: IoUring::new(5).context("unable to open io_uring instance at wal")?,
                tmp_encoding_buffer: Buffer::with_size(2 << 20),
                wal_path: opt.wal_path,
                pending_io_submission: false,
            });
        }
        // Looks like last file is big. Let's create a new wal file.
        let wal_file =
            File::create(opt.wal_path.join(format!("{:?}.wal", last_file_id + 1))).unwrap();
        return Ok(Wal {
            last_wal_id: last_file_id + 1,
            tmp_span_buffer: Buffer::with_size(4 << 20),
            written_offset: 0,
            current_wal_file: wal_file,
            iou: IoUring::new(5).context("unable to open io_uring instance at wal")?,
            tmp_encoding_buffer: Buffer::with_size(2 << 20),
            wal_path: opt.wal_path,
            pending_io_submission: false,
        });
    }

    /// write_spans writes the given span to the wal file using io_uring. wait_for_submitted_wal_span
    /// needs to called before writing the next batch of spans.
    pub fn write_spans(&mut self, span: Span) -> EncodedRequest {
        self.tmp_encoding_buffer.clear();
        let indices = encode_span(&span, &mut self.tmp_encoding_buffer);
        let offset = self
            .tmp_span_buffer
            .write_slice(self.tmp_encoding_buffer.bytes_ref());
        EncodedRequest {
            start_ts: span.start_time_unix_nano,
            encoded_span: self.tmp_span_buffer.slice_at(offset),
            wal_offset: self.written_offset + offset as u64,
            indices: indices,
        }
    }

    /// submit_buffer_to_iou submits the buffered spans to the wal file.
    pub fn submit_buffer_to_iou(&mut self) {
        self.pending_io_submission = true;
        unsafe {
            let mut sq = self.iou.sq();
            let mut sqe = sq.next_sqe().expect(
                "unable to get next submission
            queue entry for wal",
            );
            let slice = [IoSlice::new(self.tmp_span_buffer.bytes_ref())];
            sqe.prep_write_vectored(
                self.current_wal_file.as_raw_fd(),
                &slice,
                self.written_offset as usize,
            );
            sqe.set_user_data(WAL_USER_DATA);
            self.iou
                .sq()
                .submit()
                .expect("unable to submit entry queue in wal");
        }
    }

    /// wait_for_submitted_wal_span wait for the submmited buffer to go thorough the file.
    pub fn wait_for_submitted_wal_span(&mut self) {
        if !self.pending_io_submission {
            return;
        }
        let mut cq = self.iou.cq();
        let cqe = cq
            .wait_for_cqe()
            .expect("unable to wait for cqe entry at wal");
        assert_eq!(cqe.is_timeout(), false);
        assert_eq!(cqe.user_data(), WAL_USER_DATA);
        self.written_offset += self.tmp_span_buffer.size() as u64;
        self.tmp_span_buffer.clear();
        self.pending_io_submission = false;
    }

    pub fn buffered_size(&self) -> usize {
        self.tmp_span_buffer.size()
    }

    /// current_wal_id returns the current wal file id.
    pub fn current_wal_id(&self) -> u64 {
        self.last_wal_id
    }

    /// change_wal_if_neccessary changes to new wal file. If the current wal file size goes
    /// above 1 GB.
    pub fn change_wal_if_neccessary(&mut self) {
        if self.written_offset < 1024 << 20 {
            return;
        }
        let new_wal_id = self.last_wal_id + 1;
        let file = File::create(self.wal_path.join(format!("{:?}.wal", new_wal_id)))
            .expect("unable to create new wal file on wal refresh");
        self.last_wal_id += 1;
        self.written_offset = 0;
        self.current_wal_file = file;
    }
}
