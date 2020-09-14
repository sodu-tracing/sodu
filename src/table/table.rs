use std::fs::File;
use std::io::SeekFrom;
use std::io::{Read, Seek};
use std::u32;
use std::convert::TryInto;
use unsigned_varint::decode;
use rand::AsByteSliceMut;

pub struct Table {
    file: File,
    pub indices: Vec<(String, Vec<u32>)>,
    file_length: usize,
}

impl Table {
    pub fn from_file(mut file: File) -> Table {
        let file_length = file.metadata().unwrap().len() as usize;
        // let's read the last 4 bytes to read the
        file.seek(SeekFrom::Start((file_length - 4) as u64)).unwrap();
        let mut buf: [u8; 4] = [0; 4];
        file.read_exact(&mut buf).unwrap();
        let index_offset = u32::from_be_bytes(buf);
        // Let's read the all the indexing and keep that
        // in memory.
        file.seek(SeekFrom::Start(index_offset as u64)).unwrap();
        // from here onwards read the read the indices one by one till you reach the
        // end of the file.
        let mut table = Table{
            file,
            indices: Vec::default(),
            file_length
        };
        table.read_index(index_offset as usize);
        table
    }

    fn read_index(&mut self, offset: usize){
        // Create buffer for index and posting list.
        let mut main_buf = vec![0; self.file_length-offset-4];
        let mut buf = main_buf.as_mut_slice();
        self.file.read_exact(buf).unwrap();
        // Convert mutable to immutable. Because, we need to assign immutable reference
        // from the decoding library later.
        let mut buf = buf.as_ref();
        // recursively read the buffer.
        loop{
            // break the loop. If no more data to read.
            if buf.len() == 0 {
                break;
            }
            // Get the index key.
            let (key_size, mut rem_buf)=decode::u32(&buf[..]).unwrap();
            buf = rem_buf;
            let key_buf = &buf[..key_size as usize];
            let key = String::from_utf8(key_buf.to_vec()).unwrap();
            // Advance the buffer and read the posting list.
            buf = &buf[key_size as usize..];
            let (posting_size, mut rem_buf) = decode::u32(&buf[..]).unwrap();
            buf = rem_buf;
            let posting_list = decode_posting_list(&buf[..posting_size as usize]);
            // Save the index and advance the buffer.
            self.indices.push((key, posting_list));
            buf = &buf[posting_size as usize..];
        }
        self.file.seek(SeekFrom::Start(0)).unwrap();
    }
}

/// decode_posting_list decodes posting list from the given buffer and returns posting list.
fn decode_posting_list(buf: &[u8]) -> Vec<u32>{
    assert_eq!(buf.len()%4,0);
    let mut list = Vec::with_capacity(buf.len()/4);
    let mut index: usize = 0;
    loop {
        if index == buf.len(){
            break;
        }
        list.push(u32::from_be_bytes(buf[index..index+4].try_into().unwrap()));
        index = index + 4;
    }
    list
}



#[cfg(test)]
pub mod tests {
    use super::*;
    fn test_decode_posting_list(){
        let i: u32 = 23;
        let posting = i.to_be_bytes();
        let mut buffer = Vec::new();
        buffer.extend(&posting);
        buffer.extend(&posting);
        buffer.extend(&posting);
        buffer.extend(&posting);
        buffer.extend(&posting);
        buffer.extend(&posting);
        let posting_list = decode_posting_list(&buffer[..]);
        assert_eq!(posting_list.len(), 6);
        assert_eq!(posting_list[0], 23);
    }

}