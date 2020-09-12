use std::fs::File;

pub struct Table {
    file: File,
    indices: Vec<(String, Vec<u8>)>,
}

impl Table {
    fn from_file(mut file: File) {}
}
