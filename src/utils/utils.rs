use flexi_logger::Logger;
use std::fmt::Display;

pub fn init_all_utils() {
    Logger::with_env_or_str("info").start().unwrap();
}

/// create_index_key creates index key. It's used as a primary key to store posting list.
pub fn create_index_key<T: Display>(k: &String, v: T) -> String {
    format!("{}-{}", k, v)
}
