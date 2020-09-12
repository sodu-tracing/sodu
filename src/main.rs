#![allow(dead_code)]
#![feature(test)]
extern crate test;
mod buffer;
mod encoder;
mod memtable;
mod proto;
mod server;
mod table;
mod utils;

fn main() {
    // Initialize all the global helper utils.
    utils::utils::init_all_utils();
    // Start the grpc server.
    server::grpc::start_server();
}
