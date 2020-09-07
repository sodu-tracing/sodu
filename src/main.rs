#![allow(dead_code)]
#![feature(test)]
extern crate test;
mod server;
mod proto;
mod utils;
mod trace_writer;
fn main() {
    // Initialize all the global helper utils.
    utils::utils::init_all_utils();
    // Start the grpc server.
    server::grpc::start_server();
}
