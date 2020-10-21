use anyhow::Context;
use log::debug;
use parking_lot::Mutex;
use sodu_core::ingester::coordinator::IngesterCoordinator;
use sodu_core::ingester::ingester_runner::IngesterRunner;
use sodu_core::ingester::segment_ingester::SegmentIngester;
use sodu_core::meta_store::sodu_meta_store::SoduMetaStore;
use sodu_core::options::options::Options;
use sodu_core::query_executor::executor::QueryExecutor;
use sodu_core::recovery::recovery_manager::RecoveryManager;
use sodu_core::server::grpc::start_server;
use sodu_core::utils::utils::init_all_utils;
use sodu_core::wal::wal::Wal;
use std::sync::Arc;
mod http_server;
use http_server::server::start_http_server;
use std::thread;

fn main() {
    let opt = Options::init();
    init_all_utils();
    debug!("running in debug mode");
    let meta_store = Arc::new(SoduMetaStore::new(&opt));
    let recovery_mngr = RecoveryManager::new(opt.clone(), meta_store.clone());
    recovery_mngr.repair();
    // Create the ingester instance.
    let ingester = SegmentIngester::new(opt.shard_path.clone(), meta_store.clone());
    let protected_ingester = Arc::new(Mutex::new(ingester));
    // run the the ingester.
    let (coordinator, receiver) = IngesterCoordinator::new();
    let wal = Wal::new(opt.clone())
        .context(format!("error while building wal"))
        .unwrap();
    let runner = IngesterRunner::new(protected_ingester.clone(), wal, meta_store.clone());
    runner.run(receiver);
    // Start the grpc server.
    // Start the grpc server.
    thread::spawn(move || {
        start_server(coordinator);
    });
    // Start http server.
    let executor = QueryExecutor::new(opt.shard_path, protected_ingester);
    start_http_server(executor);
}
