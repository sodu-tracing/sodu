use futures::channel::oneshot;
use futures::executor::block_on;
use futures::prelude::*;
use grpcio::{ChannelBuilder, Environment, ResourceQuota, RpcContext, ServerBuilder, UnarySink};
use log::info;
use sodu_core::proto::service::{QueryRequest, QueryResponse, TagRequest, TagResponse};
use sodu_core::proto::service_grpc::{create_sodu_storage, SoduStorage};
use sodu_core::query_executor::executor::QueryExecutor;
use std::sync::Arc;
/// StorageServer is the grpc server which is used for executing the sodu internal query on
/// sodu storage node.
#[derive(Clone)]
struct StorageServer {
    /// query_executor is used to execute query on the storage node.
    query_executor: QueryExecutor,
}

impl SoduStorage for StorageServer {
    /// query_trace excutes internal query request.
    fn query_trace(&mut self, ctx: RpcContext, req: QueryRequest, sink: UnarySink<QueryResponse>) {
        let res = self.query_executor.query(req);
        let f = sink.success(res).map(|_| ());
        ctx.spawn(f);
    }

    /// get_tags returns all the tags.
    fn get_tags(&mut self, ctx: RpcContext, _: TagRequest, sink: UnarySink<TagResponse>) {
        let res = self.query_executor.get_tags();
        let f = sink.success(res).map(|_| ());
        ctx.spawn(f);
    }
}

/// start_storage_server starts the storage server.
pub fn start_storage_server(query_executor: QueryExecutor) {
    let env = Arc::new(Environment::new(1));
    let service = create_sodu_storage(StorageServer { query_executor });
    let quota = ResourceQuota::new(None).resize_memory(100 << 20);
    let ch_builder = ChannelBuilder::new(env.clone()).set_resource_quota(quota);

    info!("starting sodu storage");
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 50_052)
        .channel_args(ch_builder.build_args())
        .build()
        .unwrap();
    server.start();
    for (host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }
    let (_tx, rx) = oneshot::channel::<()>();
    let _ = block_on(rx);
}
