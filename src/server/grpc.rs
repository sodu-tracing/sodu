use crate::proto::trace_service::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use crate::proto::trace_service_grpc::{create_trace_service, TraceService};
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::prelude::*;
use grpcio::{ChannelBuilder, Environment, ResourceQuota, RpcContext, ServerBuilder, UnarySink};
use log::{error, info};
use protobuf::Message;
use std::sync::Arc;
/// GrpcServer is the grpc server which collects opentelementry traces
/// from the collector. It is implemented according to the opentelemetry spec.
#[derive(Clone)]
struct OpenTelementryExportServer {}

impl TraceService for OpenTelementryExportServer {
    /// export save the incoming request traces which is exported by the opentelementry collector.
    /// into our storage.
    fn export(
        &mut self,
        ctx: RpcContext,
        req: ExportTraceServiceRequest,
        sink: UnarySink<ExportTraceServiceResponse>,
    ) {
        println!("batch len {:?}", req.get_resource_spans().len());
        println!("protobuf size {:?}", req.compute_size());
        let f = sink
            .success(ExportTraceServiceResponse::default())
            .map_err(move |e| error!("failed to reply {:?}: {:?}", req, e))
            .map(|_| ());
        ctx.spawn(f);
    }
}

/// start_server starts the export server.
pub fn start_server() {
    let env = Arc::new(Environment::new(1));
    let service = create_trace_service(OpenTelementryExportServer {});
    let quota = ResourceQuota::new(None).resize_memory(1024 * 1024);
    let ch_builder = ChannelBuilder::new(env.clone()).set_resource_quota(quota);

    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 50_051)
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
