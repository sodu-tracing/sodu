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
use crate::ingester::coordinator::IngesterCoordinator;
use crate::proto::trace_service::{ExportTraceServiceRequest, ExportTraceServiceResponse};
use crate::proto::trace_service_grpc::{create_trace_service, TraceService};
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::prelude::*;
use grpcio::{ChannelBuilder, Environment, ResourceQuota, RpcContext, ServerBuilder, UnarySink};
use log::{error, info};
use std::sync::Arc;
/// GrpcServer is the grpc server which collects opentelemetry traces
/// from the collector. It is implemented according to the opentelemetry spec.
#[derive(Clone)]
struct OpenTelemetryExportServer {
    ingester_coordinator: IngesterCoordinator,
}

impl TraceService for OpenTelemetryExportServer {
    /// export save the incoming request traces which is exported by the opentelemetry collector.
    /// into our storage.
    fn export(
        &mut self,
        ctx: RpcContext,
        req: ExportTraceServiceRequest,
        sink: UnarySink<ExportTraceServiceResponse>,
    ) {
        self.ingester_coordinator
            .send_spans(req.resource_spans.into_vec());
        let f = sink
            .success(ExportTraceServiceResponse::default())
            .map_err(move |e| error!("failed to reply {:?}", e))
            .map(|_| ());
        ctx.spawn(f);
    }
}

/// start_server starts the export server.
pub fn start_server(ingester_coordinator: IngesterCoordinator) {
    info!("starting grpc server");
    let env = Arc::new(Environment::new(1));
    let service = create_trace_service(OpenTelemetryExportServer {
        ingester_coordinator,
    });
    let quota = ResourceQuota::new(None).resize_memory(100 <<20);
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
