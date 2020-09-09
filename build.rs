extern crate protoc_grpcio;

use protobuf_codegen::Customize;
use protoc_grpcio::compile_grpc_protos;

fn main() {
    compile_grpc_protos(
        &[
            "opentelemetry-proto/opentelemetry/proto/common/v1/common.proto",
            "opentelemetry-proto/opentelemetry/proto/resource/v1/resource.proto",
            "opentelemetry-proto/opentelemetry/proto/trace/v1/trace.proto",
            "opentelemetry-proto/opentelemetry/proto/trace/v1/trace_config.proto",
            "opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
            "opentelemetry-proto/opentelemetry/proto/metrics/v1/metrics.proto",
            "opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
        ],
        &["opentelemetry-proto/"],
        "src/proto",
        Some(Customize {
            expose_fields: Some(true),
            serde_derive: Some(true),
            ..Default::default()
        }),
    )
    .expect("Error generating protobuf");
}
