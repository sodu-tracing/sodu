// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_SODU_STORAGE_QUERY_TRACE: ::grpcio::Method<super::service::QueryRequest, super::service::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/SoduStorage/QueryTrace",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct SoduStorageClient {
    client: ::grpcio::Client,
}

impl SoduStorageClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        SoduStorageClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn query_trace_opt(&self, req: &super::service::QueryRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::service::QueryResponse> {
        self.client.unary_call(&METHOD_SODU_STORAGE_QUERY_TRACE, req, opt)
    }

    pub fn query_trace(&self, req: &super::service::QueryRequest) -> ::grpcio::Result<super::service::QueryResponse> {
        self.query_trace_opt(req, ::grpcio::CallOption::default())
    }

    pub fn query_trace_async_opt(&self, req: &super::service::QueryRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::service::QueryResponse>> {
        self.client.unary_call_async(&METHOD_SODU_STORAGE_QUERY_TRACE, req, opt)
    }

    pub fn query_trace_async(&self, req: &super::service::QueryRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::service::QueryResponse>> {
        self.query_trace_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Output = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait SoduStorage {
    fn query_trace(&mut self, ctx: ::grpcio::RpcContext, req: super::service::QueryRequest, sink: ::grpcio::UnarySink<super::service::QueryResponse>);
}

pub fn create_sodu_storage<S: SoduStorage + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_unary_handler(&METHOD_SODU_STORAGE_QUERY_TRACE, move |ctx, req, resp| {
        instance.query_trace(ctx, req, resp)
    });
    builder.build()
}
