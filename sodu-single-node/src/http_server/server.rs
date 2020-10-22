use async_std::task;
use serde::{Deserialize, Serialize};
use sodu_core::buffer::buffer::Buffer;
use sodu_core::json_encoder::encoder::encode_traces;
use sodu_core::proto::service::{QueryRequest, TimeRange};
use sodu_core::query_executor::executor::QueryExecutor;
use std::collections::HashMap;
use std::default::Default;
use tide::http::mime::Mime;
use tide::prelude::*;
use tide::{Request, Response};

/// SoduState contains the requrired paramenter to execute user query.
/// It is used to pass it around the tide framework.
#[derive(Clone)]
struct SoduState {
    /// executor is the sodu query executor.
    executor: QueryExecutor,
}

/// TagResponse is the https reponse for get tag request.
#[derive(Deserialize, Serialize, Debug)]
struct TagResponse {
    /// operation_names contains all the operation that this sodu-instance serves.
    operation_names: Vec<String>,
    /// instance_names contains a;l the instance name that this sodu-instance serves.
    instance_names: Vec<String>,
    /// services_names contains all the service name that this sodu-instance serves.
    services_names: Vec<String>,
}

/// HttpQueryRequest is the json represention of the query request.
#[derive(Deserialize, Serialize, Debug)]
struct HttpQueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// service_name is the microservice name.
    service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// operation_name is the span name.
    operation_name: Option<String>,
    /// tags to filter on.
    tags: HashMap<String, String>,
    /// start_ts is the start time of the time range to filter on.
    start_ts: u64,
    /// end_ts is the end time of the the time range to filter on.
    end_ts: u64,
}

/// start_http_server start http server to serve user quries.
pub fn start_http_server(executor: QueryExecutor) {
    let mut app = tide::with_state(SoduState { executor });
    app.at("/query")
        .post(|mut req: Request<SoduState>| async move {
            let executor = &req.state().executor.clone();
            let req: HttpQueryRequest = req.body_json().await?;
            // covert http request to protobuf request. Ideally, we should
            // have to convert json directly to protobuf request.
            let mut executor_req = QueryRequest::default();
            if let Some(service_name) = req.service_name {
                executor_req.set_service_name(service_name);
            }
            if let Some(operation_name) = req.operation_name {
                executor_req.set_operation_name(operation_name);
            }
            executor_req.tags = req.tags;
            let range = TimeRange {
                min_start_ts: Some(req.start_ts),
                max_start_ts: Some(req.end_ts),
                ..Default::default()
            };
            executor_req.set_time_range(range);
            // excute the given query.
            let res = executor.query(executor_req);
            // convert the response to json format.
            let mut buffer = Buffer::with_size(1000);
            encode_traces(&mut buffer, res.traces.into_vec());
            let mut res = Response::new(200);
            res.set_content_type(Mime::from("application/json"));
            res.set_body(buffer.bytes());
            Ok(res)
        });
    task::block_on(async move { app.listen("127.0.0.1:8080").await.unwrap() });
}
