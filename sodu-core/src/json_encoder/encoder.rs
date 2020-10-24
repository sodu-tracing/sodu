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

use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_reader::BufferReader;
use crate::encoder::span::{
    ATTRIBUTE_TYPE, BOOL_VAL_TYPE, DOUBLE_VAL_TYPE, EVENT_TYPE, INT_VAL_TYPE, LINK_TYPE,
    PARENT_SPAN_ID_EXIST, STRING_VAL_TYPE,
};
use crate::proto::service::InternalTrace;
use crate::utils::utils::hash_bytes;
use std::convert::TryInto;
use std::fmt::Display;
use std::{i64, u64};

/// encode_traces convert the interanal traces to json format.
pub fn encode_traces(buf: &mut Buffer, traces: Vec<InternalTrace>) {
    buf.write_byte(b'{');
    buf.write_raw_slice(r#""traces":["#.as_bytes());
    for idx in 0..traces.len() {
        encode_trace(buf, &traces[idx].get_trace()[..]);
        if idx + 1 < traces.len() {
            buf.write_byte(b',');
        }
    }
    buf.write_byte(b']');
    buf.write_byte(b'}');
}
/// encode_trace encodes the trace into json structure.
pub fn encode_trace(buf: &mut Buffer, trace: &[u8]) {
    buf.write_byte(b'{');
    // write trace id.
    buf.write_raw_slice(r#""trace_id":"#.as_bytes());
    buf.write_raw_slice(format!("{:?},", hash_bytes(&trace[..16])).as_bytes());
    encode_spans(buf, &trace[16..]);
    buf.write_byte(b'}');
}

/// encode_spans encodes the spans into json structure.
fn encode_spans(buf: &mut Buffer, spans: &[u8]) {
    buf.write_raw_slice(r#""spans":["#.as_bytes());
    let mut reader = BufferReader::new(spans);
    loop {
        encode_span(buf, &mut reader);
        if let Some(_) = reader.peek_byte() {
            buf.write_byte(b',');
            continue;
        }
        break;
    }
    buf.write_byte(b']');
}

/// encode_span encodes the span into json structure.
fn encode_span(buf: &mut Buffer, reader: &mut BufferReader) {
    buf.write_byte(b'{');
    let span_id = reader.read_exact_length(16).unwrap();
    // write span id.
    buf.write_raw_slice(r#""span_id":"#.as_bytes());
    buf.write_raw_slice(format!("{:?},", hash_bytes(span_id)).as_bytes());
    // write parent span id if it's exist.
    if reader.read_byte().unwrap() == PARENT_SPAN_ID_EXIST {
        buf.write_raw_slice(r#""parent_span_id":"#.as_bytes());
        let parent_span_id = reader.read_exact_length(16).unwrap();
        buf.write_raw_slice(format!("\"{:?}\",", hash_bytes(parent_span_id)).as_bytes());
    }
    // write read and end timestamp.
    buf.write_raw_slice(r#""start_unix_nano":"#.as_bytes());
    let start_ts_buf = reader.read_exact_length(8).unwrap();
    buf.write_raw_slice(
        format!(
            "{:?},",
            u64::from_be_bytes(start_ts_buf.try_into().unwrap())
        )
        .as_bytes(),
    );
    buf.write_raw_slice(r#""end_unix_nano":"#.as_bytes());
    let end_ts_buf = reader.read_exact_length(8).unwrap();
    buf.write_raw_slice(
        format!("{:?},", u64::from_be_bytes(end_ts_buf.try_into().unwrap())).as_bytes(),
    );
    // write span kind.
    buf.write_raw_slice(r#""span_kind":"#.as_bytes());
    buf.write_raw_slice(format!("{:?},", reader.read_byte().unwrap()).as_bytes());
    // write span name.
    buf.write_raw_slice(r#""name":"#.as_bytes());
    let name = String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
    buf.write_raw_slice(format!("{:?},", name).as_bytes());
    // write attributes
    buf.write_raw_slice(r#""attributes":["#.as_bytes());
    encode_attribute(buf, reader, false);
    buf.write_byte(b']');
    buf.write_byte(b',');
    // write events
    buf.write_raw_slice(r#""events":["#.as_bytes());
    encode_event(buf, reader, false);
    buf.write_byte(b']');
    buf.write_byte(b',');
    buf.write_raw_slice(r#""links":["#.as_bytes());
    encode_link(buf, reader, false);
    buf.write_byte(b']');
    buf.write_byte(b'}');
}

/// encode_span encodes the link into json structure.
fn encode_link(buf: &mut Buffer, reader: &mut BufferReader, prev: bool) {
    reader.peek_byte().map(|meta| {
        if meta != LINK_TYPE {
            return;
        }
        if prev {
            buf.write_byte(b',');
        }
        reader.consume(1).unwrap();
        buf.write_byte(b'{');
        let trace_id = reader.read_exact_length(16).unwrap();
        buf.write_raw_slice(format!(r#""trace_id":{:?},"#, hash_bytes(trace_id)).as_bytes());
        let span_id = reader.read_exact_length(16).unwrap();
        buf.write_raw_slice(format!(r#""span_id":{:?},"#, hash_bytes(span_id)).as_bytes());
        let trace_state =
            String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
        buf.write_raw_slice(format!(r#""trace_state":{:?},"#, trace_state).as_bytes());
        buf.write_raw_slice(r#""attributes":["#.as_bytes());
        encode_attribute(buf, reader, false);
        buf.write_byte(b']');
        buf.write_byte(b'}');
        encode_link(buf, reader, true);
    });
}

/// encode_span encodes the event into json structure.
fn encode_event(buf: &mut Buffer, reader: &mut BufferReader, prev: bool) {
    reader.peek_byte().map(|meta| {
        if meta != EVENT_TYPE {
            return;
        }
        reader.consume(1).unwrap();
        if prev {
            buf.write_byte(b',');
        }
        buf.write_byte(b'{');
        // write time.
        buf.write_raw_slice(r#""time_unix_nano":"#.as_bytes());
        let ts_buf = reader.read_exact_length(8).unwrap();
        buf.write_raw_slice(
            format!("{:?},", u64::from_be_bytes(ts_buf.try_into().unwrap())).as_bytes(),
        );
        // write span name.
        buf.write_raw_slice(r#""name":"#.as_bytes());
        let name = String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
        buf.write_raw_slice(format!("{:?},", name).as_bytes());
        // write attributes.
        buf.write_raw_slice(r#""attributes":["#.as_bytes());
        encode_attribute(buf, reader, false);
        buf.write_byte(b']');
        buf.write_byte(b'}');
        encode_event(buf, reader, true);
    });
}

/// encode_span attribute the span into json structure.
fn encode_attribute(buf: &mut Buffer, reader: &mut BufferReader, prev: bool) {
    reader.peek_byte().map(|meta| {
        if meta != ATTRIBUTE_TYPE {
            return;
        }
        if prev {
            // previous attribute was writter so put a comma before writing
            // the current attribute.
            buf.write_byte(b',');
        }
        reader.consume(1).unwrap();
        match reader.read_byte().unwrap() {
            BOOL_VAL_TYPE => {
                let key =
                    String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
                let mut val = false;
                if reader.read_byte().unwrap() == 1 {
                    val = true;
                }
                encode_kv(buf, key, val);
            }
            STRING_VAL_TYPE => {
                let key =
                    String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
                let val =
                    String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
                encode_kv(buf, key, val);
            }
            INT_VAL_TYPE => {
                let key =
                    String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
                let val =
                    i64::from_be_bytes(reader.read_slice().unwrap().unwrap().try_into().unwrap());
                encode_kv(buf, key, val);
            }
            DOUBLE_VAL_TYPE => {
                let key =
                    String::from_utf8_lossy(reader.read_slice().unwrap().unwrap()).to_string();
                let val =
                    i64::from_be_bytes(reader.read_slice().unwrap().unwrap().try_into().unwrap());
                encode_kv(buf, key, val);
            }
            _ => {
                unreachable!("yolo yada what did you do wrong tiger");
            }
        }
        encode_attribute(buf, reader, true);
    });
}

fn encode_kv<K: Display, V: Display>(buf: &mut Buffer, key: K, val: V) {
    buf.write_byte(b'{');
    buf.write_raw_slice(format!(r#""key": "{:}", "value":"{:}""#, key, val).as_bytes());
    buf.write_byte(b'}');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer::Buffer;
    use crate::encoder::span::encode_span;
    use crate::proto::common::{AnyValue, AnyValue_oneof_value, KeyValue};
    use crate::proto::trace::{Span, Span_Event, Span_Link};
    use crate::utils::utils::spans_to_trace;
    use protobuf::{RepeatedField, SingularPtrField};

    #[test]
    fn test_json_encoding() {
        let mut span = Span::default();
        span.trace_id = vec![0; 16];
        span.span_id = vec![0; 16];
        span.start_time_unix_nano = 200;
        span.end_time_unix_nano = 203;
        span.name = String::from("hello");
        let mut kv = KeyValue::default();
        kv.key = String::from("string kv");
        let mut val = AnyValue::default();
        val.value = Some(AnyValue_oneof_value::string_value(String::from(
            "let's make it right",
        )));
        kv.value = SingularPtrField::from(Some(val));
        span.attributes = RepeatedField::from(vec![kv.clone()]);
        let mut event = Span_Event::default();
        event.name = String::from("log 1");
        event.time_unix_nano = 100;
        event.attributes = RepeatedField::from(vec![kv.clone()]);
        let mut events = vec![event.clone()];
        event.name = String::from("log2");
        events.push(event);
        span.events = RepeatedField::from(events);
        let mut link = Span_Link::default();
        link.attributes = RepeatedField::from(vec![kv.clone()]);
        link.span_id = vec![0; 16];
        link.trace_id = vec![0; 16];
        span.links = RepeatedField::from(vec![link]);
        let mut buffer = Buffer::with_size(2 << 20);
        encode_span(&span, &mut buffer);
        let mut buffer_2 = Buffer::with_size(2 << 20);
        encode_trace(&mut buffer_2, buffer.bytes_ref());
        assert_eq!(
            String::from_utf8_lossy(buffer_2.bytes_ref()).to_string(),
            r#"{"trace_id":5506010466157640574,"spans":[{"span_id":5506010466157640574,"start_unix_nano":200,"end_unix_nano":203,"span_kind":0,"name":"hello","attributes":[{"key": "string kv", "value":"let's make it right"}],"events":[{"time_unix_nano":100,"name":"log 1","attributes":[{"key": "string kv", "value":"let's make it right"}]},{"time_unix_nano":100,"name":"log2","attributes":[{"key": "string kv", "value":"let's make it right"}]}],"links":[{"trace_id":5506010466157640574,"span_id":5506010466157640574,"trace_state":"","attributes":[{"key": "string kv", "value":"let's make it right"}]}]}]}"#
        )
    }

    #[test]
    fn test_trace_encoding() {
        let mut span = Span::default();
        span.trace_id = vec![0; 16];
        span.span_id = vec![0; 16];
        span.start_time_unix_nano = 200;
        span.end_time_unix_nano = 203;
        span.name = String::from("hello");
        let mut kv = KeyValue::default();
        kv.key = String::from("string kv");
        let mut val = AnyValue::default();
        val.value = Some(AnyValue_oneof_value::string_value(String::from(
            "let's make it right",
        )));
        kv.value = SingularPtrField::from(Some(val));
        span.attributes = RepeatedField::from(vec![kv.clone()]);
        let mut event = Span_Event::default();
        event.name = String::from("log 1");
        event.time_unix_nano = 100;
        event.attributes = RepeatedField::from(vec![kv.clone()]);
        let mut events = vec![event.clone()];
        event.name = String::from("log2");
        events.push(event);
        span.events = RepeatedField::from(events);
        let mut link = Span_Link::default();
        link.attributes = RepeatedField::from(vec![kv.clone()]);
        link.span_id = vec![0; 16];
        link.trace_id = vec![0; 16];
        span.links = RepeatedField::from(vec![link]);
        let mut buffer = Buffer::with_size(2 << 20);
        encode_span(&span, &mut buffer);
        let mut trace = Vec::new();
        trace.push(buffer.bytes_ref());
        let mut buffer_2 = Buffer::with_size(2 << 20);
        encode_span(&span, &mut buffer_2);
        trace.push(buffer_2.bytes_ref());
        let encoded_trace = spans_to_trace(trace);
        let mut json_buffer = Buffer::with_size(2 << 20);
        encode_trace(&mut json_buffer, &encoded_trace[..]);
    }
}
