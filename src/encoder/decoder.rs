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
use crate::buffer::buffer_reader::BufferReader;
use crate::encoder::span::{
    ATTRIBUTE_TYPE, BOOL_VAL_TYPE, DOUBLE_VAL_TYPE, EVENT_TYPE, INT_VAL_TYPE, LINK_TYPE,
    PARENT_SPAN_ID_EXIST, STRING_VAL_TYPE,
};
use crate::proto::common::{AnyValue, AnyValue_oneof_value, KeyValue};
use crate::proto::trace::{Span_Event, Span_Link};
use protobuf::{RepeatedField, SingularPtrField};
use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::default::Default;
use std::hash::{Hash, Hasher};
use unsigned_varint::decode;

pub struct InplaceSpanDecoder<'a>(pub &'a [u8]);

impl<'a> InplaceSpanDecoder<'a> {
    pub fn decode_trace_id(&self) -> &[u8] {
        &self.0[..16]
    }

    /// hashed_trace_id returns the hash of trace_id.
    pub fn hashed_trace_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        &self.0[..16].hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Default)]
pub struct SpanDecoder<'a> {
    reader: BufferReader<'a>,
    trace_id: Option<&'a [u8]>,
    span_id: Option<&'a [u8]>,
    parent_span_id: Option<&'a [u8]>,
    start_time: u64,
    end_time: u64,
    span_kind: u8,
    name: String,
    attributes: Vec<KeyValue>,
    events: Vec<Span_Event>,
    links: Vec<Span_Link>,
}

impl<'a> SpanDecoder<'a> {
    pub fn new(src: &[u8]) -> SpanDecoder {
        SpanDecoder {
            reader: BufferReader::new(src),
            ..Default::default()
        }
    }

    pub fn decode(&mut self) {
        self.trace_id = Some(self.reader.read_exact_length(16).unwrap());
        self.span_id = Some(self.reader.read_exact_length(16).unwrap());
        if self.reader.read_byte().unwrap() == PARENT_SPAN_ID_EXIST {
            self.parent_span_id = Some(self.reader.read_exact_length(16).unwrap());
        }
        let buf = self.reader.read_exact_length(8).unwrap();
        self.start_time = u64::from_be_bytes(buf.try_into().unwrap());
        let buf = self.reader.read_exact_length(8).unwrap();
        self.end_time = u64::from_be_bytes(buf.try_into().unwrap());
        self.span_kind = self.reader.read_byte().unwrap();
        self.name = String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
        self.decode_attributes(&mut self.attributes);
        self.decode_event();
        self.decode_link();
    }

    pub fn decode_attributes(&mut self, attributes: &mut Vec<KeyValue>) {
        let meta = self.reader.peek_byte();
        if let None = meta {
            return;
        }
        if meta.unwrap() != ATTRIBUTE_TYPE {
            return;
        }
        self.reader.consume(1).unwrap();
        match self.reader.read_byte().unwrap() {
            BOOL_VAL_TYPE => {
                // parse the bool kv.
                let mut kv = KeyValue::default();
                kv.key =
                    String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
                let mut val = AnyValue::default();
                if self.reader.read_byte().unwrap() == 1 {
                    val.value = Some(AnyValue_oneof_value::bool_value(true));
                } else {
                    val.value = Some(AnyValue_oneof_value::bool_value(false));
                }
                kv.value = SingularPtrField::some(val);
                attributes.push(kv);
                // recursively check for attributes.
                self.decode_attributes(attributes);
            }
            STRING_VAL_TYPE => {
                let mut kv = KeyValue::default();
                kv.key =
                    String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
                let mut val = AnyValue::default();
                val.value = Some(AnyValue_oneof_value::string_value(
                    String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string(),
                ));
                attributes.push(kv);
                self.decode_attributes(attributes);
            }
            INT_VAL_TYPE => {
                let mut kv = KeyValue::default();
                kv.key =
                    String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
                let mut val = AnyValue::default();
                val.value = Some(AnyValue_oneof_value::int_value(i64::from_be_bytes(
                    self.reader
                        .read_slice()
                        .unwrap()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                )));
                attributes.push(kv);
                self.decode_attributes(attributes);
            }
            DOUBLE_VAL_TYPE => {
                let mut kv = KeyValue::default();
                kv.key =
                    String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
                let mut val = AnyValue::default();
                val.value = Some(AnyValue_oneof_value::double_value(f64::from_be_bytes(
                    self.reader
                        .read_slice()
                        .unwrap()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                )));
                attributes.push(kv);
                self.decode_attributes(attributes);
            }
            _ => {
                unreachable!("yolo");
            }
        }
    }

    fn decode_event(&mut self) {
        self.reader.peek_byte().map(|meta| {
            if meta != EVENT_TYPE {
                return;
            }
            self.reader.consume(1).unwrap();
            let mut event = Span_Event::new();
            event.time_unix_nano = u64::from_be_bytes(
                self.reader
                    .read_slice()
                    .unwrap()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            event.name =
                String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
            let mut attributes = Vec::new();
            self.decode_attributes(&mut attributes);
            event.attributes = RepeatedField::from(attributes);
            self.events.push(event);
            self.decode_event();
        });
    }

    fn decode_link(&mut self) {
        let meta = self.reader.peek_byte();
        meta.map(|meta| {
            if meta != LINK_TYPE {
                return;
            }
            self.reader.consume(1).unwrap();
            let mut link = Span_Link::new();
            link.trace_id = self.reader.read_exact_length(16).unwrap().to_vec();
            link.span_id = self.reader.read_exact_length(16).unwrap().to_vec();
            link.trace_state =
                String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
            let mut attributes = Vec::new();
            self.decode_attributes(&mut attributes);
            link.attributes = RepeatedField::from(attributes);
        });
    }
}
