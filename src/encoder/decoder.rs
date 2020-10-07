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
    pub trace_id: Option<Vec<u8>>,
    pub span_id: Option<Vec<u8>>,
    pub parent_span_id: Option<Vec<u8>>,
    pub start_time: u64,
    pub end_time: u64,
    pub span_kind: u8,
    pub name: String,
    pub attributes: Vec<KeyValue>,
    pub events: Vec<Span_Event>,
    pub links: Vec<Span_Link>,
}

impl<'a> SpanDecoder<'a> {
    /// new decodes the encoded span and returns the decoded span.
    pub fn new(src: &[u8]) -> SpanDecoder {
        let mut decoder = SpanDecoder {
            reader: BufferReader::new(src),
            ..Default::default()
        };
        decoder.decode();
        decoder
    }

    /// decode decodes the span.
    fn decode(&mut self) {
        self.trace_id = Some(self.reader.read_exact_length(16).unwrap().to_vec());
        self.span_id = Some(self.reader.read_exact_length(16).unwrap().to_vec());
        if self.reader.read_byte().unwrap() == PARENT_SPAN_ID_EXIST {
            self.parent_span_id = Some(self.reader.read_exact_length(16).unwrap().to_vec());
        }
        let buf = self.reader.read_exact_length(8).unwrap();
        self.start_time = u64::from_be_bytes(buf.try_into().unwrap());
        let buf = self.reader.read_exact_length(8).unwrap();
        self.end_time = u64::from_be_bytes(buf.try_into().unwrap());
        self.span_kind = self.reader.read_byte().unwrap();
        self.name = String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
        let mut attributes = Vec::new();
        self.decode_attributes(&mut attributes);
        self.attributes = attributes;
        self.decode_event();
        self.decode_link();
    }

    /// decode_attributes decodes the attribute and add the key value to the given
    /// vector.
    fn decode_attributes(&mut self, attributes: &mut Vec<KeyValue>) {
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

    /// decode_event decodes the event.
    fn decode_event(&mut self) {
        self.reader.peek_byte().map(|meta| {
            if meta != EVENT_TYPE {
                return;
            }
            self.reader.consume(1).unwrap();
            let mut event = Span_Event::new();
            let buf = self.reader.read_exact_length(8).unwrap();
            event.time_unix_nano = u64::from_be_bytes(buf.try_into().unwrap());
            event.name =
                String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
            let mut attributes = Vec::new();
            self.decode_attributes(&mut attributes);
            event.attributes = RepeatedField::from(attributes);
            self.events.push(event);
            self.decode_event();
        });
    }

    /// decode_link decodes the span link.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer::Buffer;
    use crate::encoder::span::encode_span;
    use crate::proto::trace::Span;
    use rand::Rng;

    #[test]
    fn test_decoding() {
        let mut span = Span::default();
        span.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
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
        link.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        link.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.links = RepeatedField::from(vec![link]);

        let mut buffer = Buffer::with_size(2 << 20);
        encode_span(&span, &mut buffer);

        let mut decode = SpanDecoder::new(buffer.bytes_ref());
        decode.decode();
        assert_eq!(decode.name, String::from("hello"));
        assert_eq!(decode.attributes.len(), 1);
        assert_eq!(decode.events.len(), 2);
        assert_eq!(decode.attributes[0].key, String::from("string kv"));
    }
}
