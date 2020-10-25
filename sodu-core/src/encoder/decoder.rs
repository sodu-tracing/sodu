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
use crate::proto::trace::{Span, Span_Event, Span_Link, Span_SpanKind};
use protobuf::ProtobufEnum;
use protobuf::{RepeatedField, SingularPtrField};
use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::default::Default;
use std::hash::{Hash, Hasher};

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

    pub fn start_ts(&self) -> u64 {
        // skipping trace_id and span id. Moving the offset to parent span id.
        let mut offset = 16 + 16;
        if self.0[offset] == PARENT_SPAN_ID_EXIST {
            // consume offset for parent span id.
            offset += 16
        }
        // consume offset for PARENT_SPAN_ID_EXIST
        offset += 1;
        u64::from_be_bytes(self.0[offset..offset + 8].try_into().unwrap())
    }
}

/// decode_span decodes the given span to protobuf span.
pub fn decode_span(src: &[u8]) -> Span {
    let decoder = SpanDecoder::new(src);
    let mut span = Span::default();
    span.trace_id = decoder.trace_id.unwrap();
    span.span_id = decoder.span_id.unwrap();
    if let Some(parent_span_id) = decoder.parent_span_id {
        span.parent_span_id = parent_span_id;
    }
    span.name = decoder.name;
    span.kind = u8_to_span_kind(decoder.span_kind);
    span.start_time_unix_nano = decoder.start_time;
    span.end_time_unix_nano = decoder.end_time;
    span.attributes = RepeatedField::from(decoder.attributes);
    span.events = RepeatedField::from(decoder.events);
    span.links = RepeatedField::from(decoder.links);
    span
}

#[inline(always)]
fn u8_to_span_kind(kind: u8) -> Span_SpanKind {
    Span_SpanKind::from_i32(kind as i32).unwrap()
}

#[derive(Default)]
struct SpanDecoder<'a> {
    reader: BufferReader<'a>,
    trace_id: Option<Vec<u8>>,
    span_id: Option<Vec<u8>>,
    parent_span_id: Option<Vec<u8>>,
    start_time: u64,
    end_time: u64,
    span_kind: u8,
    name: String,
    attributes: Vec<KeyValue>,
    events: Vec<Span_Event>,
    links: Vec<Span_Link>,
}

impl<'a> SpanDecoder<'a> {
    /// new decodes the encoded span and returns the decoded span.
    fn new(src: &[u8]) -> SpanDecoder {
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
        let meta = self.reader.read_byte();
        if let None = meta {
            return;
        }
        if meta.unwrap() != ATTRIBUTE_TYPE {
            return;
        }
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
                kv.value = SingularPtrField::some(val);
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
                kv.value = SingularPtrField::some(val);
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
                kv.value = SingularPtrField::some(val);
                attributes.push(kv);
                self.decode_attributes(attributes);
            }
            _ => {
                unreachable!("yolo yada what did you do wrong tiger");
            }
        }
    }

    /// decode_event decodes the event.
    fn decode_event(&mut self) {
        self.reader.read_byte().map(|meta| {
            if meta != EVENT_TYPE {
                return;
            }
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
        let meta = self.reader.read_byte();
        meta.map(|meta| {
            if meta != LINK_TYPE {
                return;
            }
            let mut link = Span_Link::new();
            link.trace_id = self.reader.read_exact_length(16).unwrap().to_vec();
            link.span_id = self.reader.read_exact_length(16).unwrap().to_vec();
            link.trace_state =
                String::from_utf8_lossy(self.reader.read_slice().unwrap().unwrap()).to_string();
            let mut attributes = Vec::new();
            self.decode_attributes(&mut attributes);
            link.attributes = RepeatedField::from(attributes);
            self.links.push(link);
            self.decode_link();
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
        assert_eq!(decode.name, String::from("hello"));
        assert_eq!(decode.attributes.len(), 1);
        assert_eq!(decode.events.len(), 2);
        assert_eq!(decode.attributes[0].key, String::from("string kv"));
    }

    #[test]
    fn test_decoding_deep_equal() {
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
        let decoded_span = decode_span(buffer.bytes_ref());
        assert_eq!(span, decoded_span);
    }

    #[test]
    fn test_inplace_decoder() {
        let mut span = Span::default();
        span.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.start_time_unix_nano = 200;
        span.end_time_unix_nano = 203;
        let mut buffer = Buffer::with_size(2 << 20);
        encode_span(&span, &mut buffer);
        let decoder = InplaceSpanDecoder(buffer.bytes_ref());
        assert_eq!(decoder.start_ts(), 200);
        span.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.parent_span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.start_time_unix_nano = 200;
        span.end_time_unix_nano = 203;
        let mut buffer = Buffer::with_size(2 << 20);
        encode_span(&span, &mut buffer);
        let decoder = InplaceSpanDecoder(buffer.bytes_ref());
        assert_eq!(decoder.start_ts(), 200);
    }
}
