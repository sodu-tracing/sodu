use crate::buffer::buffer::Buffer;
use crate::proto::common::{AnyValue_oneof_value, KeyValue};
use crate::proto::trace::{Span, Span_SpanKind, Span_Event};
use log::warn;
// Tells that upcoming bytes of attribute key and value.
const ATTRIBUTE_TYPE: u8 = 1;
// Tells that upcoming bytes is of link type.
const LINK_TYPE: u8 = 2;
// Tells that upcoming bytes is of event type.
const EVENT_TYPE: u8 = 3;
// Tells that parent span id exist.
const PARENT_SPAN_ID_EXIST: u8 = 4;
// Tells that parent span id is not exist.
const PARENT_SPAN_ID_NOT_EXIST: u8 = 5;
// Attribute of bool type.
const BOOL_VAL_TYPE: u8 = 6;
// Attribute of double val type.
const DOUBLE_VAL_TYPE: u8 = 8;
// Attribute of int val type.
const INT_VAL_TYPE: u8 = 9;
// Attribute of string val type.
const STRING_VAL_TYPE: u8 = 10;
// Tells that attribute exist.
const ATTRIBUTE_EXIST: u8 = 11;
// Tells that attribute not exist.
const ATTRIBUTE_NOT_EXIST: u8 = 12;
// Tells that attribute is end.
const ATTRIBUTE_END: u8 = 13;

/// encode_span encodes the given span into the buffer.
pub fn encode_span(span: &Span, buffer: &mut Buffer) {
    buffer.write_raw_slice(&span.span_id);
    // Parent span id can be empty because first span don't have any parent
    // span id.
    if !span.parent_span_id.is_empty() {
        buffer.write_byte(PARENT_SPAN_ID_EXIST);
        buffer.write_raw_slice(&span.parent_span_id);
    } else {
        buffer.write_byte(PARENT_SPAN_ID_NOT_EXIST);
    }
    buffer.write_raw_slice(&span.start_time_unix_nano.to_be_bytes());
    buffer.write_raw_slice(&span.end_time_unix_nano.to_be_bytes());
    buffer.write_raw_slice(&[span_kind_to_u8(&span.kind)]);
    buffer.write_slice(span.name.as_bytes());
    encode_attributes(&span.attributes.as_ref().to_vec(),  buffer);
}

fn encode_event(event: &Vec<Span_Event>,buffer: &mut Buffer){
    if event.len() == 0 {
        return;
    }

}

fn encode_attributes(attributes: &Vec<KeyValue>, buffer: &mut Buffer) {
    if attributes.len() == 0 {
        buffer.write_byte(ATTRIBUTE_NOT_EXIST);
        return;
    }
    let mut one_attribute_written = false;
    for attribute in attributes {
        let value = attribute.value.as_ref().unwrap().value.as_ref().unwrap();
        if let AnyValue_oneof_value::array_value(_) = value {
            warn!("dropping array attribute {:?}", attribute);
            continue;
        } else if let AnyValue_oneof_value::kvlist_value(_) = value {
            warn!("dropping kv list value {:?}", attribute);
            continue;
        }
        if !one_attribute_written {
           buffer.write_byte(ATTRIBUTE_EXIST);
            one_attribute_written = true;
        }
        match value {
            AnyValue_oneof_value::bool_value(val) => {
                buffer.write_byte(BOOL_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                if *val {
                    buffer.write_byte(1);
                    continue;
                }
                buffer.write_byte(0);
            }
            AnyValue_oneof_value::string_value(val) => {
                buffer.write_byte(STRING_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                buffer.write_slice(&val.as_bytes());
            }
            AnyValue_oneof_value::int_value(val) => {
                buffer.write_byte(INT_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                buffer.write_slice(&val.to_be_bytes());
            }
            AnyValue_oneof_value::double_value(val) => {
                buffer.write_byte(DOUBLE_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                buffer.write_slice(&val.to_be_bytes());
            }
            _ => {
                panic!("undefined ub");
            }
        }
    }
    if one_attribute_written {
        buffer.write_byte(ATTRIBUTE_END);
    }
}

#[inline(always)]
fn span_kind_to_u8(kind: &Span_SpanKind) -> u8 {
    match kind {
        Span_SpanKind::SPAN_KIND_UNSPECIFIED => Span_SpanKind::SPAN_KIND_UNSPECIFIED as u8,
        Span_SpanKind::SPAN_KIND_INTERNAL => Span_SpanKind::SPAN_KIND_INTERNAL as u8,
        Span_SpanKind::SPAN_KIND_SERVER => Span_SpanKind::SPAN_KIND_SERVER as u8,
        Span_SpanKind::SPAN_KIND_CLIENT => Span_SpanKind::SPAN_KIND_CLIENT as u8,
        Span_SpanKind::SPAN_KIND_PRODUCER => Span_SpanKind::SPAN_KIND_PRODUCER as u8,
        Span_SpanKind::SPAN_KIND_CONSUMER => Span_SpanKind::SPAN_KIND_CONSUMER as u8,
    }
}
