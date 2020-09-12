use crate::buffer::buffer::Buffer;
use crate::proto::common::{AnyValue_oneof_value, KeyValue};
use crate::proto::trace::{Span, Span_SpanKind, Span_Event, Span_Link};
use crate::utils::utils::create_index_key;
use std::collections::HashSet;
use log::warn;
use crate::memtable::index_store::IndexStore;

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
// Tells that span is ended.
const SPAN_END: u8 = 11;
/// encode_span encodes the given span into the buffer.
pub fn encode_span(span: &Span, buffer: &mut Buffer) -> HashSet<String>{
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
    let mut indices = HashSet::default();
    encode_attributes(&span.attributes.as_ref().to_vec(),  buffer, &mut indices);
    encode_event(&span.events.to_vec(), buffer,&mut indices);
    encode_links(&span.links.to_vec(), buffer,&mut indices);
    indices
}

fn encode_event(events: &Vec<Span_Event>,buffer: &mut Buffer, indices: &mut HashSet<String>){
    if events.len() == 0 {
    }
    for event in events{
        buffer.write_byte(EVENT_TYPE);
        buffer.write_raw_slice(&event.time_unix_nano.to_be_bytes());
        buffer.write_slice(event.name.as_bytes());
        encode_attributes(&event.attributes.to_vec(), buffer, indices);
    }
}

/// encode_links encode span links.
fn encode_links(links: &Vec<Span_Link>, buffer: &mut Buffer, indices:&mut HashSet<String>){
    if links.len() == 0{
        return;
    }
    for link in links{
        buffer.write_byte(LINK_TYPE);
        buffer.write_raw_slice(&link.trace_id);
        buffer.write_raw_slice(&link.span_id);
        buffer.write_raw_slice(link.trace_state.as_bytes());
        encode_attributes(&link.attributes.to_vec(), buffer, indices);
    }
}

fn encode_attributes(attributes: &Vec<KeyValue>, buffer: &mut Buffer, indices:&mut HashSet<String>){
    if attributes.len() == 0 {
        return ;
    }
    for attribute in attributes {
        buffer.write_byte(ATTRIBUTE_TYPE);
        let value = attribute.value.as_ref().unwrap().value.as_ref().unwrap();
        if let AnyValue_oneof_value::array_value(_) = value {
            warn!("dropping array attribute {:?}", attribute);
            continue;
        } else if let AnyValue_oneof_value::kvlist_value(_) = value {
            warn!("dropping kv list value {:?}", attribute);
            continue;
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
                indices.insert(create_index_key(&attribute.key, val));
            }
            AnyValue_oneof_value::string_value(val) => {
                buffer.write_byte(STRING_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                buffer.write_slice(&val.as_bytes());
                indices.insert(create_index_key(&attribute.key, val));
            }
            AnyValue_oneof_value::int_value(val) => {
                buffer.write_byte(INT_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                buffer.write_slice(&val.to_be_bytes());
                indices.insert(create_index_key(&attribute.key, val));
            }
            AnyValue_oneof_value::double_value(val) => {
                buffer.write_byte(DOUBLE_VAL_TYPE);
                buffer.write_slice(&attribute.key.as_bytes());
                buffer.write_slice(&val.to_be_bytes());
                indices.insert(create_index_key(&attribute.key, val));
            }
            _ => {
                panic!("undefined ub");
            }
        }
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
