use crate::proto::common::{AnyValue_oneof_value, KeyValue};
use crate::proto::trace::{Span, Span_Event, Span_Link, Span_SpanKind};
use log::warn;
use std::{u16, u8};
const FALSE_BOOL_VAL_TYPE: u8 = 0;
const TRUE_BOOL_VAL_TYPE: u8 = 1;
const DOUBLE_VAL_TYPE: u8 = 2;
const INT_VAL_TYPE: u8 = 3;
const STRING_VAL_TYPE: u8 = 4;

/// encode_trace encodes the given trace into akkal file format for the efficient storing of data
/// into binary format instead of protobuf encoding.
#[inline(always)]
pub fn encode_trace(service_id: u64, span: &Span, mut buffer: &mut Vec<u8>) {
    // clear the buffer before writing anything.
    buffer.clear();
    // Write all the fixed size parameter to the buffer first.
    buffer.extend(&span.trace_id);
    buffer.extend(&span.span_id);
    buffer.extend(&span.parent_span_id);
    buffer.extend(service_id.to_be_bytes().iter());
    buffer.extend(span.start_time_unix_nano.to_be_bytes().iter());
    buffer.extend(span.end_time_unix_nano.to_be_bytes().iter());
    buffer.push(span_kind_to_u8(&span.kind));
    // Encode the given attributes to the buffer.
    encode_attributes(&mut buffer, &span.attributes.to_vec());
    // Encode the events to the buffer.
    encode_event(&mut buffer, &span.events.to_vec());
    // Encode the link to the buffer.
    encode_links(&mut buffer, &span.links.to_vec());
}

/// encode_attributes to the given buffer and returns the size of attribute that
/// has been encoded.
#[inline(always)]
fn encode_attributes(buffer: &mut Vec<u8>, attributes: &Vec<KeyValue>) {
    let attribute_len_offset = buffer.len();
    // Reserve attribute size for the
    let mut attribute_size: u16 = 0;
    buffer.extend(attribute_size.to_be_bytes().iter());
    // Track the attribute initial offset to do the total attribute
    // size calculation at the end.
    let initial_offset = buffer.len();
    // Now write attribute one by one.
    for attribute in attributes {
        // Ignore all the attributes which can't be fit into u8.
        if attribute.key.len() > u8::MAX.into() {
            warn!("igoring attribute {}", attribute.key);
            continue;
        }
        let val = attribute.value.as_ref().unwrap().value.as_ref().unwrap();
        if let AnyValue_oneof_value::array_value(_) = val {
            warn!("dropping array attribute");
            continue;
        } else if let AnyValue_oneof_value::kvlist_value(_) = val {
            warn!("dropping kv list attribute");
            continue;
        }
        let key_size: u8 = attribute.key.len() as u8;
        // attribute_size = attribute_size + key_size as u16;
        buffer.extend(key_size.to_be_bytes().iter());
        buffer.extend(attribute.key.bytes());
        // Write the value
        match val {
            AnyValue_oneof_value::bool_value(val) => {
                if *val {
                    buffer.push(TRUE_BOOL_VAL_TYPE);
                    continue;
                }
                buffer.push(FALSE_BOOL_VAL_TYPE);
            }
            AnyValue_oneof_value::double_value(val) => {
                buffer.push(DOUBLE_VAL_TYPE);
                buffer.extend(val.to_be_bytes().iter());
            }
            AnyValue_oneof_value::int_value(val) => {
                buffer.push(INT_VAL_TYPE);
                buffer.extend(val.to_be_bytes().iter());
            }
            AnyValue_oneof_value::string_value(val) => {
                // Store the paritial  string since max string array
                // we are allocating is u8::MAX
                buffer.push(STRING_VAL_TYPE);
                let val_bytes = val.as_bytes();
                let trimmerd_len = val_bytes.len() as u8;
                buffer.push(trimmerd_len);
                buffer.extend(&val_bytes[..trimmerd_len as usize]);
            }
            _ => panic!("ub at encoding attributes"),
        }
    }
    let size = buffer.len() - initial_offset;
    assert!(size <= u16::MAX as usize);
    attribute_size = size as u16;
    // Inplace update the attribute size.
    let attribute_size_buffer = attribute_size.to_be_bytes();
    buffer[attribute_len_offset] = attribute_size_buffer[0];
    buffer[attribute_len_offset + 1] = attribute_size_buffer[1];
}

/// encode_event encodes span event to the given buffer.
#[inline(always)]
fn encode_event(buffer: &mut Vec<u8>, events: &Vec<Span_Event>) {
    let event_len_offset = buffer.len();
    // reserve size for storing event size.
    let mut event_size: u16 = 0;
    buffer.extend(event_size.to_be_bytes().iter());
    // Track the event initial offset to do the total event
    // size calculation at the end.
    let initial_offset = buffer.len();
    // Now write each event one by one
    for event in events {
        buffer.extend(&event.time_unix_nano.to_be_bytes());
        let name_bytes = event.name.as_bytes();
        let trimmed_len = name_bytes.len() as u8;
        buffer.push(trimmed_len);
        buffer.extend(&name_bytes[..trimmed_len as usize]);
    }
    let size = buffer.len() - initial_offset;
    assert!(size <= u16::MAX as usize);
    event_size = size as u16;
    // Inplace update the attribute size.
    let event_size_buffer = event_size.to_be_bytes();
    buffer[event_len_offset] = event_size_buffer[0];
    buffer[event_len_offset + 1] = event_size_buffer[1];
}

/// encode_links encodes span link's to the given buffer.
#[inline(always)]
fn encode_links(buffer: &mut Vec<u8>, links: &Vec<Span_Link>) {
    let links_len_offset = buffer.len();
    // reserve size for storing link's size.
    let mut link_size: u16 = 0;
    buffer.extend(link_size.to_be_bytes().iter());
    // Track the event initial offset to do the total event
    // size calculation at the end.
    let initial_offset = buffer.len();
    // Now write each link one by one
    for link in links {
        buffer.extend(&link.trace_id);
        buffer.extend(&link.span_id);
        let trace_state_bytes = link.trace_state.as_bytes();
        let trimmed_len = trace_state_bytes.len() as u8;
        buffer.push(trimmed_len);
        buffer.extend(&trace_state_bytes[..trimmed_len as usize]);
        encode_attributes(buffer, &link.attributes.to_vec());
    }
    let size = buffer.len() - initial_offset;
    assert!(size <= u16::MAX as usize);
    link_size = size as u16;
    // Inplace update the link size.
    let link_size_buffer = link_size.to_be_bytes();
    buffer[links_len_offset] = link_size_buffer[0];
    buffer[links_len_offset + 1] = link_size_buffer[1];
}

/// span_kind_to_u8 converts span kind to u8
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::proto::common::{AnyValue, AnyValue_oneof_value, KeyValue};
    use protobuf::{Message, RepeatedField, SingularPtrField};
    use rand;
    use rand::Rng;
    use test::Bencher;
    pub fn generate_random_spans() -> Vec<Span> {
        let mut span = Span::default();
        span.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.parent_span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        span.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let mut event = Span_Event::default();
        let mut kv = KeyValue::default();
        kv.key = String::from("sup magic man");
        let mut val = AnyValue::default();
        val.value = Some(AnyValue_oneof_value::string_value(String::from(
            "let's make it right",
        )));
        kv.value = SingularPtrField::from(Some(val));
        let mut attributes = vec![kv.clone()];
        // Let's add more event.
        attributes.push(kv.clone());
        attributes.push(kv.clone());
        attributes.push(kv.clone());
        attributes.push(kv.clone());
        span.attributes = RepeatedField::from(attributes.clone());
        event.attributes = RepeatedField::from(attributes.clone());
        span.events = RepeatedField::from(vec![event.clone()]);
        span.events.push(event.clone());
        span.events.push(event.clone());
        span.events.push(event.clone());
        span.events.push(event.clone());
        span.events.push(event.clone());
        let mut link = Span_Link::default();
        link.trace_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        link.span_id = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        link.trace_state = String::from("supl loadded state");
        link.attributes = RepeatedField::from(attributes);
        let mut links = vec![link.clone()];
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        links.push(link.clone());
        span.links = RepeatedField::from(links);
        let mut spans = Vec::new();
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans.push(span.clone());
        spans
    }

    #[bench]
    fn bench_protobuf_span_encoding(b: &mut Bencher) {
        let spans = generate_random_spans();
        b.iter(|| {
            for span in spans.iter() {
                let _bytes = span.write_to_bytes();
            }
        });
    }

    #[bench]
    fn bench_akkal_encoding(b: &mut Bencher) {
        let spans = generate_random_spans();
        b.iter(|| {
            for span in spans.iter() {
                let mut buffer = Vec::new();
                encode_trace(1, &span, &mut buffer);
            }
        });
    }

    #[bench]
    fn bench_akkal_encoding_reusing_bytes(b: &mut Bencher) {
        let spans = generate_random_spans();
        let mut buffer = Vec::with_capacity(3000);
        b.iter(|| {
            for span in spans.iter() {
                encode_trace(1, &span, &mut buffer);
            }
        });
    }

    #[bench]
    fn bench_protobuf_span_encoding_reuse(b: &mut Bencher) {
        let spans = generate_random_spans();
        let mut buffer = Vec::with_capacity(3000);
        b.iter(|| {
            for span in spans.iter() {
                span.write_to_vec(&mut buffer).unwrap();
                buffer.clear();
            }
        });
    }
}
