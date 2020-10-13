// This file is generated by rust-protobuf 2.17.0. Do not edit
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![rustfmt::skip]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_imports)]
#![allow(unused_results)]
//! Generated file from `service.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
// const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_2_17_0;

#[derive(PartialEq,Clone,Default)]
#[cfg_attr(feature = "with-serde", derive(Serialize, Deserialize))]
pub struct QueryRequest {
    // message fields
    pub service_name: ::protobuf::SingularField<::std::string::String>,
    pub operation_name: ::protobuf::SingularField<::std::string::String>,
    pub tags: ::std::collections::HashMap<::std::string::String, ::std::string::String>,
    pub start_ts: ::std::option::Option<u64>,
    pub end_ts: ::std::option::Option<u64>,
    // special fields
    #[cfg_attr(feature = "with-serde", serde(skip))]
    pub unknown_fields: ::protobuf::UnknownFields,
    #[cfg_attr(feature = "with-serde", serde(skip))]
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a QueryRequest {
    fn default() -> &'a QueryRequest {
        <QueryRequest as ::protobuf::Message>::default_instance()
    }
}

impl QueryRequest {
    pub fn new() -> QueryRequest {
        ::std::default::Default::default()
    }

    // optional string service_name = 1;


    pub fn get_service_name(&self) -> &str {
        match self.service_name.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
    pub fn clear_service_name(&mut self) {
        self.service_name.clear();
    }

    pub fn has_service_name(&self) -> bool {
        self.service_name.is_some()
    }

    // Param is passed by value, moved
    pub fn set_service_name(&mut self, v: ::std::string::String) {
        self.service_name = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_service_name(&mut self) -> &mut ::std::string::String {
        if self.service_name.is_none() {
            self.service_name.set_default();
        }
        self.service_name.as_mut().unwrap()
    }

    // Take field
    pub fn take_service_name(&mut self) -> ::std::string::String {
        self.service_name.take().unwrap_or_else(|| ::std::string::String::new())
    }

    // optional string operation_name = 2;


    pub fn get_operation_name(&self) -> &str {
        match self.operation_name.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
    pub fn clear_operation_name(&mut self) {
        self.operation_name.clear();
    }

    pub fn has_operation_name(&self) -> bool {
        self.operation_name.is_some()
    }

    // Param is passed by value, moved
    pub fn set_operation_name(&mut self, v: ::std::string::String) {
        self.operation_name = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_operation_name(&mut self) -> &mut ::std::string::String {
        if self.operation_name.is_none() {
            self.operation_name.set_default();
        }
        self.operation_name.as_mut().unwrap()
    }

    // Take field
    pub fn take_operation_name(&mut self) -> ::std::string::String {
        self.operation_name.take().unwrap_or_else(|| ::std::string::String::new())
    }

    // repeated .QueryRequest.TagsEntry tags = 3;


    pub fn get_tags(&self) -> &::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &self.tags
    }
    pub fn clear_tags(&mut self) {
        self.tags.clear();
    }

    // Param is passed by value, moved
    pub fn set_tags(&mut self, v: ::std::collections::HashMap<::std::string::String, ::std::string::String>) {
        self.tags = v;
    }

    // Mutable pointer to the field.
    pub fn mut_tags(&mut self) -> &mut ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        &mut self.tags
    }

    // Take field
    pub fn take_tags(&mut self) -> ::std::collections::HashMap<::std::string::String, ::std::string::String> {
        ::std::mem::replace(&mut self.tags, ::std::collections::HashMap::new())
    }

    // required uint64 start_ts = 4;


    pub fn get_start_ts(&self) -> u64 {
        self.start_ts.unwrap_or(0)
    }
    pub fn clear_start_ts(&mut self) {
        self.start_ts = ::std::option::Option::None;
    }

    pub fn has_start_ts(&self) -> bool {
        self.start_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_ts(&mut self, v: u64) {
        self.start_ts = ::std::option::Option::Some(v);
    }

    // required uint64 end_ts = 5;


    pub fn get_end_ts(&self) -> u64 {
        self.end_ts.unwrap_or(0)
    }
    pub fn clear_end_ts(&mut self) {
        self.end_ts = ::std::option::Option::None;
    }

    pub fn has_end_ts(&self) -> bool {
        self.end_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_end_ts(&mut self, v: u64) {
        self.end_ts = ::std::option::Option::Some(v);
    }
}

impl ::protobuf::Message for QueryRequest {
    fn is_initialized(&self) -> bool {
        if self.start_ts.is_none() {
            return false;
        }
        if self.end_ts.is_none() {
            return false;
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.service_name)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.operation_name)?;
                },
                3 => {
                    ::protobuf::rt::read_map_into::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(wire_type, is, &mut self.tags)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.end_ts = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.service_name.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.operation_name.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        my_size += ::protobuf::rt::compute_map_size::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(3, &self.tags);
        if let Some(v) = self.start_ts {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.end_ts {
            my_size += ::protobuf::rt::value_size(5, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.service_name.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.operation_name.as_ref() {
            os.write_string(2, &v)?;
        }
        ::protobuf::rt::write_map_with_cached_sizes::<::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(3, &self.tags, os)?;
        if let Some(v) = self.start_ts {
            os.write_uint64(4, v)?;
        }
        if let Some(v) = self.end_ts {
            os.write_uint64(5, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> QueryRequest {
        QueryRequest::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::LazyV2<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::LazyV2::INIT;
        descriptor.get(|| {
            let mut fields = ::std::vec::Vec::new();
            fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                "service_name",
                |m: &QueryRequest| { &m.service_name },
                |m: &mut QueryRequest| { &mut m.service_name },
            ));
            fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                "operation_name",
                |m: &QueryRequest| { &m.operation_name },
                |m: &mut QueryRequest| { &mut m.operation_name },
            ));
            fields.push(::protobuf::reflect::accessor::make_map_accessor::<_, ::protobuf::types::ProtobufTypeString, ::protobuf::types::ProtobufTypeString>(
                "tags",
                |m: &QueryRequest| { &m.tags },
                |m: &mut QueryRequest| { &mut m.tags },
            ));
            fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                "start_ts",
                |m: &QueryRequest| { &m.start_ts },
                |m: &mut QueryRequest| { &mut m.start_ts },
            ));
            fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                "end_ts",
                |m: &QueryRequest| { &m.end_ts },
                |m: &mut QueryRequest| { &mut m.end_ts },
            ));
            ::protobuf::reflect::MessageDescriptor::new_pb_name::<QueryRequest>(
                "QueryRequest",
                fields,
                file_descriptor_proto()
            )
        })
    }

    fn default_instance() -> &'static QueryRequest {
        static instance: ::protobuf::rt::LazyV2<QueryRequest> = ::protobuf::rt::LazyV2::INIT;
        instance.get(QueryRequest::new)
    }
}

impl ::protobuf::Clear for QueryRequest {
    fn clear(&mut self) {
        self.service_name.clear();
        self.operation_name.clear();
        self.tags.clear();
        self.start_ts = ::std::option::Option::None;
        self.end_ts = ::std::option::Option::None;
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for QueryRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for QueryRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ReflectValueRef {
        ::protobuf::reflect::ReflectValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
#[cfg_attr(feature = "with-serde", derive(Serialize, Deserialize))]
pub struct InternalTrace {
    // message fields
    pub start_ts: ::std::option::Option<u64>,
    pub trace: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    #[cfg_attr(feature = "with-serde", serde(skip))]
    pub unknown_fields: ::protobuf::UnknownFields,
    #[cfg_attr(feature = "with-serde", serde(skip))]
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a InternalTrace {
    fn default() -> &'a InternalTrace {
        <InternalTrace as ::protobuf::Message>::default_instance()
    }
}

impl InternalTrace {
    pub fn new() -> InternalTrace {
        ::std::default::Default::default()
    }

    // required uint64 start_ts = 1;


    pub fn get_start_ts(&self) -> u64 {
        self.start_ts.unwrap_or(0)
    }
    pub fn clear_start_ts(&mut self) {
        self.start_ts = ::std::option::Option::None;
    }

    pub fn has_start_ts(&self) -> bool {
        self.start_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_ts(&mut self, v: u64) {
        self.start_ts = ::std::option::Option::Some(v);
    }

    // required bytes trace = 2;


    pub fn get_trace(&self) -> &[u8] {
        match self.trace.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
    pub fn clear_trace(&mut self) {
        self.trace.clear();
    }

    pub fn has_trace(&self) -> bool {
        self.trace.is_some()
    }

    // Param is passed by value, moved
    pub fn set_trace(&mut self, v: ::std::vec::Vec<u8>) {
        self.trace = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_trace(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.trace.is_none() {
            self.trace.set_default();
        }
        self.trace.as_mut().unwrap()
    }

    // Take field
    pub fn take_trace(&mut self) -> ::std::vec::Vec<u8> {
        self.trace.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }
}

impl ::protobuf::Message for InternalTrace {
    fn is_initialized(&self) -> bool {
        if self.start_ts.is_none() {
            return false;
        }
        if self.trace.is_none() {
            return false;
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.trace)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.start_ts {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.trace.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.start_ts {
            os.write_uint64(1, v)?;
        }
        if let Some(ref v) = self.trace.as_ref() {
            os.write_bytes(2, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> InternalTrace {
        InternalTrace::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::LazyV2<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::LazyV2::INIT;
        descriptor.get(|| {
            let mut fields = ::std::vec::Vec::new();
            fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                "start_ts",
                |m: &InternalTrace| { &m.start_ts },
                |m: &mut InternalTrace| { &mut m.start_ts },
            ));
            fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                "trace",
                |m: &InternalTrace| { &m.trace },
                |m: &mut InternalTrace| { &mut m.trace },
            ));
            ::protobuf::reflect::MessageDescriptor::new_pb_name::<InternalTrace>(
                "InternalTrace",
                fields,
                file_descriptor_proto()
            )
        })
    }

    fn default_instance() -> &'static InternalTrace {
        static instance: ::protobuf::rt::LazyV2<InternalTrace> = ::protobuf::rt::LazyV2::INIT;
        instance.get(InternalTrace::new)
    }
}

impl ::protobuf::Clear for InternalTrace {
    fn clear(&mut self) {
        self.start_ts = ::std::option::Option::None;
        self.trace.clear();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for InternalTrace {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for InternalTrace {
    fn as_ref(&self) -> ::protobuf::reflect::ReflectValueRef {
        ::protobuf::reflect::ReflectValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
#[cfg_attr(feature = "with-serde", derive(Serialize, Deserialize))]
pub struct QueryResponse {
    // message fields
    pub traces: ::protobuf::RepeatedField<InternalTrace>,
    // special fields
    #[cfg_attr(feature = "with-serde", serde(skip))]
    pub unknown_fields: ::protobuf::UnknownFields,
    #[cfg_attr(feature = "with-serde", serde(skip))]
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a QueryResponse {
    fn default() -> &'a QueryResponse {
        <QueryResponse as ::protobuf::Message>::default_instance()
    }
}

impl QueryResponse {
    pub fn new() -> QueryResponse {
        ::std::default::Default::default()
    }

    // repeated .InternalTrace traces = 1;


    pub fn get_traces(&self) -> &[InternalTrace] {
        &self.traces
    }
    pub fn clear_traces(&mut self) {
        self.traces.clear();
    }

    // Param is passed by value, moved
    pub fn set_traces(&mut self, v: ::protobuf::RepeatedField<InternalTrace>) {
        self.traces = v;
    }

    // Mutable pointer to the field.
    pub fn mut_traces(&mut self) -> &mut ::protobuf::RepeatedField<InternalTrace> {
        &mut self.traces
    }

    // Take field
    pub fn take_traces(&mut self) -> ::protobuf::RepeatedField<InternalTrace> {
        ::std::mem::replace(&mut self.traces, ::protobuf::RepeatedField::new())
    }
}

impl ::protobuf::Message for QueryResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.traces {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.traces)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.traces {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        for v in &self.traces {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> QueryResponse {
        QueryResponse::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::LazyV2<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::LazyV2::INIT;
        descriptor.get(|| {
            let mut fields = ::std::vec::Vec::new();
            fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<InternalTrace>>(
                "traces",
                |m: &QueryResponse| { &m.traces },
                |m: &mut QueryResponse| { &mut m.traces },
            ));
            ::protobuf::reflect::MessageDescriptor::new_pb_name::<QueryResponse>(
                "QueryResponse",
                fields,
                file_descriptor_proto()
            )
        })
    }

    fn default_instance() -> &'static QueryResponse {
        static instance: ::protobuf::rt::LazyV2<QueryResponse> = ::protobuf::rt::LazyV2::INIT;
        instance.get(QueryResponse::new)
    }
}

impl ::protobuf::Clear for QueryResponse {
    fn clear(&mut self) {
        self.traces.clear();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for QueryResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for QueryResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ReflectValueRef {
        ::protobuf::reflect::ReflectValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\rservice.proto\"\xf0\x01\n\x0cQueryRequest\x12!\n\x0cservice_name\x18\
    \x01\x20\x01(\tR\x0bserviceName\x12%\n\x0eoperation_name\x18\x02\x20\x01\
    (\tR\roperationName\x12+\n\x04tags\x18\x03\x20\x03(\x0b2\x17.QueryReques\
    t.TagsEntryR\x04tags\x12\x19\n\x08start_ts\x18\x04\x20\x02(\x04R\x07star\
    tTs\x12\x15\n\x06end_ts\x18\x05\x20\x02(\x04R\x05endTs\x1a7\n\tTagsEntry\
    \x12\x10\n\x03key\x18\x01\x20\x01(\tR\x03key\x12\x14\n\x05value\x18\x02\
    \x20\x01(\tR\x05value:\x028\x01\"@\n\rInternalTrace\x12\x19\n\x08start_t\
    s\x18\x01\x20\x02(\x04R\x07startTs\x12\x14\n\x05trace\x18\x02\x20\x02(\
    \x0cR\x05trace\"7\n\rQueryResponse\x12&\n\x06traces\x18\x01\x20\x03(\x0b\
    2\x0e.InternalTraceR\x06traces2<\n\x0bSoduStorage\x12-\n\nQueryTrace\x12\
    \r.QueryRequest\x1a\x0e.QueryResponse\"\0\
";

static file_descriptor_proto_lazy: ::protobuf::rt::LazyV2<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::LazyV2::INIT;

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    file_descriptor_proto_lazy.get(|| {
        parse_descriptor_proto()
    })
}
