use crate::proto::trace::Span;
use crate::proto::common::{AnyValue_oneof_value, KeyValue};
use std::collections::HashMap;

// TODO: schema for attributes.
// or give all the attribute names.
pub struct TraceWriter {
    service_names: HashMap<String, u32>,
    assigned_service_id: u32,
    tag_index: HashMap<String, Vec<Vec<u8>>>
}

impl TraceWriter {
    pub fn write(&mut self, mut spans: Vec<Span>){
        // Sort the span according to the trace id. 
        // Since all the span of same trace needs to collocated for the easy retrival of entire
        // trace. 
        spans.sort_by(|a, b| a.trace_id.cmp(&b.trace_id));
        
        for span in spans{
            let mut service_id: &u32 = &0;
            if let Some(id) = self.service_names.get(&span.name){
                service_id = id;
            } else{
                self.service_names.insert(span.name.clone(), self.assigned_service_id);
                self.assigned_service_id = self.assigned_service_id + 1;
            }
            self.create_attributes_index(&span.trace_id,&span.attributes.into_vec());
        }
    }

    /// create_attributes_index creates index for the given span attributes.
    fn create_attributes_index(&mut self,trace_id: &Vec<u8>, attributes: &Vec<KeyValue>){
        // Create index for traces.
        for attribute in attributes.into_iter(){
            let val = attribute.value.as_ref().unwrap().value.as_ref().unwrap();
            match val{
                AnyValue_oneof_value::string_value(str_val) =>{
                    let index_key = format!("{}_{}", attribute.key, str_val);
                    self.push_index(index_key, trace_id.clone());
                },
                AnyValue_oneof_value::bool_value(bool_val) =>{
                    let mut val: Vec<u8> = vec![0];
                    if *bool_val{
                        val[0] = 1;
                    }
                    let index_key = format!("{}_{}", attribute.key, bool_val);
                    self.push_index(index_key, val);
                },
                _ =>{
                   // For remaining value don't create index just push it as part of 
                   // trace. 
                }
            }
        }
    }
    
    /// push_index add the given index to inmemory
    fn push_index(&mut self, key: String,val: Vec<u8>){
        if let Some(index) = self.tag_index.get_mut(&key){
            index.push(val);
        } else{
            self.tag_index.insert(key, vec![val]);
        }
    }
}