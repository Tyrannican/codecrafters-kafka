use anyhow::Result;
use bytes::{Buf, BufMut};

use crate::protocol::error::KafkaError;
use std::io::Read;

pub mod api_versions;
pub use api_versions::ApiVersionResponse;

pub trait KafkaMessage {
    fn to_bytes(&self) -> Vec<u8>;
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Request {
    pub message_size: i32,
    pub header: MessageHeader,
    pub content: Vec<u8>,
}

impl Request {
    pub async fn from_bytes(mut input: &[u8]) -> Result<Self> {
        let message_size = input.get_i32();
        let api_key = input.get_i16();
        let api_version = input.get_i16();
        let correlation_id = input.get_i32();
        let mut content = vec![];
        let mut reader = input.reader();
        reader.read_to_end(&mut content)?;

        Ok(Self {
            message_size,
            header: MessageHeader {
                api_key,
                api_version,
                correlation_id,
            },
            content,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

#[derive(Debug)]
pub struct GeneralResponse {
    pub message_size: i32,
    pub error: KafkaError,
    pub correlation_id: i32,
}

impl GeneralResponse {
    pub fn new(c_id: i32, error: KafkaError) -> Self {
        Self {
            message_size: 6,
            error,
            correlation_id: c_id,
        }
    }
}

impl KafkaMessage for GeneralResponse {
    fn to_bytes(&self) -> Vec<u8> {
        let mut resp = Vec::new();
        resp.put_i32(self.message_size);
        resp.put_i32(self.correlation_id);
        resp.put_i16(self.error.into());

        resp
    }
}
