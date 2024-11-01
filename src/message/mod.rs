use anyhow::Result;
use error::KafkaError;
use tokio::io::{AsyncReadExt, BufReader};

pub mod error;

#[derive(Debug)]
pub struct Request {
    pub message_size: i32,
    pub header: MessageHeader,
}

#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

#[derive(Debug)]
pub struct Response {
    pub message_size: i32,
    pub header: MessageHeader,
    pub error: KafkaError,
}

impl Request {
    pub async fn from_bytes(input: &[u8]) -> Result<Self> {
        let mut input = BufReader::new(input);
        let message_size = input.read_i32().await?;
        let api_key = input.read_i16().await?;
        let api_version = input.read_i16().await?;
        let correlation_id = input.read_i32().await?;

        Ok(Self {
            message_size,
            header: MessageHeader {
                api_key,
                api_version,
                correlation_id,
            },
        })
    }
}

impl Response {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(self.message_size.to_be_bytes());
        bytes.extend(self.header.correlation_id.to_be_bytes());
        bytes.extend(self.error.to_bytes());
        bytes
    }
}
