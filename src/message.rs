use anyhow::Result;
use tokio::io::{AsyncReadExt, BufReader};

#[derive(Debug)]
pub struct Request {
    pub message_size: i32,
    pub header: RequestHeader,
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

#[derive(Debug)]
pub struct Response {
    pub message_size: i32,
    pub correlation_id: i32,
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
            header: RequestHeader {
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
        bytes.extend(self.correlation_id.to_be_bytes());
        bytes
    }
}
