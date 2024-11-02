use bytes::BufMut;

use crate::{
    message::{KafkaMessage, MessageHeader},
    protocol::{ApiKey, KafkaError},
};

fn supported_apis() -> Vec<ApiKey> {
    vec![ApiKey::ApiVersions, ApiKey::DescribeTopicPartitions]
}

#[derive(Debug)]
pub struct ApiVersionResponse {
    correlation_id: i32,
    error: KafkaError,
    api_keys: Vec<ApiKey>,
}

impl ApiVersionResponse {
    pub fn new(req: &MessageHeader) -> Self {
        Self {
            correlation_id: req.correlation_id,
            error: KafkaError::None,
            api_keys: supported_apis(),
        }
    }
}

impl KafkaMessage for ApiVersionResponse {
    fn to_bytes(&self) -> Vec<u8> {
        let mut api_keys = Vec::new();
        let length = self.api_keys.len() + 1;
        for api_key in self.api_keys.iter() {
            let key = *api_key;
            let (min, max) = api_key.supported_versions();
            api_keys.put_i16(key.into());
            api_keys.put_i16(min);
            api_keys.put_i16(max);
            api_keys.put_u8(0x00);
        }
        let mut body = Vec::new();
        body.put_i32(self.correlation_id);
        body.put_i16(self.error.into());
        body.put_i8(length as i8);
        body.extend(api_keys);
        body.put_i32(0);
        body.put_u8(0x00);

        let mut resp = Vec::new();
        resp.put_i32(body.len() as i32);
        resp.extend(body);

        resp
    }
}
