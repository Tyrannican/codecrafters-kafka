use crate::message::{
    describe_topic_partitions::DescribeTopicPartitionsResponse, ApiVersionResponse,
    GeneralResponse, KafkaMessage, Request,
};

pub mod api;
pub use api::ApiKey;
pub mod error;
pub use error::KafkaError;
pub mod topic;
pub use topic::{Topic, TopicOp};

pub const TAG_BUFFER: u8 = 0x00;
pub const NULL_VALUE: u8 = 0xff;

#[derive(Debug)]
pub struct RequestParser {
    pub req: Request,
}

impl RequestParser {
    pub fn new(req: Request) -> Self {
        Self { req }
    }

    pub fn parse(&self) -> Box<dyn KafkaMessage + Send> {
        let header = &self.req.header;
        let api_key = ApiKey::from(header.api_key);

        if api_key == ApiKey::Unsupported || !api_key.is_supported(header.api_version) {
            return Box::new(GeneralResponse::new(
                header.correlation_id,
                KafkaError::UnsupportedVersion,
            ));
        }

        match api_key {
            ApiKey::ApiVersions => Box::new(ApiVersionResponse::new(header)),
            ApiKey::DescribeTopicPartitions => {
                Box::new(DescribeTopicPartitionsResponse::new(&self.req))
            }
            _ => unreachable!("should not be possible"),
        }
    }
}
