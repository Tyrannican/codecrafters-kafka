use super::{KafkaMessage, Request};
use crate::protocol::{Topic, NULL_VALUE, TAG_BUFFER};

use bytes::{Buf, BufMut};

use std::io::Write;

#[derive(Debug)]
struct DescribeTopicPartitionsRequest {
    pub correlation_id: i32,
    pub client_id: String,
    pub requested_topics: Vec<String>,
    pub partition_limit: i32,
}

impl DescribeTopicPartitionsRequest {
    pub fn new(req: &Request) -> Self {
        let header = &req.header;
        let mut content = req.content.as_slice();
        let c_id = header.correlation_id;

        let client_id_len = content.get_i16();
        let mut client_id = Vec::with_capacity(client_id_len as usize);
        for _ in 0..client_id_len {
            client_id.push(content.get_u8());
        }
        let client_id = String::from_utf8(client_id).expect("should be a valid client-id");
        content.get_u8();

        let topic_arr_len = content.get_i8() - 1;
        let mut topics = Vec::with_capacity(topic_arr_len as usize);
        for _ in 0..topic_arr_len {
            let topic_name_len = content.get_u8() - 1;
            let mut topic_name = Vec::with_capacity(topic_name_len as usize);
            for _ in 0..topic_name_len {
                topic_name.push(content.get_u8());
            }
            topics.push(
                String::from_utf8(topic_name).expect("should be a valid utf-8 encoded string"),
            );
            content.get_u8();
        }
        let partition_limit = content.get_i32();
        let _next = content.get_u8();

        Self {
            correlation_id: c_id,
            client_id,
            requested_topics: topics,
            partition_limit,
        }
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponse {
    pub correlation_id: i32,
    pub throttle_time: i32,
    pub topics: Vec<Topic>,
}

impl DescribeTopicPartitionsResponse {
    pub fn new(req: &Request) -> Self {
        let dtp_req = DescribeTopicPartitionsRequest::new(req);
        let c_id = dtp_req.correlation_id;
        let throttle_time: i32 = 0;

        let mut topics = Vec::with_capacity(dtp_req.requested_topics.len());
        for topic in dtp_req.requested_topics.iter() {
            // TODO: Some check on if the topic is valid
            // For now, they're all Unknown
            topics.push(Topic::unknown(topic));
        }

        Self {
            correlation_id: c_id,
            throttle_time,
            topics,
        }
    }
}

impl KafkaMessage for DescribeTopicPartitionsResponse {
    fn to_bytes(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.put_i32(self.correlation_id);
        body.put_u8(TAG_BUFFER);
        body.put_i32(self.throttle_time);

        let topic_arr_len = (self.topics.len() + 1) as i8;
        body.put_i8(topic_arr_len);
        for topic in self.topics.iter() {
            body.put_i16(topic.error.into());
            let topic_name_len = (topic.name.len() + 1) as i8;
            body.put_i8(topic_name_len);
            body.write(&topic.name.as_bytes())
                .expect("unable to write topic name");
            body.write(&topic.id).expect("unable to write topic id");
            let internal = if topic.internal { 1 } else { 0 };
            body.put_i8(internal);
            let partition_len = (topic.partitions.len() + 1) as i8;
            body.put_i8(partition_len);
            body.put_i32(topic.authorized_operations);
            body.put_u8(TAG_BUFFER);
            body.put_u8(NULL_VALUE);
        }
        body.put_u8(TAG_BUFFER);

        let mut resp = Vec::new();
        resp.put_i32(body.len() as i32);
        resp.extend(body);

        resp
    }
}
