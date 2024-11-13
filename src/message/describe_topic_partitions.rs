use super::{KafkaMessage, Request};
use crate::protocol::KafkaError;

use bytes::{Buf, BufMut};
use uuid::uuid;

use std::io::Write;

// TODO: Fix error with the slice being smaller than expected

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

#[derive(Debug)]
pub struct Partition;

#[derive(Debug)]
pub struct Topic {
    pub error: KafkaError,
    pub id: [u8; 16],
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<Partition>,
    pub authorized_operations: i32,
}

impl Topic {
    pub fn unknown(name: &str) -> Self {
        Self {
            error: KafkaError::UnknownTopicOrPartition,
            name: name.to_owned(),
            id: uuid!("00000000-0000-0000-0000-000000000000").into_bytes(),
            internal: false,
            partitions: vec![],
            authorized_operations: 0,
        }
    }
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
        body.put_i8(0x00);
        body.put_i32(self.throttle_time);
        body.put_i8((self.topics.len() as i8) + 1);
        for topic in self.topics.iter() {
            body.put_i16(topic.error.into());
            body.put_i8(topic.name.len() as i8);
            body.write(topic.name.as_bytes())
                .expect("cannot write topic name as bytes");
            body.write(&topic.id).expect("cannot write topic id");
            if topic.internal {
                body.put_i8(1);
            } else {
                body.put_i8(0);
            }

            body.put_i8((topic.partitions.len() as i8) + 1);
            body.put_i32(topic.authorized_operations);
            body.put_i8(0x00);
        }

        body.put_u8(0xff);
        body.put_i8(0x00);

        let mut resp = Vec::new();
        resp.put_i32(body.len() as i32);
        resp.extend(body);

        resp
    }
}
