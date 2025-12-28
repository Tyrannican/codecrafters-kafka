use bytes::{Buf, Bytes};

use crate::{request::RequestHeader, varint_decode};

pub struct DescribeTopicsRequest {
    pub header: RequestHeader,
    pub topic_names: Vec<Bytes>,
    pub partition_limit: i32,
    pub cursor: u8,
    pub tags: i8,
}

impl DescribeTopicsRequest {
    pub fn new(header: RequestHeader, content: Bytes) -> Self {
        let mut payload = content;
        let (len, consumed) = varint_decode(&payload[..]);
        payload.advance(consumed);
        let mut topics = Vec::with_capacity((len - 1) as usize);
        for _ in 0..len - 1 {
            let (topic_len, consumed) = varint_decode(&payload[..]);
            let topic_len = (topic_len - 1) as usize;
            payload.advance(consumed);
            topics.push(Bytes::from_iter(payload[..topic_len].to_vec()));
            payload.advance(topic_len);
            let tags = payload.get_i8();
            assert_eq!(tags, 0x00);
        }

        eprintln!("Topics: {topics:?}");
        let partition_limit = payload.get_i32();
        eprintln!("Limit: {partition_limit:?}");
        let cursor = payload.get_u8();
        eprintln!("Cursor: {cursor:?}");
        let tags = payload.get_i8();
        eprintln!("Tags: {tags:?}");

        Self {
            header,
            topic_names: topics,
            partition_limit,
            cursor,
            tags,
        }
    }
}
