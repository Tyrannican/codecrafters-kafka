use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    request::{ErrorCode, IntoResponse, Request, RequestHeader},
    varint_decode,
};

pub struct DescribeTopicsRequest {
    pub header: RequestHeader,
    pub topic_names: Vec<Bytes>,
    pub partition_limit: i32,
    pub cursor: u8,
    pub tags: i8,
}

impl DescribeTopicsRequest {
    pub fn new(request: Request) -> Self {
        let Request {
            header,
            mut payload,
            ..
        } = request;

        let len = varint_decode(&mut payload);
        let mut topics = Vec::with_capacity((len - 1) as usize);
        for _ in 0..len - 1 {
            let topic_len = varint_decode(&mut payload);
            let topic_len = (topic_len - 1) as usize;
            topics.push(Bytes::from_iter(payload[..topic_len].to_vec()));
            payload.advance(topic_len);
            let tags = payload.get_i8();
            assert_eq!(tags, 0x00);
        }

        let partition_limit = payload.get_i32();
        let cursor = payload.get_u8();
        let tags = payload.get_i8();

        Self {
            header,
            topic_names: topics,
            partition_limit,
            cursor,
            tags,
        }
    }
}

impl IntoResponse for DescribeTopicsRequest {
    fn response(&self) -> bytes::BytesMut {
        let mut content = BytesMut::new();
        let throttle: i32 = 0;
        let tag: i8 = 0;
        content.put_i32(self.header.correlation_id);
        content.put_i8(tag);

        content.put_i32(throttle);
        content.put_i8((self.topic_names.len() + 1) as i8);

        // TODO: Derive a Topic
        let topic_id = vec![0; 16];
        let is_internal: i8 = 0x00;
        let partition_arr_len: i8 = 0x00;
        let authorised_ops: i32 = 0;
        for topic in self.topic_names.iter() {
            // TODO: Get topic and perform checks
            let topic_len = (topic.len() + 1) as i8;
            content.put_i16(ErrorCode::UnknownTopicOrPartition as i16);
            content.put_i8(topic_len);
            content.extend_from_slice(&topic[..]);
            content.extend_from_slice(&topic_id);
            content.put_i8(is_internal);
            content.put_i8(partition_arr_len);
            content.put_i32(authorised_ops);
            content.put_i8(tag);
        }
        // content.put_i32(request.partition_limit);
        content.put_u8(self.cursor);
        content.put_i8(tag);

        content
    }
}
