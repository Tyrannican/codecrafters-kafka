use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    metadata::RecordBatch,
    request::{ErrorCode, IntoResponse, Request, RequestHeader},
    unsigned_varint_decode,
};

use std::sync::Arc;

pub struct DescribeTopicsRequest {
    pub header: RequestHeader,
    pub topic_names: Vec<Bytes>,
    pub partition_limit: i32,
    pub cursor: u8,
    pub tags: i8,
    metadata: Arc<Box<[RecordBatch]>>,
}

impl DescribeTopicsRequest {
    pub fn new(request: Request, metadata: Arc<Box<[RecordBatch]>>) -> Self {
        let Request {
            header,
            mut payload,
            ..
        } = request;

        let len = unsigned_varint_decode(&mut payload);
        let mut topic_names = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let topic_len = unsigned_varint_decode(&mut payload);
            let topic_len = topic_len as usize;
            topic_names.push(Bytes::from_iter(payload[..topic_len].to_vec()));
            payload.advance(topic_len);
            let tags = payload.get_i8();
            assert_eq!(tags, 0x00);
        }
        topic_names.sort();

        let partition_limit = payload.get_i32();
        let cursor = payload.get_u8();
        let tags = payload.get_i8();

        Self {
            header,
            topic_names,
            partition_limit,
            cursor,
            tags,
            metadata,
        }
    }

    pub fn topic_response(&self, topic_name: &Bytes) -> BytesMut {
        let mut content = BytesMut::new();

        let is_internal: i8 = 0x00;
        let authorised_ops: i32 = 0x00;
        let topic_len = (topic_name.len() + 1) as u8;

        for record in self.metadata.iter() {
            if let Some(partitions) = record.get_topic_partitions(topic_name) {
                let uuid = record
                    .get_topic_uuid(topic_name)
                    .expect("this is guaranteed to be here");

                content.put_i16(ErrorCode::None as i16);
                content.put_u8(topic_len);
                content.extend_from_slice(&topic_name[..]);
                content.extend_from_slice(&uuid.into_bytes());
                content.put_i8(is_internal);

                let partition_arr_len = (partitions.len() + 1) as u8;
                content.put_u8(partition_arr_len);

                for partition in partitions {
                    content.put_i16(ErrorCode::None as i16);
                    content.put_i32(partition.partition_id);
                    content.put_i32(partition.leader);
                    content.put_i32(partition.leader_epoch);

                    let repl_node_len = (partition.replication_ids.len() + 1) as u8;
                    content.put_u8(repl_node_len);
                    for repl_node in &partition.replication_ids {
                        content.put_i32(*repl_node);
                    }

                    let isr_node_len = (partition.in_sync_replica_ids.len() + 1) as u8;
                    content.put_u8(isr_node_len);
                    for isr_node in &partition.in_sync_replica_ids {
                        content.put_i32(*isr_node);
                    }

                    // Eligble leader replicas (Compact Array)
                    content.put_u8(0x01);

                    // Last Known Eligble Leader Replica (Compact Array)
                    content.put_u8(0x01);

                    // Offline replicas (Compact Array)
                    content.put_u8(0x01);

                    // Tags
                    content.put_u8(0x00);
                }

                content.put_i32(authorised_ops);
                content.put_i8(0x00);

                return content;
            }
        }

        // No topic record present
        let empty_topic_id = vec![0; 16];
        let partition_arr_len: i8 = 0x00;
        content.put_i16(ErrorCode::UnknownTopicOrPartition as i16);
        content.put_u8(topic_len);
        content.extend_from_slice(&topic_name[..]);
        content.extend_from_slice(&empty_topic_id);
        content.put_i8(is_internal);
        content.put_i8(partition_arr_len);
        content.put_i32(authorised_ops);
        content.put_i8(0x00);

        content
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
        content.put_u8((self.topic_names.len() + 1) as u8);

        for topic_name in &self.topic_names {
            let topic = self.topic_response(topic_name);
            content.extend_from_slice(&topic[..]);
        }

        content.put_i8(-1);
        content.put_i8(tag);

        content
    }
}
