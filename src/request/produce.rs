#![allow(dead_code)]

use crate::{
    metadata::RecordBatch,
    request::{ErrorCode, IntoResponse, Request, RequestHeader},
    unsigned_varint_decode, unsigned_varint_encode,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::{path::PathBuf, sync::Arc};

const LOG_DIR: &str = "/tmp/kraft-combined-logs";

#[derive(Debug)]
pub struct ProduceRequest {
    header: RequestHeader,
    metadata: Arc<Box<[RecordBatch]>>,
    transactional_id: Bytes,
    required_acknowledgements: i16,
    timeout: i32,
    topics: Box<[(Bytes, Box<[Partition]>)]>,
}

impl ProduceRequest {
    pub fn new(req: Request, metadata: Arc<Box<[RecordBatch]>>) -> Self {
        let mut payload = req.payload;
        let txn_id_len = unsigned_varint_decode(&mut payload);
        let transactional_id = Bytes::copy_from_slice(&payload[..txn_id_len as usize]);
        payload.advance(txn_id_len as usize);
        let required_acks = payload.get_i16();
        let timeout = payload.get_i32();

        let topics_len = unsigned_varint_decode(&mut payload);
        let topics: Vec<(Bytes, Box<[Partition]>)> = (0..topics_len)
            .map(|_| {
                let topic_name_len = unsigned_varint_decode(&mut payload);
                let topic_name = Bytes::copy_from_slice(&payload[..topic_name_len as usize]);
                payload.advance(topic_name_len as usize);

                let partitions_arr_len = unsigned_varint_decode(&mut payload);
                let partitions: Vec<Partition> = (0..partitions_arr_len)
                    .map(|_| {
                        let index = payload.get_i32();
                        let records_len = unsigned_varint_decode(&mut payload);
                        let records = Bytes::copy_from_slice(&payload[..records_len as usize]);
                        payload.advance(records_len as usize);
                        // Skip TAG buffer
                        payload.get_i8();

                        Partition {
                            index,
                            record_batches: records,
                        }
                    })
                    .collect();

                (topic_name, partitions.into_boxed_slice())
            })
            .collect();

        Self {
            header: req.header,
            metadata,
            transactional_id,
            required_acknowledgements: required_acks,
            timeout,
            topics: topics.into_boxed_slice(),
        }
    }

    pub fn invalid_topic(&self, content: &mut BytesMut, idx: i32) {
        content.put_i32(idx);
        content.put_i16(ErrorCode::UnknownTopicOrPartition as i16);
        // // Base offset
        content.put_i64(-1);
        // // Log append time
        content.put_i64(-1);
        // // Log start offset
        content.put_i64(-1);
        // // Record errors array
        unsigned_varint_encode(content, 0);
        // // Error Message
        content.put_i8(0x00);
        // // Tags
        content.put_i8(0x00);
    }

    fn write_record_batch(&self, topic_name: &Bytes, partition: &Partition) {
        let root = PathBuf::from(LOG_DIR);
        let name = String::from_utf8(topic_name.to_vec()).expect("guaranteed to be utf-8");
        let dir = root.join(format!("{name}-{}", partition.index));
        if !dir.exists() {
            std::fs::create_dir_all(&dir).expect("directory creation");
        }

        let path = dir.join("00000000000000000000.log");
        std::fs::write(path, partition.record_batches.clone()).expect("should be valid");
    }
}

#[derive(Debug)]
pub struct Partition {
    index: i32,
    record_batches: Bytes,
}

impl IntoResponse for ProduceRequest {
    fn response(&self) -> BytesMut {
        let mut content = BytesMut::new();
        let throttle_time = 0;
        content.put_i32(self.header.correlation_id);
        content.put_i8(0x00);

        unsigned_varint_encode(&mut content, self.topics.len());
        for topic in self.topics.iter() {
            let (topic_name, partitions) = topic;
            unsigned_varint_encode(&mut content, topic_name.len());
            content.put(topic_name.clone());
            unsigned_varint_encode(&mut content, partitions.len());
            for partition in partitions.iter() {
                let matches: Vec<&RecordBatch> = self
                    .metadata
                    .iter()
                    .filter_map(|record| match record.get_topic_uuid(topic_name) {
                        Some(uuid) => {
                            if record.valid_partition(&uuid, partition.index) {
                                Some(record)
                            } else {
                                None
                            }
                        }
                        None => None,
                    })
                    .collect();

                if matches.is_empty() {
                    self.invalid_topic(&mut content, partition.index);
                } else {
                    self.write_record_batch(topic_name, partition);
                    content.put_i32(partition.index);
                    content.put_i16(ErrorCode::None as i16);
                    // // Base offset
                    content.put_i64(0);
                    // // Log append time
                    content.put_i64(-1);
                    // // Log start offset
                    content.put_i64(0);
                    // // Record errors array
                    unsigned_varint_encode(&mut content, 0);
                    // // Error Message
                    content.put_i8(0x00);
                    // // Tags
                    content.put_i8(0x00);
                }
            }

            content.put_i8(0x00);
        }

        content.put_i32(throttle_time);
        content.put_i8(0x00);

        content
    }
}
