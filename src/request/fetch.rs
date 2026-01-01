use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::{
    metadata::RecordBatch,
    request::{ErrorCode, IntoResponse, Request, RequestHeader},
    unsigned_varint_decode,
};

#[derive(Debug)]
pub struct FetchRequest {
    header: RequestHeader,
    metadata: Arc<Box<[RecordBatch]>>,
    max_wait: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: Box<[(Uuid, Box<[PartitionRequest]>)]>,
    forgotten_topics: Vec<(Uuid, i32)>,
    rack_id: Bytes,
}

impl FetchRequest {
    pub fn new(req: Request, metadata: Arc<Box<[RecordBatch]>>) -> Self {
        let mut payload = req.payload;
        let max_wait = payload.get_i32();
        let min_bytes = payload.get_i32();
        let max_bytes = payload.get_i32();
        let isolation_level = payload.get_i8();
        let session_id = payload.get_i32();
        let session_epoch = payload.get_i32();
        let topics_len = unsigned_varint_decode(&mut payload);
        let topics = (0..topics_len as usize)
            .into_iter()
            .map(|_| {
                let uuid = Uuid::from_u128(payload.get_u128());
                let partition_len = unsigned_varint_decode(&mut payload);
                let partitions = (0..partition_len as usize)
                    .map(|_| PartitionRequest {
                        partition_id: payload.get_i32(),
                        current_leader_epoch: payload.get_i32(),
                        fetch_offset: payload.get_i64(),
                        last_fetched_epoch: payload.get_i32(),
                        log_start_offset: payload.get_i64(),
                        partition_max_bytes: payload.get_i32(),
                    })
                    .collect::<Vec<PartitionRequest>>();

                (uuid, partitions.into_boxed_slice())
            })
            .collect::<Vec<(Uuid, Box<[PartitionRequest]>)>>();

        let forgotten_topics_len = unsigned_varint_decode(&mut payload);
        let forgotten_topics = (0..forgotten_topics_len as usize)
            .map(|_| {
                let topic_id = Uuid::from_u128(payload.get_u128());
                let partitions = payload.get_i32();
                (topic_id, partitions)
            })
            .collect::<Vec<(Uuid, i32)>>();
        let rack_id_len = unsigned_varint_decode(&mut payload);
        let rack_id = Bytes::copy_from_slice(&payload[..rack_id_len as usize]);
        payload.advance(rack_id_len as usize);

        Self {
            header: req.header,
            metadata,
            max_wait,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics: topics.into_boxed_slice(),
            forgotten_topics,
            rack_id,
        }
    }
}

#[derive(Debug)]
pub struct PartitionRequest {
    partition_id: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

impl IntoResponse for FetchRequest {
    fn response(&self) -> BytesMut {
        let mut content = BytesMut::new();
        if self.topics.is_empty() {
            let throttle_time = 0;
            let error_code = ErrorCode::None as i16;
            let responses_len = 1;

            content.put_i32(self.header.correlation_id);
            content.put_i8(0x00);
            content.put_i32(throttle_time);
            content.put_i16(error_code);
            content.put_i32(self.session_id);
            content.put_i8(responses_len);
            content.put_i8(0x00);
        }

        content
    }
}
