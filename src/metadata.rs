#![allow(dead_code)]

use crate::{unsigned_varint_decode, varint_decode};
use bytes::{Buf, Bytes};
use uuid::Uuid;

use std::collections::HashMap;

const METADATA_FILE: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn parse_metadata() -> Box<[RecordBatch]> {
    let mut content = match std::fs::read(METADATA_FILE) {
        Ok(content) => Bytes::from(content),
        Err(_) => return Vec::new().into_boxed_slice(),
    };

    let mut batches = Vec::new();
    while content.has_remaining() {
        let _ = content.get_i64();
        let batch_len = content.get_i32();
        let mut batch = Bytes::copy_from_slice(&content[..batch_len as usize]);
        let batch_header = RecordBatchHeader::new(&mut batch);
        let total_records = batch.get_i32();

        let mut topics = HashMap::new();
        let mut current_topic_id = None;
        for _ in (0..total_records).into_iter() {
            let record = Record::new(&mut batch);
            match record.record_type {
                RecordType::Feature(_) => {} // TODO: Deal with this if required
                RecordType::Topic(topic) => {
                    if current_topic_id.is_none() {
                        current_topic_id = Some(topic.uuid);
                    }
                }
                RecordType::Partition(partition) => {
                    if let Some(uuid) = current_topic_id
                        && partition.uuid == uuid
                    {
                        let entry = topics.entry(uuid).or_insert_with(|| Vec::new());
                        entry.push(partition);
                    }
                }
            }
        }

        batches.push(RecordBatch {
            header: batch_header,
            topics,
        });

        content.advance(batch_len as usize);
    }
    eprintln!("{batches:?}");

    batches.into_boxed_slice()
}

#[derive(Debug)]
pub struct RecordBatch {
    header: RecordBatchHeader,
    topics: HashMap<Uuid, Vec<PartitionRecord>>,
}

#[derive(Debug)]
pub struct RecordBatchHeader {
    leader_epoch: i32,
    magic: i8,
    crc: i32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

impl RecordBatchHeader {
    pub fn new(batch: &mut Bytes) -> Self {
        Self {
            leader_epoch: batch.get_i32(),
            magic: batch.get_i8(),
            crc: batch.get_i32(),
            attributes: batch.get_i16(),
            last_offset_delta: batch.get_i32(),
            base_timestamp: batch.get_i64(),
            max_timestamp: batch.get_i64(),
            producer_id: batch.get_i64(),
            producer_epoch: batch.get_i16(),
            base_sequence: batch.get_i32(),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    attributes: i8,
    timestamp_delta: i32,
    offset_delta: i32,
    key: Bytes,
    record_type: RecordType,
}

impl Record {
    pub fn new(buf: &mut Bytes) -> Self {
        let record_length = varint_decode(buf);
        let mut record = Bytes::copy_from_slice(&buf[..record_length as usize]);
        buf.advance(record_length as usize);

        let attributes = record.get_i8();
        let timestamp_delta = varint_decode(&mut record);
        let offset_delta = varint_decode(&mut record);
        let key_len = varint_decode(&mut record);
        let key = if key_len < 0 {
            Bytes::new()
        } else {
            let key = Bytes::copy_from_slice(&record[..key_len as usize]);
            record.advance(key_len as usize);
            key
        };

        let value_len = varint_decode(&mut record);
        let value = if value_len < 0 {
            Bytes::new()
        } else {
            let value = Bytes::copy_from_slice(&record[..value_len as usize]);
            record.advance(value_len as usize);
            value
        };
        let record_type = RecordType::new(value);
        assert_eq!(unsigned_varint_decode(&mut record), 0);

        Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            record_type,
        }
    }
}

#[derive(Debug)]
pub enum RecordType {
    Feature(FeatureRecord),
    Topic(TopicRecord),
    Partition(PartitionRecord),
}

impl RecordType {
    pub fn new(mut buf: Bytes) -> Self {
        let _ = buf.get_i8();
        let record_type = buf.get_i8();

        match record_type {
            2 => Self::Topic(TopicRecord::new(buf)),
            3 => Self::Partition(PartitionRecord::new(buf)),
            12 => Self::Feature(FeatureRecord::new(buf)),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct FeatureRecord {
    version: i8,
    name: Bytes,
    feature_level: i16,
    tags: i8,
}

impl FeatureRecord {
    pub fn new(mut buf: Bytes) -> Self {
        let version = buf.get_i8();
        let name_len = unsigned_varint_decode(&mut buf);
        let name = if name_len == 0 {
            Bytes::new()
        } else {
            let name = Bytes::copy_from_slice(&buf[..name_len as usize]);
            buf.advance(name_len as usize);
            name
        };
        let feature_level = buf.get_i16();
        let tags = buf.get_i8();

        Self {
            version,
            name,
            feature_level,
            tags,
        }
    }
}

#[derive(Debug)]
pub struct TopicRecord {
    version: i8,
    topic_name: Bytes,
    uuid: Uuid,
    tags: i8,
}

impl TopicRecord {
    pub fn new(mut buf: Bytes) -> Self {
        let version = buf.get_i8();
        let name_len = unsigned_varint_decode(&mut buf);
        let topic_name = if name_len == 0 {
            Bytes::new()
        } else {
            let name = Bytes::copy_from_slice(&buf[..name_len as usize]);
            buf.advance(name_len as usize);
            name
        };
        let uuid = Uuid::from_u128(buf.get_u128());
        let tags = buf.get_i8();

        Self {
            version,
            topic_name,
            uuid,
            tags,
        }
    }
}

#[derive(Debug)]
pub struct PartitionRecord {
    version: i8,
    partition_id: i32,
    uuid: Uuid,
    replication_ids: Box<[i32]>,
    in_sync_replica_ids: Box<[i32]>,
    removing_replica_ids: Box<[i32]>,
    adding_replica_ids: Box<[i32]>,
    leader: i32,
    leader_epoch: i32,
    partition_epoch: i32,
    directories: Box<[Uuid]>,
    tags: i8,
}

impl PartitionRecord {
    pub fn new(mut buf: Bytes) -> Self {
        let version = buf.get_i8();
        let partition_id = buf.get_i32();
        let uuid = Uuid::from_u128(buf.get_u128());
        let repl_arr_len = unsigned_varint_decode(&mut buf);
        let repl_id_arr: Vec<i32> = (0..repl_arr_len).map(|_| buf.get_i32()).collect();
        let in_sync_arr_len = unsigned_varint_decode(&mut buf);
        let in_sync_arr: Vec<i32> = (0..in_sync_arr_len).map(|_| buf.get_i32()).collect();
        let rem_repl_arr_len = unsigned_varint_decode(&mut buf);
        let rem_repl_arr: Vec<i32> = (0..rem_repl_arr_len).map(|_| buf.get_i32()).collect();
        let add_repl_arr_len = unsigned_varint_decode(&mut buf);
        let add_repl_arr: Vec<i32> = (0..add_repl_arr_len).map(|_| buf.get_i32()).collect();
        let leader = buf.get_i32();
        let leader_epoch = buf.get_i32();
        let partition_epoch = buf.get_i32();
        let dir_len = unsigned_varint_decode(&mut buf);
        let dirs: Vec<Uuid> = (0..dir_len)
            .map(|_| Uuid::from_u128(buf.get_u128()))
            .collect();

        let tags = buf.get_i8();

        Self {
            version,
            partition_id,
            uuid,
            replication_ids: repl_id_arr.into_boxed_slice(),
            in_sync_replica_ids: in_sync_arr.into_boxed_slice(),
            removing_replica_ids: rem_repl_arr.into_boxed_slice(),
            adding_replica_ids: add_repl_arr.into_boxed_slice(),
            leader,
            leader_epoch,
            partition_epoch,
            directories: dirs.into_boxed_slice(),
            tags,
        }
    }
}
