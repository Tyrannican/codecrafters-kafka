pub mod api_versions;
pub mod describe_topics;
pub mod fetch;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub trait IntoResponse {
    fn response(&self) -> BytesMut;
}

#[repr(u16)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ApiType {
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl ApiType {
    pub fn supported_versions(&self) -> (i16, i16) {
        match self {
            Self::Fetch => (0, 16),
            Self::ApiVersions => (0, 4),
            Self::DescribeTopicPartitions => (0, 0),
        }
    }

    pub fn metadata(&self, buf: &mut BytesMut) {
        let (min, max) = self.supported_versions();

        // Api Key
        buf.put_i16(*self as i16);
        // Min supported version
        buf.put_i16(min);

        // Max supported version
        buf.put_i16(max);

        // Tag buffer
        buf.put_i8(0x00);
    }
}

impl TryFrom<i16> for ApiType {
    type Error = std::io::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Fetch),
            18 => Ok(Self::ApiVersions),
            75 => Ok(Self::DescribeTopicPartitions),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid api type: {value}"),
            )),
        }
    }
}

#[repr(i16)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    Unknown = -1,
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    UnknownTopicId = 100,
}

#[derive(Debug)]
pub struct Request {
    pub message_size: i32,
    pub header: RequestHeader,
    pub payload: Bytes,
}

impl Request {
    pub fn parse(mut buf: BytesMut) -> Result<Self> {
        let message_size = buf.get_i32();
        let header = RequestHeader::parse(&mut buf).context("parse request header")?;
        let payload = Bytes::from(buf.clone());

        Ok(Self {
            message_size,
            header,
            payload,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: ApiType,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Bytes,
    pub tag_buffer: i8,
}

impl RequestHeader {
    fn parse(buf: &mut BytesMut) -> Result<Self> {
        let api_key = ApiType::try_from(buf.get_i16()).context("parsing api key type")?;
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();

        let id_len = buf.get_i16();
        let client_id = if id_len != -1 {
            let mut c_id = vec![0; id_len as usize];
            c_id.copy_from_slice(&buf[..id_len as usize]);
            let c_id = Bytes::from(c_id);
            buf.advance(id_len as usize);
            c_id
        } else {
            Bytes::new()
        };

        let tag_buffer = buf.get_i8();
        assert_eq!(tag_buffer, 0x00);

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tag_buffer,
        })
    }

    pub fn version_supported(&self) -> ErrorCode {
        let (min, max) = self.api_key.supported_versions();
        if self.api_version >= min && self.api_version <= max {
            ErrorCode::None
        } else {
            ErrorCode::UnsupportedVersion
        }
    }
}
