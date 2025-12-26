use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[repr(u16)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ApiType {
    ApiVersions = 18,
}

impl ApiType {
    pub fn supported_versions(&self) -> (i16, i16) {
        match self {
            Self::ApiVersions => (0, 4),
        }
    }
}

impl TryFrom<i16> for ApiType {
    type Error = std::io::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(Self::ApiVersions),
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
    UnsupportedVersion = 35,
}

#[derive(Debug)]
pub struct Request {
    message_size: i32,
    header: RequestHeader,
    payload: Bytes,
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

    pub fn response(&self) -> BytesMut {
        let mut response = BytesMut::new();
        let api_key = self.header.api_key;
        let resp = match api_key {
            ApiType::ApiVersions => {
                let mut inner = BytesMut::new();
                let c_id = self.header.correlation_id;
                let error_code = self.header.version_supported();
                let (min, max) = api_key.supported_versions();

                inner.put_i32(c_id);
                inner.put_i16(error_code as i16);
                inner.put_i8(2);
                inner.put_i16(api_key as i16);
                inner.put_i16(min);
                inner.put_i16(max);
                inner.put_i8(0);
                inner.put_i32(0);
                inner.put_i8(0);

                inner
            }
        };

        response.put_i32(resp.len() as i32);
        response.extend_from_slice(&resp[..]);

        response
    }
}

#[derive(Debug, Clone)]
struct RequestHeader {
    api_key: ApiType,
    api_version: i16,
    correlation_id: i32,
    client_id: String,
    tag_buffer: i8,
}

impl RequestHeader {
    fn parse(buf: &mut BytesMut) -> Result<Self> {
        let api_key = ApiType::try_from(buf.get_i16()).context("parsing api key type")?;
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();

        let id_len = buf.get_i16();
        let client_id = if id_len != -1 {
            let s = String::from_utf8(buf[..id_len as usize].to_vec())
                .context("extracting client id")?;
            buf.advance(id_len as usize);
            s
        } else {
            String::from("")
        };

        // TODO: Parse Tag Buffers
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
