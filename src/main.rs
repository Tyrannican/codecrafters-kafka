use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use codecrafters_kafka::{ApiType, ErrorCode, varint_encode};
use std::{
    io::{Read, Write},
    net::TcpListener,
};

#[derive(Debug)]
struct Request {
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
        match api_key {
            ApiType::ApiVersions => {
                let c_id = self.header.correlation_id;
                let error_code = self.header.version_supported();
                let (min, max) = api_key.supported_versions();

                response.put_i32(c_id);
                response.put_i16(error_code as i16);
                response.put_i8(2);
                response.put_i16(api_key as i16);
                response.put_i16(min);
                response.put_i16(max);
                response.put_i8(0);
                response.put_i32(0);
                response.put_i8(0);
            }
        }

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

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = BytesMut::from_iter(vec![0u8; 1024].into_iter());
                let n = stream.read(&mut buf).context("read from client")?;
                buf.truncate(n);
                let request = Request::parse(buf).context("parse request")?;
                eprintln!("Request: {request:?}");
                eprintln!("Varint Encode: {:?}", varint_encode(2));
                let resp = request.response();
                eprintln!("Response length: {}", resp.len());

                let mut response = BytesMut::new();
                response.put_i32(resp.len() as i32);
                response.extend_from_slice(&resp[..]);
                stream.write(&response).context("write to client")?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
