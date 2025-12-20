use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use std::{
    io::{Read, Write},
    net::TcpListener,
};

struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

impl RequestHeader {
    pub fn parse(req: &mut BytesMut) -> Result<Self> {
        let api_key = i16::from_be_bytes(req[..2].try_into()?);
        req.advance(2);
        let api_version = i16::from_be_bytes(req[..2].try_into()?);
        req.advance(2);
        let correlation_id = i32::from_be_bytes(req[..4].try_into()?);

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
        })
    }
}

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = BytesMut::from_iter(vec![0; 1024].iter());
                stream.read(&mut buf).context("read from client")?;
                let _msg_size = i32::from_be_bytes(buf[..4].try_into()?);
                buf.advance(4);
                let header = RequestHeader::parse(&mut buf)?;

                let error_code = if header.api_version < 0 || header.api_key > 4 {
                    35i16.to_be_bytes()
                } else {
                    0i16.to_be_bytes()
                };

                let mut response = Vec::with_capacity(8);
                response.extend(0i32.to_be_bytes());
                response.extend(header.correlation_id.to_be_bytes());
                response.extend(error_code);
                stream.write(&response).context("write to client")?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
