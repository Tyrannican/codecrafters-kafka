use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = BytesMut::from_iter(vec![0; 1024].iter());
                stream.read(&mut buf).context("read from client")?;
                let _msg_size = i32::from_be_bytes(buf[..4].try_into()?);
                buf.advance(4);
                let _api_key = i16::from_be_bytes(buf[..2].try_into()?);
                buf.advance(2);
                let _api_version = i16::from_be_bytes(buf[..2].try_into()?);
                buf.advance(2);
                let correlation_id = i32::from_be_bytes(buf[..4].try_into()?);

                let mut response = Vec::with_capacity(8);
                response.extend(0i32.to_be_bytes());
                response.extend(correlation_id.to_be_bytes());
                stream.write(&response).context("write to client")?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
