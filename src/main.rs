use anyhow::{Context, Result};
use std::{io::Write, net::TcpListener};

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut response = Vec::with_capacity(8);
                response.extend(0i32.to_be_bytes());
                response.extend(7i32.to_be_bytes());
                stream
                    .write(&response)
                    .context("sending response to client")?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
