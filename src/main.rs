use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

mod message;
use message::{error::KafkaError, Request, Response};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9092").await?;

    loop {
        match listener.accept().await {
            Ok((mut client, _)) => {
                let mut buf = [0; 1024];
                client.read(&mut buf).await?;
                let req = Request::from_bytes(&buf).await?;

                let resp = if req.header.api_version < 0 || req.header.api_version > 4 {
                    Response {
                        message_size: 35,
                        header: req.header.clone(),
                        error: KafkaError::UNSUPPORTED_VERSION,
                    }
                } else {
                    Response {
                        message_size: 35,
                        header: req.header.clone(),
                        error: KafkaError::UNSUPPORTED_VERSION,
                    }
                };

                client.write(&resp.to_bytes()).await?;
            }
            Err(e) => anyhow::bail!(e),
        }
    }
}
