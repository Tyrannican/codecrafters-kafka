use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

mod message;
mod protocol;

use message::Request;
use protocol::RequestParser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9092").await?;

    loop {
        match listener.accept().await {
            Ok((mut client, _)) => {
                let mut buf = [0; 1024];
                client.read(&mut buf).await?;
                let req = Request::from_bytes(&buf).await?;
                let resp = RequestParser::new(req).parse();

                let resp_bytes = resp.to_bytes();
                client.write(&resp_bytes).await?;
            }
            Err(e) => anyhow::bail!(e),
        }
    }
}
