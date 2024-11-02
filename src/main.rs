use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod message;
mod protocol;

use message::Request;
use protocol::RequestParser;

async fn handler(mut client: TcpStream) -> anyhow::Result<()> {
    loop {
        let mut buf = [0; 1024];
        client.read(&mut buf).await?;
        let req = Request::from_bytes(&buf).await?;
        let resp = RequestParser::new(req).parse();
        client.write(&resp.to_bytes()).await?;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9092").await?;

    loop {
        match listener.accept().await {
            Ok((client, _)) => {
                tokio::task::spawn(async move { handler(client).await });
            }
            Err(e) => anyhow::bail!(e),
        }
    }
}
