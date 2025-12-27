use anyhow::{Context, Result};
use codecrafters_kafka::server::{ConnectionHandler, Server};
use kanal::unbounded_async;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9092")
        .await
        .context("starting server")?;

    let mut server = Server::new();
    let (tx, rx) = unbounded_async();
    server.start(rx);

    loop {
        let (stream, _) = listener.accept().await?;
        let mut handler = ConnectionHandler::new(stream, tx.clone());
        tokio::task::spawn(async move {
            if let Err(err) = handler.handle_connection().await {
                eprintln!("connection error occurred: {err:#?}");
            }
        });
    }
}
