use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9092").await?;

    loop {
        match listener.accept().await {
            Ok((mut client, _)) => {
                let message_size: i32 = 12;
                let correlation_id: i32 = 7;
                let mut response = vec![];
                response.extend(message_size.to_be_bytes());
                response.extend(correlation_id.to_be_bytes());
                client.write(&response).await?;
            }
            Err(e) => anyhow::bail!(e),
        }
    }
}
