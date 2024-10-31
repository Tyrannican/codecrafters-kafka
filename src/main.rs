use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9092").await?;

    while let Ok((_client, _)) = listener.accept().await {
        println!("Client accepted");
    }

    Ok(())
}
