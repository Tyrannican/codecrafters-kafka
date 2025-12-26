use anyhow::Result;
use codecrafters_kafka::server::*;
use crossbeam_channel::unbounded;
use std::net::TcpListener;

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    let mut server = Server::new();
    let (tx, rx) = unbounded();
    server.start(rx);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut handler = ConnectionHandler::new(stream, tx.clone());
                std::thread::spawn(move || {
                    if let Err(err) = handler.handle_connection() {
                        eprintln!("connection error occurred: {err:#?}");
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
