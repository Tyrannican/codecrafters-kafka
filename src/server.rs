use super::message::Request;
use anyhow::{Context, Result};
use bytes::BytesMut;
use crossbeam_channel::{Receiver, Sender, unbounded};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread::JoinHandle,
};

const WORKER_COUNT: usize = 5;
pub type ServerRequest = (Request, Sender<BytesMut>);

pub struct ConnectionHandler {
    stream: TcpStream,
    msg_sender: Sender<ServerRequest>,
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream, msg_sender: Sender<ServerRequest>) -> Self {
        Self { stream, msg_sender }
    }

    pub fn handle_connection(&mut self) -> Result<()> {
        loop {
            let mut buf = BytesMut::from_iter(vec![0; 4096].into_iter());
            let n = self
                .stream
                .read(&mut buf)
                .context("reading client request")?;

            if n == 0 {
                break;
            }

            let (tx, rx) = unbounded();
            let request = Request::parse(buf).context("parsing incoming request")?;
            self.msg_sender
                .send((request, tx))
                .context("sending request to server")?;

            if let Ok(response) = rx.recv() {
                self.stream
                    .write(&response[..])
                    .context("sending response back to client")?;
            }
        }

        Ok(())
    }
}

pub struct Server {
    worker_count: usize,
    pool: HashMap<usize, JoinHandle<Result<(), anyhow::Error>>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            worker_count: WORKER_COUNT,
            pool: HashMap::new(),
        }
    }

    pub fn start(&mut self, receiver: Receiver<ServerRequest>) {
        for i in 0..self.worker_count {
            let rx = receiver.clone();
            let mut worker = ServerWorker::new(rx);
            let handle = std::thread::spawn(move || worker.start());
            self.pool.insert(i, handle);
        }
    }
}

pub struct ServerWorker {
    receiver: Receiver<ServerRequest>,
}

impl ServerWorker {
    pub fn new(rx: Receiver<ServerRequest>) -> Self {
        Self { receiver: rx }
    }

    pub fn start(&mut self) -> Result<()> {
        while let Ok((request, responder)) = self.receiver.recv() {
            let response = request.response();
            responder
                .send(response)
                .context("sending response to client")?;
        }

        Ok(())
    }
}
