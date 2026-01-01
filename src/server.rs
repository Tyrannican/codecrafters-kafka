use crate::{
    metadata::{RecordBatch, parse_metadata},
    request::{
        ApiType, IntoResponse, api_versions::ApiVersionsRequest,
        describe_topics::DescribeTopicsRequest, fetch::FetchRequest,
    },
};

use super::request::Request;
use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use kanal::{AsyncReceiver, AsyncSender, unbounded_async};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
};

const WORKER_COUNT: usize = 10;
pub type ServerRequest = (Request, AsyncSender<BytesMut>);

pub struct ConnectionHandler {
    stream: TcpStream,
    msg_sender: AsyncSender<ServerRequest>,
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream, msg_sender: AsyncSender<ServerRequest>) -> Self {
        Self { stream, msg_sender }
    }

    pub async fn handle_connection(&mut self) -> Result<()> {
        loop {
            let mut buf = BytesMut::from_iter(vec![0; 4096].into_iter());
            let n = self
                .stream
                .read(&mut buf)
                .await
                .context("reading client request")?;

            if n == 0 {
                break;
            }

            buf.truncate(n);
            let (tx, rx) = unbounded_async();
            let request = Request::parse(buf).context("parsing incoming request")?;
            self.msg_sender
                .send((request, tx))
                .await
                .context("sending request to server")?;

            if let Ok(response) = rx.recv().await {
                self.stream
                    .write(&response[..])
                    .await
                    .context("sending response back to client")?;
            }
        }

        Ok(())
    }
}

pub struct Server {
    worker_count: usize,
    metadata: Arc<Box<[RecordBatch]>>,
    pool: HashMap<usize, JoinHandle<Result<(), anyhow::Error>>>,
}

impl Server {
    pub fn new() -> Self {
        let metadata = parse_metadata();
        Self {
            worker_count: WORKER_COUNT,
            metadata: Arc::new(metadata),
            pool: HashMap::new(),
        }
    }

    pub fn start(&mut self, receiver: AsyncReceiver<ServerRequest>) {
        for i in 0..self.worker_count {
            let rx = receiver.clone();
            let metadata = Arc::clone(&self.metadata);
            let mut worker = ServerWorker::new(rx, metadata);
            let handle = tokio::task::spawn(async move { worker.start().await });
            self.pool.insert(i, handle);
        }
    }
}

pub struct ServerWorker {
    metadata: Arc<Box<[RecordBatch]>>,
    receiver: AsyncReceiver<ServerRequest>,
}

impl ServerWorker {
    pub fn new(rx: AsyncReceiver<ServerRequest>, metadata: Arc<Box<[RecordBatch]>>) -> Self {
        Self {
            receiver: rx,
            metadata,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        while let Ok((request, responder)) = self.receiver.recv().await {
            let request: &dyn IntoResponse = match request.header.api_key {
                ApiType::ApiVersions => &ApiVersionsRequest::new(request),
                ApiType::DescribeTopicPartitions => {
                    &DescribeTopicsRequest::new(request, Arc::clone(&self.metadata))
                }
                ApiType::Fetch => &FetchRequest::new(request, Arc::clone(&self.metadata)),
            };

            let mut response = BytesMut::new();
            let content = request.response();
            response.put_i32(content.len() as i32);
            response.extend_from_slice(&content[..]);

            responder
                .send(response)
                .await
                .context("sending response to client")?;
        }

        Ok(())
    }
}
