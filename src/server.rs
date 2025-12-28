use crate::request::{ApiType, ErrorCode, describe_topics::DescribeTopicsRequest};

use super::request::Request;
use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use kanal::{AsyncReceiver, AsyncSender, unbounded_async};
use std::collections::HashMap;
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
    pool: HashMap<usize, JoinHandle<Result<(), anyhow::Error>>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            worker_count: WORKER_COUNT,
            pool: HashMap::new(),
        }
    }

    pub fn start(&mut self, receiver: AsyncReceiver<ServerRequest>) {
        for i in 0..self.worker_count {
            let rx = receiver.clone();
            let mut worker = ServerWorker::new(rx);
            let handle = tokio::task::spawn(async move { worker.start().await });
            self.pool.insert(i, handle);
        }
    }
}

pub struct ServerWorker {
    receiver: AsyncReceiver<ServerRequest>,
}

impl ServerWorker {
    pub fn new(rx: AsyncReceiver<ServerRequest>) -> Self {
        Self { receiver: rx }
    }

    pub async fn start(&mut self) -> Result<()> {
        while let Ok((request, responder)) = self.receiver.recv().await {
            let mut response = BytesMut::new();
            let content = match request.header.api_key {
                ApiType::ApiVersions => {
                    let mut content = BytesMut::new();
                    let c_id = request.header.correlation_id;
                    let error_code = request.header.version_supported();
                    let thottle: i32 = 0;

                    let supported_apis =
                        vec![ApiType::ApiVersions, ApiType::DescribeTopicPartitions];
                    let api_items = supported_apis.len() + 1; // TODO: varint encode

                    content.put_i32(c_id);
                    content.put_i16(error_code as i16);
                    content.put_i8(api_items as i8);
                    for api in supported_apis.iter() {
                        api.metadata(&mut content);
                    }
                    content.put_i32(thottle);

                    // Tags
                    content.put_i8(0x00);

                    content
                }
                ApiType::DescribeTopicPartitions => {
                    let request =
                        DescribeTopicsRequest::new(request.header.clone(), request.payload);
                    let mut content = BytesMut::new();
                    let throttle: i32 = 0;
                    let tag: i8 = 0;
                    content.put_i32(request.header.correlation_id);
                    content.put_i8(tag);

                    content.put_i32(throttle);
                    content.put_i8((request.topic_names.len() + 1) as i8);

                    // TODO: Derive a Topic
                    let topic_id = vec![0; 16];
                    let is_internal: i8 = 0x00;
                    let partition_arr_len: i8 = 0x00;
                    let authorised_ops: i32 = 0;
                    for topic in request.topic_names.iter() {
                        // TODO: Get topic and perform checks
                        let topic_len = (topic.len() + 1) as i8;
                        content.put_i16(ErrorCode::UnknownTopicOrPartition as i16);
                        content.put_i8(topic_len);
                        content.extend_from_slice(&topic[..]);
                        content.extend_from_slice(&topic_id);
                        content.put_i8(is_internal);
                        content.put_i8(partition_arr_len);
                        content.put_i32(authorised_ops);
                        content.put_i8(tag);
                    }
                    // content.put_i32(request.partition_limit);
                    content.put_u8(request.cursor);
                    content.put_i8(tag);

                    content
                }
            };

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
