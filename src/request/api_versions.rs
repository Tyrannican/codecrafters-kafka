use bytes::{BufMut, BytesMut};

use crate::request::{ApiType, IntoResponse, Request, RequestHeader};

pub struct ApiVersionsRequest {
    header: RequestHeader,
}

impl ApiVersionsRequest {
    pub fn new(request: Request) -> Self {
        Self {
            header: request.header,
        }
    }
}

impl IntoResponse for ApiVersionsRequest {
    fn response(&self) -> bytes::BytesMut {
        let mut content = BytesMut::new();
        let c_id = self.header.correlation_id;
        let error_code = self.header.version_supported();
        let thottle: i32 = 0;

        let supported_apis = vec![
            ApiType::ApiVersions,
            ApiType::DescribeTopicPartitions,
            ApiType::Fetch,
        ];

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
}
