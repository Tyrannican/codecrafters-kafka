use crate::message::{ApiVersionResponse, GeneralResponse, KafkaMessage, Request};

pub mod error;
pub use error::KafkaError;

#[derive(Debug)]
pub struct RequestParser {
    pub req: Request,
}

impl RequestParser {
    pub fn new(req: Request) -> Self {
        Self { req }
    }

    pub fn parse(&self) -> Box<dyn KafkaMessage + Send> {
        let header = &self.req.header;
        let api_key = ApiKey::from(header.api_key);

        if api_key == ApiKey::Unsupported || !api_key.is_supported(header.api_version) {
            return Box::new(GeneralResponse::new(
                header.correlation_id,
                KafkaError::UnsupportedVersion,
            ));
        }

        match api_key {
            ApiKey::ApiVersions => Box::new(ApiVersionResponse::new(header)),
            _ => unreachable!("should not be possible"),
        }
    }
}

pub type SupportedApiVersions = (i16, i16);

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ApiKey {
    ApiVersions,
    Unsupported,
}

impl ApiKey {
    pub fn supported_versions(&self) -> SupportedApiVersions {
        match self {
            Self::ApiVersions => (0, 4),
            Self::Unsupported => (-1, -1),
        }
    }

    pub fn is_supported(&self, api_version: i16) -> bool {
        let (min, max) = self.supported_versions();
        api_version >= min && api_version <= max
    }
}

impl From<i16> for ApiKey {
    fn from(value: i16) -> Self {
        match value {
            18 => Self::ApiVersions,
            _ => Self::Unsupported,
        }
    }
}

impl Into<i16> for ApiKey {
    fn into(self) -> i16 {
        match self {
            Self::ApiVersions => 18,
            Self::Unsupported => -1,
        }
    }
}
