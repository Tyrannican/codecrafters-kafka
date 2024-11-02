pub type SupportedApiVersions = (i16, i16);

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ApiKey {
    ApiVersions,
    DescribeTopicPartitions,
    Unsupported,
}

impl ApiKey {
    pub fn supported_versions(&self) -> SupportedApiVersions {
        match self {
            Self::ApiVersions => (0, 4),
            Self::DescribeTopicPartitions => (0, 0),
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
            75 => Self::DescribeTopicPartitions,
            _ => Self::Unsupported,
        }
    }
}

impl Into<i16> for ApiKey {
    fn into(self) -> i16 {
        match self {
            Self::ApiVersions => 18,
            Self::DescribeTopicPartitions => 75,
            Self::Unsupported => -1,
        }
    }
}
