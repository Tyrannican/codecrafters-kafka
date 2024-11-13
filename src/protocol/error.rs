#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KafkaError {
    None,
    UnsupportedVersion,
    UnknownTopicOrPartition,
    UnknownServerError,
}

impl From<i16> for KafkaError {
    fn from(value: i16) -> Self {
        match value {
            -1 => Self::UnknownServerError,
            0 => Self::None,
            3 => Self::UnknownTopicOrPartition,
            35 => Self::UnsupportedVersion,
            _ => Self::UnknownServerError,
        }
    }
}

impl Into<i16> for KafkaError {
    fn into(self) -> i16 {
        match self {
            Self::UnknownServerError => -1,
            Self::None => 0,
            Self::UnknownTopicOrPartition => 3,
            Self::UnsupportedVersion => 35,
        }
    }
}
