#[derive(Debug, Clone, PartialEq)]
pub enum KafkaError {
    NONE,
    UNSUPPORTED_VERSION,
    UNKNOWN_SERVER_ERROR,
}

impl KafkaError {
    pub fn to_bytes(&self) -> [u8; 2] {
        match self {
            Self::NONE => 0i16.to_be_bytes(),
            Self::UNSUPPORTED_VERSION => 35i16.to_be_bytes(),
            Self::UNKNOWN_SERVER_ERROR => (-1 as i16).to_be_bytes(),
        }
    }
}
