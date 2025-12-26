#[repr(u16)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ApiType {
    ApiVersions = 18,
}

impl ApiType {
    pub fn supported_versions(&self) -> (i16, i16) {
        match self {
            Self::ApiVersions => (0, 4),
        }
    }
}

impl TryFrom<i16> for ApiType {
    type Error = std::io::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(Self::ApiVersions),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid api type: {value}"),
            )),
        }
    }
}

#[repr(i16)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    Unknown = -1,
    None = 0,
    UnsupportedVersion = 35,
}

pub fn varint_decode(bytes: &[u8]) -> (u32, usize) {
    let mut result = 0;
    let mut shift = 0;
    let mut consumed = 0;

    for byte in bytes.iter() {
        consumed += 1;
        let value = (byte & 0x7F) as u32;
        result |= value << shift;
        if byte & 0x80 == 0 {
            return (result, consumed);
        }

        shift += 7;
    }

    (result, consumed)
}

pub fn varint_encode(mut value: u32) -> Vec<u8> {
    let mut output = Vec::new();

    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        output.push(byte);
        if value == 0 {
            return output;
        }
    }
}
