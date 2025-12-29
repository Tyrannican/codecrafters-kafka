use bytes::{Buf, Bytes};
pub mod metadata;
pub mod request;
pub mod server;

#[inline]
pub fn varint_decode(bytes: &mut Bytes) -> i32 {
    let mut value = 0;
    let mut shift = 0;
    let mut consumed = 0;

    for byte in bytes.iter() {
        consumed += 1;
        value |= ((byte & 0x7F) as u32) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }
    }

    bytes.advance(consumed);
    ((value >> 1) as i32) ^ -((value & 1) as i32)
}

#[inline]
pub fn unsigned_varint_decode(bytes: &mut Bytes) -> u32 {
    let mut value = 0;
    let mut shift = 0;
    let mut consumed = 0;

    for byte in bytes.iter() {
        consumed += 1;
        value |= ((byte & 0x7F) as u32) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }
    }

    bytes.advance(consumed);

    value.saturating_sub(1)
}
