use bytes::{Buf, BufMut, Bytes, BytesMut};
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

#[inline]
pub fn unsigned_varint_encode(buf: &mut BytesMut, mut length: usize) {
    length += 1;
    while length >= 0x80 {
        buf.put_u8((length & 0x7F) as u8 | 0x80);
        length >>= 7;
    }

    buf.put_u8(length as u8);
}
