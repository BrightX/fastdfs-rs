use bytes::{BufMut, Bytes, BytesMut};
use std::time::{Duration, SystemTime};

pub fn trim_nul_end(mut bytes: &[u8]) -> &[u8] {
    // Note: A pattern matching based approach (instead of indexing) allows
    // making the function const.
    while let [rest @ .., last] = bytes {
        if *last == b'\0' {
            bytes = rest;
        } else {
            break;
        }
    }
    bytes
}

#[inline]
pub fn trim_nul_end_str(s: &str) -> &str {
    s.trim_end_matches(|c: char| c == '\0')
}

/// Pads a string to a fixed length with null bytes (0x00)
///
/// This is used to create fixed-width fields in the FastDFS protocol.
/// If the string is longer than length, it will be truncated.
pub fn pad_string(s: &str, length: usize) -> Bytes {
    let mut buf = BytesMut::with_capacity(length);
    let bytes = s.as_bytes();
    let copy_len = bytes.len().min(length);
    buf.put_slice(&bytes[..copy_len]);
    buf.resize(length, 0);
    buf.freeze()
}

pub fn pad_str_buf(buf: &mut BytesMut, s: &str, length: usize) {
    let bytes = s.as_bytes();
    let copy_len = bytes.len().min(length);
    buf.put_slice(&bytes[..copy_len]);
    for _ in copy_len..length {
        buf.put_u8(0);
    }
}

/// Removes trailing null bytes from a byte slice
///
/// This is the inverse of pad_string, used to extract strings from
/// fixed-width protocol fields.
pub fn unpad_string(data: &[u8]) -> String {
    let bytes = trim_nul_end(data);
    String::from_utf8_lossy(bytes).to_string()
}

pub fn secs_to_sys_time(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}
