#![allow(dead_code)]

pub(crate) mod decode;
mod tracker_request;

use crate::types::FDFS_FILE_EXT_NAME_MAX_LEN;
use crate::types::FDFS_MAX_META_NAME_LEN;
use crate::types::FDFS_MAX_META_VALUE_LEN;
use crate::types::{FileInfo, Metadata, FDFS_GROUP_NAME_MAX_LEN};
use crate::{FastDFSError, FileId, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use crate::utils::{pad_str_buf, secs_to_sys_time, unpad_string};
pub(crate) use tracker_request::TrackerRequest;

/// Protocol header size
pub const FDFS_PROTO_HEADER_LEN: usize = 10;

pub const FDFS_PROTO_CMD_QUIT: u8 = 82;
pub const TRACKER_PROTO_CMD_RESP: u8 = 100;
pub const FDFS_PROTO_CMD_ACTIVE_TEST: u8 = 111;
pub const STORAGE_PROTO_CMD_RESP: u8 = TRACKER_PROTO_CMD_RESP;

pub const FDFS_VERSION_SIZE_OLD: usize = 6;
pub const FDFS_VERSION_SIZE: usize = 8;
pub const FDFS_IPV4_SIZE: usize = 16;
pub const FDFS_IPV6_SIZE: usize = 46;

/// Protocol separators
pub const FDFS_RECORD_SEPARATOR: u8 = 0x01;
pub const FDFS_FIELD_SEPARATOR: u8 = 0x02;

/// FastDFS protocol header (10 bytes)
#[derive(Debug, Clone)]
pub struct ProtoHeader {
    /// Length of the message body (not including header)
    pub length: u64,
    /// Command code (request type or response type)
    pub cmd: u8,
    /// Status code (0 for success, error code otherwise)
    pub status: u8,
}

impl Default for ProtoHeader {
    fn default() -> Self {
        Self {
            length: 0,
            cmd: 0,
            status: 0,
        }
    }
}

#[allow(dead_code)]
impl ProtoHeader {
    pub const fn new(length: u64, cmd: u8, status: u8) -> Self {
        Self {
            length,
            cmd,
            status,
        }
    }

    pub const fn of(cmd: u8) -> Self {
        Self::new(0, cmd, 0)
    }

    pub fn length(mut self, length: u64) -> Self {
        self.length = length;
        self
    }

    pub fn len(mut self, length: usize) -> Self {
        self.length = length as u64;
        self
    }

    pub fn cmd(mut self, cmd: u8) -> Self {
        self.cmd = cmd;
        self
    }

    pub fn status(mut self, status: u8) -> Self {
        self.status = status;
        self
    }

    #[inline]
    pub fn is_ok(&self) -> bool {
        self.status == 0
    }

    pub fn put_buf(&self, buf: &mut BytesMut) {
        buf.put_u64(self.length);
        buf.put_u8(self.cmd);
        buf.put_u8(self.status);
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(FDFS_PROTO_HEADER_LEN);
        self.put_buf(&mut buf);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let mut buf = data;
        let len = buf.get_u64();
        let cmd = buf.get_u8();
        let status = buf.get_u8();

        Self::new(len, cmd, status)
    }
}

impl ProtoHeader {
    async fn send_inner<S: AsyncWriteExt + Unpin>(&self, stream: &mut S) -> std::io::Result<()> {
        // stream.write_all(self.encode().as_ref()).await
        stream.write_u64(self.length).await?;
        stream.write_u8(self.cmd).await?;
        stream.write_u8(self.status).await?;
        stream.flush().await?;

        Ok(())
    }
}

impl ProtoHeader {
    pub(crate) async fn send<S: AsyncWriteExt + Unpin>(&self, stream: &mut S) -> Result<()> {
        self.send_inner(stream).await?;
        Ok(())
    }

    pub(crate) async fn send_timeout<S: AsyncWriteExt + Unpin>(
        &self,
        stream: &mut S,
        network_timeout: Duration,
        addr: Option<SocketAddr>,
    ) -> Result<()> {
        let result = timeout(network_timeout, self.send_inner(stream)).await;

        match result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(FastDFSError::Network {
                operation: "write".to_string(),
                addr: addr.map(|a| a.to_string()).unwrap_or_default(),
                source: e,
            }),
            Err(_) => Err(FastDFSError::NetworkTimeout("write".to_string())),
        }
    }

    pub(crate) async fn read<S: AsyncReadExt + Unpin>(stream: &mut S) -> Result<ProtoHeader> {
        let len = stream.read_u64().await?;
        let cmd = stream.read_u8().await?;
        let status = stream.read_u8().await?;

        Ok(ProtoHeader::new(len, cmd, status))
    }
}

pub const ACTIVE_TEST_HEADER: &'static ProtoHeader = &ProtoHeader::of(FDFS_PROTO_CMD_ACTIVE_TEST);
pub const QUIT_HEADER: &'static ProtoHeader = &ProtoHeader::of(FDFS_PROTO_CMD_QUIT);

/// Tracker protocol commands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TrackerCommand {
    ServiceQueryStoreWithoutGroupOne = 101,
    ServiceQueryFetchOne = 102,
    ServiceQueryUpdate = 103,
    ServiceQueryStoreWithGroupOne = 104,
    ServiceQueryFetchAll = 105,
    ServiceQueryStoreWithoutGroupAll = 106,
    ServiceQueryStoreWithGroupAll = 107,
    ServerListOneGroup = 90,
    ServerListAllGroups = 91,
    ServerListStorage = 92,
    ServerDeleteStorage = 93,
    StorageReportIpChanged = 94,
    StorageReportStatus = 95,
    StorageReportDiskUsage = 96,
    StorageSyncTimestamp = 97,
    StorageSyncReport = 98,
}

impl From<TrackerCommand> for u8 {
    #[inline]
    fn from(cmd: TrackerCommand) -> u8 {
        cmd as u8
    }
}

/// Storage protocol commands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageCommand {
    UploadFile = 11,
    DeleteFile = 12,
    SetMetadata = 13,
    DownloadFile = 14,
    GetMetadata = 15,
    UploadSlaveFile = 21,
    QueryFileInfo = 22,
    UploadAppenderFile = 23,
    AppendFile = 24,
    ModifyFile = 34,
    TruncateFile = 36,
    RegenerateAppenderFilename = 38,
}

impl From<StorageCommand> for u8 {
    #[inline]
    fn from(cmd: StorageCommand) -> u8 {
        cmd as u8
    }
}

/// Encodes metadata key-value pairs into FastDFS wire format
///
/// The format uses special separators:
///   - Field separator (0x02) between key and value
///   - Record separator (0x01) between different key-value pairs
///
/// Format: key1<0x02>value1<0x01>key2<0x02>value2<0x01>
///
/// Keys are truncated to 64 bytes and values to 256 bytes if they exceed limits.
pub fn encode_metadata(metadata: &Metadata) -> Bytes {
    if metadata.is_empty() {
        return Bytes::new();
    }

    let mut buf = BytesMut::new();

    for (key, value) in metadata {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        // Truncate if necessary
        let key_len = key_bytes.len().min(FDFS_MAX_META_NAME_LEN);
        let value_len = value_bytes.len().min(FDFS_MAX_META_VALUE_LEN);

        buf.put_slice(&key_bytes[..key_len]);
        buf.put_u8(FDFS_FIELD_SEPARATOR);
        buf.put_slice(&value_bytes[..value_len]);
        buf.put_u8(FDFS_RECORD_SEPARATOR);
    }

    buf.freeze()
}

/// Decodes FastDFS wire format metadata into a HashMap
///
/// This is the inverse operation of encode_metadata.
///
/// The function parses records separated by 0x01 and fields separated by 0x02.
/// Invalid records (not exactly 2 fields) are silently skipped.
pub fn decode_metadata(data: &[u8]) -> Result<Metadata> {
    if data.is_empty() {
        return Ok(Metadata::new());
    }

    let mut metadata = Metadata::new();
    let records: Vec<&[u8]> = data.split(|&b| b == FDFS_RECORD_SEPARATOR).collect();

    for record in records {
        if record.is_empty() {
            continue;
        }

        let fields: Vec<&[u8]> = record.split(|&b| b == FDFS_FIELD_SEPARATOR).collect();
        if fields.len() != 2 {
            continue;
        }

        let key = unpad_string(fields[0]);
        let value = unpad_string(fields[1]);
        metadata.insert(key, value);
    }

    Ok(metadata)
}

/// Extracts and validates the file extension from a filename
///
/// The extension is extracted without the leading dot and truncated to 6 characters
/// if it exceeds the FastDFS maximum.
///
/// Examples:
///   - "test.jpg" -> "jpg"
///   - "file.tar.gz" -> "gz"
///   - "noext" -> ""
///   - "file.verylongext" -> "verylo" (truncated)
pub fn get_file_ext_name(filename: &str) -> &str {
    let path = Path::new(filename);
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

    if ext.len() > FDFS_FILE_EXT_NAME_MAX_LEN {
        &ext[..FDFS_FILE_EXT_NAME_MAX_LEN]
    } else {
        ext
    }
}

pub fn decode_file_id(data: &[u8]) -> Result<FileId> {
    if data.len() < FDFS_GROUP_NAME_MAX_LEN {
        return Err(FastDFSError::InvalidResponse(
            "Response body too short".to_string(),
        ));
    }
    let group_name = unpad_string(&data[..FDFS_GROUP_NAME_MAX_LEN]);
    let remote_filename = unpad_string(&data[FDFS_GROUP_NAME_MAX_LEN..]);

    Ok(FileId {
        group_name,
        remote_filename,
    })
}

pub fn encode_file_id(file_id: &FileId) -> Bytes {
    let buf_len = FDFS_GROUP_NAME_MAX_LEN + file_id.remote_filename.len();

    let mut buf = BytesMut::with_capacity(buf_len);
    pad_str_buf(&mut buf, &file_id.group_name, FDFS_GROUP_NAME_MAX_LEN);
    buf.put_slice(file_id.remote_filename.as_bytes());

    buf.freeze()
}

pub fn decode_file_info(mut buf: &[u8]) -> Result<FileInfo> {
    let file_size = buf.get_u64();
    let create_timestamp = buf.get_u64();
    let crc32 = buf.get_u64() as u32;
    let source_ip_addr = unpad_string(buf);

    let create_time = secs_to_sys_time(create_timestamp);

    Ok(FileInfo {
        file_size,
        create_time,
        crc32,
        source_ip_addr,
    })
}
