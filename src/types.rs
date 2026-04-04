use crate::utils::trim_nul_end_str;
use crate::FastDFSError;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::SystemTime;

const SEPARATOR: &'static str = "/";

/// Default network ports for FastDFS servers
pub const TRACKER_DEFAULT_PORT: u16 = 22122;
pub const STORAGE_DEFAULT_PORT: u16 = 23000;

/// Field size limits
pub const FDFS_GROUP_NAME_MAX_LEN: usize = 16;
pub const FDFS_FILE_EXT_NAME_MAX_LEN: usize = 6;
pub const FDFS_MAX_META_NAME_LEN: usize = 64;
pub const FDFS_MAX_META_VALUE_LEN: usize = 256;
pub const FDFS_FILE_PREFIX_MAX_LEN: usize = 16;
pub const FDFS_STORAGE_ID_MAX_SIZE: usize = 16;

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct FileId {
    pub group_name: String,
    pub remote_filename: String,
}

impl Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}{SEPARATOR}{}", self.group_name, self.remote_filename)
    }
}

impl Debug for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Hash for FileId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.group_name.as_bytes());
        state.write(SEPARATOR.as_bytes());
        state.write(self.remote_filename.as_bytes());
        state.write_u8(0xff);
    }
}

impl From<FileId> for String {
    fn from(id: FileId) -> Self {
        id.to_string()
    }
}

impl<S0: AsRef<str>, S1: AsRef<str>> Into<FileId> for (S0, S1) {
    fn into(self) -> FileId {
        FileId {
            group_name: self.0.as_ref().to_owned(),
            remote_filename: self.1.as_ref().to_owned(),
        }
    }
}

impl FileId {
    #[inline]
    pub fn new<S0: AsRef<str>, S1: AsRef<str>>(group_name: S0, remote_filename: S1) -> Self {
        (group_name, remote_filename).into()
    }
}

impl FromStr for FileId {
    type Err = FastDFSError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let file_id = trim_nul_end_str(s);

        let Some(idx) = file_id.find(SEPARATOR) else {
            return Err(FastDFSError::InvalidFileId(file_id.to_owned()));
        };

        if idx < 1 || idx >= file_id.len() - 1 || idx >= FDFS_GROUP_NAME_MAX_LEN {
            return Err(FastDFSError::InvalidFileId(file_id.to_owned()));
        }

        Ok((&file_id[..idx], &file_id[idx + 1..]).into())
    }
}

/// Information about a file stored in FastDFS
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// Size of the file in bytes
    pub file_size: u64,
    /// Timestamp when the file was created
    pub create_time: SystemTime,
    /// CRC32 checksum of the file
    pub crc32: u32,
    /// IP address of the source storage server
    pub source_ip_addr: String,
}

/// Represents a storage server in the FastDFS cluster
#[derive(Debug, Clone)]
pub struct StorageServer {
    /// IP address of the storage server
    pub ip_addr: String,
    /// Port number of the storage server
    pub port: u16,
    /// Index of the storage path to use (0-based)
    pub store_path_index: u8,
}

/// Storage server status codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageStatus {
    Init = 0,
    WaitSync = 1,
    Syncing = 2,
    IpChanged = 3,
    Deleted = 4,
    Offline = 5,
    Online = 6,
    Active = 7,
    Recovery = 9,
    None = 99,
}

/// Metadata operation flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MetadataFlag {
    /// Replace all existing metadata with new values
    Overwrite = b'O',
    /// Merge new metadata with existing metadata
    Merge = b'M',
}

impl From<MetadataFlag> for u8 {
    fn from(flag: MetadataFlag) -> u8 {
        flag as u8
    }
}

#[derive(Debug, Clone)]
pub struct GroupStat {
    /// name of this group
    pub group_name: String,
    /// disk total space in MB
    pub total_mb: u64,
    /// disk free space in MB
    pub free_mb: u64,
    /// disk reserved space in MB, since v6.13.1
    pub reserved_mb: u64,
    /// trunk free disk storage in MB
    pub trunk_free_mb: u64,
    /// storage server count
    pub storage_count: u32,
    /// storage server listen port
    pub storage_port: u32,
    pub readable_server_count: u32,
    pub writable_server_count: u32,
    /// current server index to upload file
    pub current_write_server: u32,
    /// store base path count of each storage server
    pub store_path_count: u32,
    pub subdir_count_per_path: u32,
    /// current trunk file id
    pub current_trunk_file_id: u32,
}

/// FDFSStorageInfo - C struct body
#[derive(Debug, Clone)]
pub struct StorageStat {
    pub status: u8,
    /// since v6.13
    pub rw_mode: u8,
    pub id: String,
    pub ip_addr: String,
    /// src storage id
    pub src_id: String,
    pub version: String,
    ///storage join timestamp (create timestamp)
    pub join_time: SystemTime,
    ///storage service started timestamp
    pub up_time: SystemTime,
    ///disk total space in MB
    pub total_mb: u64,
    ///disk free space in MB
    pub free_mb: u64,
    ///disk available space in MB, since v6.13.1
    pub reserved_mb: u64,
    ///upload priority
    pub upload_priority: u32,
    ///store base path count of each storage server
    pub store_path_count: u32,
    pub subdir_count_per_path: u32,
    pub storage_port: u32,
    ///current write path index
    pub current_write_path: u32,

    // FDFSStorageStat stat - C struct body
    pub connection_alloc_count: u32,
    pub connection_current_count: u32,
    pub connection_max_count: u32,
    /* following count stat by source server,
           not including synced count
    */
    pub total_upload_count: u64,
    pub success_upload_count: u64,
    pub total_append_count: u64,
    pub success_append_count: u64,
    pub total_modify_count: u64,
    pub success_modify_count: u64,
    pub total_truncate_count: u64,
    pub success_truncate_count: u64,
    pub total_set_meta_count: u64,
    pub success_set_meta_count: u64,
    pub total_delete_count: u64,
    pub success_delete_count: u64,
    pub total_download_count: u64,
    pub success_download_count: u64,
    pub total_get_meta_count: u64,
    pub success_get_meta_count: u64,
    pub total_create_link_count: u64,
    pub success_create_link_count: u64,
    pub total_delete_link_count: u64,
    pub success_delete_link_count: u64,
    pub total_upload_bytes: u64,
    pub success_upload_bytes: u64,
    pub total_append_bytes: u64,
    pub success_append_bytes: u64,
    pub total_modify_bytes: u64,
    pub success_modify_bytes: u64,
    pub total_download_bytes: u64,
    pub success_download_bytes: u64,
    pub total_sync_in_bytes: u64,
    pub success_sync_in_bytes: u64,
    pub total_sync_out_bytes: u64,
    pub success_sync_out_bytes: u64,
    pub total_file_open_count: u64,
    pub success_file_open_count: u64,
    pub total_file_read_count: u64,
    pub success_file_read_count: u64,
    pub total_file_write_count: u64,
    pub success_file_write_count: u64,
    /* last update timestamp as source server,
           current server' timestamp
    */
    pub last_source_update: SystemTime,
    /* last update timestamp as dest server,
           current server' timestamp
    */
    pub last_sync_update: SystemTime,
    /* last synced timestamp,
       source server's timestamp
    */
    pub last_synced_timestamp: SystemTime,
    /* last heart beat time */
    pub last_heart_beat_time: SystemTime,

    pub if_trunk_server: bool,
}

impl StorageStat {
    pub fn status(&self) -> StorageStatus {
        match self.status {
            0 => StorageStatus::Init,
            1 => StorageStatus::WaitSync,
            2 => StorageStatus::Syncing,
            3 => StorageStatus::IpChanged,
            4 => StorageStatus::Deleted,
            5 => StorageStatus::Offline,
            6 => StorageStatus::Online,
            7 => StorageStatus::Active,
            9 => StorageStatus::Recovery,
            _ => StorageStatus::None,
        }
    }
}

/// Metadata dictionary type
pub type Metadata = std::collections::HashMap<String, String>;

pub const FDFS_QUERY_FINFO_FLAGS_NOT_CALC_CRC32: u8 = 1;
pub const FDFS_QUERY_FINFO_FLAGS_KEEP_SILENCE: u8 = 2;
