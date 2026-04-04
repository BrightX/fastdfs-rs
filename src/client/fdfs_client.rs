use super::storage_client::{StorageClient, StorageClientFactor};
use super::tracker_client::{TrackerClient, TrackerClientFactor};
use crate::pool::{KeyedObjectPool, PoolConfig, PooledObject};
use crate::types::{FileInfo, GroupStat, Metadata, MetadataFlag, StorageServer, StorageStat};
use crate::types::{FDFS_QUERY_FINFO_FLAGS_KEEP_SILENCE, FDFS_QUERY_FINFO_FLAGS_NOT_CALC_CRC32};
use crate::{ClientOptions, FileId};
use crate::{FastDFSError, Result};
use bytes::Bytes;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct ClientInner {
    config: ClientOptions,
    tracker_pool: KeyedObjectPool<SocketAddr, TrackerClient, TrackerClientFactor>,
    storage_pool: KeyedObjectPool<SocketAddr, StorageClient, StorageClientFactor>,
    closed: AtomicBool,
}

type TrackerClientPooled = PooledObject<SocketAddr, TrackerClient, TrackerClientFactor>;
type StorageClientPooled = PooledObject<SocketAddr, StorageClient, StorageClientFactor>;

impl ClientInner {
    async fn get_tracker_client(&self) -> Result<TrackerClientPooled> {
        {
            if self.closed.load(Ordering::Relaxed) {
                return Err(FastDFSError::ClientClosed);
            }
        }
        for addr in &self.config.tracker_addrs {
            let addr: SocketAddr = addr.parse().map_err(|_| {
                FastDFSError::InvalidArgument(format!("Invalid tracker address: {}", addr))
            })?;

            if let Ok(mut client) = self.tracker_pool.get(&addr).await {
                client.in_use();
                return Ok(client);
            }
        }

        Err(FastDFSError::TrackerServerOffline(
            self.config.tracker_addrs.join(", "),
        ))
    }

    async fn get_storage_client(&self, server_info: StorageServer) -> Result<StorageClientPooled> {
        {
            if self.closed.load(Ordering::Relaxed) {
                return Err(FastDFSError::ClientClosed);
            }
        }
        let addr = format!("{}:{}", server_info.ip_addr, server_info.port);
        let addr: SocketAddr = addr.parse().map_err(|_| {
            FastDFSError::InvalidArgument(format!("Invalid tracker address: {}", addr))
        })?;

        let mut result = self.storage_pool.get(&addr).await?;
        result.store_path_index(server_info.store_path_index);
        result.in_use();
        Ok(result)
    }
}

// Tracker Service
impl ClientInner {
    /// query group stat info
    pub async fn list_groups(&self) -> Result<Vec<GroupStat>> {
        let mut tc = self.get_tracker_client().await?;
        tc.list_groups().await
    }

    /// query storage server stat info of the group
    pub async fn list_storages<S0: AsRef<str>, S1: AsRef<str>>(
        &self,
        group: S0,
        server_id: Option<S1>,
    ) -> Result<Vec<StorageStat>> {
        let mut tc = self.get_tracker_client().await?;
        tc.list_storages(group, server_id).await
    }

    /// delete a storage server from the tracker server
    pub async fn delete_storage<S0: AsRef<str>, S1: AsRef<str>>(
        &self,
        group: S0,
        server_id: S1,
    ) -> Result<bool> {
        let addr = server_id.as_ref();
        let addr: SocketAddr = addr.parse().map_err(|_| {
            FastDFSError::InvalidArgument(format!("Invalid tracker address: {}", addr))
        })?;
        let mut tc = self.get_tracker_client().await?;
        let result = tc.delete_storage(group, server_id).await?;
        if result {
            self.storage_pool.remove_key(&addr).await;
        }
        Ok(result)
    }
}

// Storage Service
impl ClientInner {
    /// Uploads data from a stream
    pub async fn upload_file<R: AsyncRead + Unpin + ?Sized>(
        &self,
        stream: &mut R,
        stream_size: u64,
        file_ext_name: &str,
        metadata: Option<&Metadata>,
        is_appender: bool,
    ) -> Result<FileId> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_store_storage::<&str>(None).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.upload_file(stream, stream_size, file_ext_name, metadata, is_appender)
            .await
    }

    /// Uploads data from a buffer
    pub async fn upload_file_buf(
        &self,
        data: &[u8],
        file_ext_name: &str,
        metadata: Option<&Metadata>,
        is_appender: bool,
    ) -> Result<FileId> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_store_storage::<&str>(None).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.upload_file_buf(data, file_ext_name, metadata, is_appender)
            .await
    }

    /// Uploads a file from the local filesystem
    pub async fn upload_file_local(
        &self,
        local_filename: &str,
        metadata: Option<&Metadata>,
        is_appender: bool,
    ) -> Result<FileId> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_store_storage::<&str>(None).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.upload_file_local(local_filename, metadata, is_appender)
            .await
    }

    /// Downloads a file and copy to a stream
    ///
    /// # Arguments
    ///
    /// * `file_id`: file id
    /// * `writer`:  a stream of the copy target
    /// * `offset`: the start offset of the file
    /// * `length`: download bytes, 0 for remain bytes from offset
    pub async fn download_file<W: AsyncWrite + Unpin + ?Sized>(
        &self,
        file_id: &FileId,
        writer: &mut W,
        offset: u64,
        length: u64,
    ) -> Result<u64> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_fetch_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.download_file(file_id, writer, offset, length).await
    }

    /// download file from storage server
    ///
    /// # Arguments
    ///
    /// * `file_id`: file id
    /// * `offset`: the start offset of the file
    /// * `length`: download bytes, 0 for remain bytes from offset
    pub async fn download_file_buf(
        &self,
        file_id: &FileId,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_fetch_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.download_file_buf(file_id, offset, length).await
    }

    /// Downloads a file and saves it to the local filesystem
    pub async fn download_file_local(&self, file_id: &FileId, local_filename: &str) -> Result<u64> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_fetch_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.download_file_local(file_id, local_filename).await
    }

    /// Deletes a file from FastDFS
    pub async fn delete_file(&self, file_id: &FileId) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.delete_file(file_id).await
    }

    /// append file to storage server (by stream)
    pub async fn append_file<R: AsyncRead + Unpin + ?Sized>(
        &self,
        file_id: &FileId,
        stream: &mut R,
        stream_size: u64,
    ) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.append_file(file_id, stream, stream_size).await
    }

    /// append file to storage server (by file buff)
    pub async fn append_file_buf(&self, file_id: &FileId, data: &[u8]) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.append_file_buf(file_id, data).await
    }

    /// append file to storage server (by local filesystem)
    pub async fn append_file_local(&self, file_id: &FileId, local_filename: &str) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.append_file_local(file_id, local_filename).await
    }

    /// modify appender file to storage server (by stream)
    /// # Arguments
    ///
    /// * `file_offset`: the offset of appender file
    pub async fn modify_file<R: AsyncRead + Unpin + ?Sized>(
        &self,
        file_id: &FileId,
        file_offset: u64,
        stream: &mut R,
        stream_size: u64,
    ) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.modify_file(file_id, file_offset, stream, stream_size)
            .await
    }

    /// modify appender file to storage server (by file buff)
    /// # Arguments
    ///
    /// * `file_offset`: the offset of appender file
    pub async fn modify_file_buf(
        &self,
        file_id: &FileId,
        file_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.modify_file_buf(file_id, file_offset, data).await
    }

    /// modify appender file to storage server (by local filesystem)
    /// # Arguments
    ///
    /// * `file_offset`: the offset of appender file
    pub async fn modify_file_local(
        &self,
        file_id: &FileId,
        file_offset: u64,
        local_filename: &str,
    ) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.modify_file_local(file_id, file_offset, local_filename)
            .await
    }

    /// truncate appender file from storage server
    ///
    /// # Arguments
    ///
    /// * `file_id`: the file id of appender file
    /// * `truncated_file_size`: truncated file size
    pub async fn truncate_file(&self, file_id: &FileId, truncated_file_size: u64) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.truncate_file(file_id, truncated_file_size).await
    }

    /// truncate appender file to size 0 from storage server
    ///
    /// # Arguments
    ///
    /// * `file_id`: the file id of appender file
    pub async fn truncate_file0(&self, file_id: &FileId) -> Result<()> {
        self.truncate_file(&file_id, 0).await
    }

    /// regenerate filename for appender file, since `v6.02`
    ///
    /// # Arguments
    ///
    /// * `file_id`: the file id of appender file
    pub async fn regenerate_appender_filename(&self, file_id: &FileId) -> Result<FileId> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.regenerate_appender_filename(file_id).await
    }

    pub async fn set_metadata(
        &self,
        file_id: &FileId,
        metadata: &Metadata,
        flag: MetadataFlag,
    ) -> Result<()> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_update_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.set_metadata(file_id, metadata, flag).await
    }

    pub async fn get_metadata(&self, file_id: &FileId) -> Result<Metadata> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_fetch_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.get_metadata(file_id).await
    }

    /// query file info
    pub async fn get_file_info(&self, file_id: &FileId) -> Result<FileInfo> {
        self.get_file_info_with_flag(file_id, 0).await
    }

    /// query file info
    ///
    /// # Arguments
    ///
    /// * `file_id`: file id
    /// * `flag`: since `v6.15.1` , combined flags as following:
    ///     - `FDFS_QUERY_FINFO_FLAGS_NOT_CALC_CRC32` : do NOT calculate CRC32 for appender file or slave file
    ///     - `FDFS_QUERY_FINFO_FLAGS_KEEP_SILENCE`   : keep silence, when this file not exist, do not log error on storage server
    pub async fn get_file_info_with_flag(&self, file_id: &FileId, flag: u8) -> Result<FileInfo> {
        let mut tc = self.get_tracker_client().await?;
        let server_info = tc.get_fetch_storage(file_id).await?;
        let mut sc = self.get_storage_client(server_info).await?;
        sc.get_file_info_with_flag(file_id, flag).await
    }
}

// ext
impl ClientInner {
    /// Checks if a file exists on the storage server
    pub async fn file_exists(&self, file_id: &FileId) -> Result<bool> {
        let flag = FDFS_QUERY_FINFO_FLAGS_NOT_CALC_CRC32 | FDFS_QUERY_FINFO_FLAGS_KEEP_SILENCE;
        let result = self.get_file_info_with_flag(file_id, flag).await;
        match result {
            Ok(_) => Ok(true),
            Err(FastDFSError::FileNotFound(_)) => Ok(false),
            Err(FastDFSError::InvalidArgument(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Closes the client and releases all resources
    ///
    /// After calling close, all operations will return ClientClosed error.
    /// It's safe to call close multiple times.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.tracker_pool.remove_all().await;
        self.storage_pool.remove_all().await;
    }
}

#[derive(Clone)]
pub struct FdfsClient {
    inner: Arc<ClientInner>,
}

/// Validates the client configuration
fn validate_config(config: &ClientOptions) -> Result<()> {
    if config.tracker_addrs.is_empty() {
        return Err(FastDFSError::InvalidArgument(
            "Tracker addresses are required".to_string(),
        ));
    }

    for addr in &config.tracker_addrs {
        if addr.is_empty() || !addr.contains(':') {
            return Err(FastDFSError::InvalidArgument(format!(
                "Invalid tracker address: {}",
                addr
            )));
        }
    }

    Ok(())
}

fn pool_config(config: &ClientOptions) -> PoolConfig {
    PoolConfig {
        max_total_per_key: config.max_connections,
        test_on_borrow: false,
        test_on_return: false,
        ..PoolConfig::default()
    }
}

impl FdfsClient {
    pub fn new(config: ClientOptions) -> Result<Self> {
        validate_config(&config)?;

        let tracker_factor = TrackerClientFactor {
            version: config.version,
            connect_timeout: config.connect_timeout,
        };
        let storage_factor = StorageClientFactor {
            version: config.version,
            connect_timeout: config.connect_timeout,
        };
        let pool_config = pool_config(&config);

        let client = ClientInner {
            config,
            tracker_pool: KeyedObjectPool::new(tracker_factor, Some(pool_config)),
            storage_pool: KeyedObjectPool::new(storage_factor, Some(pool_config)),
            closed: AtomicBool::new(false),
        };

        Ok(Self {
            inner: Arc::new(client),
        })
    }
}

impl Deref for FdfsClient {
    type Target = ClientInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
