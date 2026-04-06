use crate::errors::map_status_to_error;
use crate::pool::{BoxFuture, KeyedPoolFactory};
use crate::protocol::ProtoHeader;
use crate::protocol::StorageCommand;
use crate::protocol::ACTIVE_TEST_HEADER;
use crate::protocol::QUIT_HEADER;
use crate::protocol::{decode_file_id, decode_file_info, encode_file_id};
use crate::protocol::{decode_metadata, encode_metadata};
use crate::protocol::{get_file_ext_name, FDFS_IPV4_SIZE};
use crate::types::MetadataFlag;
use crate::types::FDFS_FILE_EXT_NAME_MAX_LEN;
use crate::types::FDFS_GROUP_NAME_MAX_LEN;
use crate::types::{FileInfo, Metadata};
use crate::utils::pad_string;
use crate::{FastDFSError, Result};
use crate::{FileId, Version};
use bytes::{BufMut, Bytes, BytesMut};
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{copy, copy_buf, AsyncRead, AsyncWriteExt, BufStream};
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::timeout;

pub struct StorageClient {
    stream: BufStream<TcpStream>,
    addr: Option<SocketAddr>,
    version: Version,
    /// Index of the storage path to use (0-based)
    store_path_index: u8,
}

impl Debug for StorageClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageClient")
            .field("addr", &self.addr)
            .field("version", &self.version)
            .field("store_path_index", &self.store_path_index)
            .finish()
    }
}

//noinspection DuplicatedCode
impl StorageClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;

        Ok(Self::new(stream, Version::latest()))
    }

    pub fn new(stream: TcpStream, version: Version) -> Self {
        let addr = stream.peer_addr().ok();
        let stream = BufStream::new(stream);
        Self {
            stream,
            addr,
            version,
            store_path_index: 0,
        }
    }

    pub fn version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    pub fn store_path_index(&mut self, store_path_index: u8) {
        self.store_path_index = store_path_index;
    }
}

impl StorageClient {
    /// Uploads data from a stream
    pub async fn upload_file<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        stream: &mut R,
        stream_size: u64,
        file_ext_name: &str,
        metadata: Option<&Metadata>,
        is_appender: bool,
    ) -> Result<FileId> {
        let cmd = if is_appender {
            StorageCommand::UploadAppenderFile
        } else {
            StorageCommand::UploadFile
        };
        let mut ext_name_bytes = pad_string(file_ext_name, FDFS_FILE_EXT_NAME_MAX_LEN);
        let body_len = 1 + 8 + FDFS_FILE_EXT_NAME_MAX_LEN as u64 + stream_size;
        let header = ProtoHeader::of(cmd.into()).length(body_len);

        header.send(&mut self.stream).await?;
        self.stream.write_u8(self.store_path_index).await?;
        self.stream.write_u64(stream_size).await?;
        self.stream.write_all_buf(&mut ext_name_bytes).await?;

        let mut reader = stream.take(stream_size);
        copy(&mut reader, &mut self.stream).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }
        if recv_header.length == 0 {
            return Err(FastDFSError::InvalidResponse(
                "Empty response body".to_string(),
            ));
        }

        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;
        let file_id = decode_file_id(&body)?;

        // Set metadata if provided
        if let Some(meta) = metadata {
            if !meta.is_empty() {
                let _ = self
                    .set_metadata(&file_id, meta, MetadataFlag::Overwrite)
                    .await;
            }
        }

        Ok(file_id)
    }

    /// Uploads data from a buffer
    pub async fn upload_file_buf(
        &mut self,
        data: &[u8],
        file_ext_name: &str,
        metadata: Option<&Metadata>,
        is_appender: bool,
    ) -> Result<FileId> {
        let mut buf = data;
        let size = buf.len() as u64;
        self.upload_file(&mut buf, size, file_ext_name, metadata, is_appender)
            .await
    }

    /// Uploads a file from the local filesystem
    pub async fn upload_file_local(
        &mut self,
        local_filename: &str,
        metadata: Option<&Metadata>,
        is_appender: bool,
    ) -> Result<FileId> {
        let mut file = File::open(local_filename).await?;
        let file_size = file.metadata().await?.len();
        let ext_name = get_file_ext_name(local_filename);

        self.upload_file(&mut file, file_size, ext_name, metadata, is_appender)
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
        &mut self,
        file_id: &FileId,
        writer: &mut W,
        offset: u64,
        length: u64,
    ) -> Result<u64> {
        let file_id_bytes = encode_file_id(file_id);
        let body_len = 8 + 8 + file_id_bytes.len();
        let header = ProtoHeader::of(StorageCommand::DownloadFile.into()).len(body_len);

        let mut body = BytesMut::with_capacity(body_len);
        body.put_u64(offset);
        body.put_u64(length);
        body.put(file_id_bytes);

        header.send(&mut self.stream).await?;
        self.stream.write_all_buf(&mut body).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        if recv_header.length == 0 {
            return Ok(0);
        }

        let mut reader = (&mut self.stream).take(recv_header.length);
        let copied = copy_buf(&mut reader, writer).await?;

        Ok(copied)
    }

    /// download file from storage server
    ///
    /// # Arguments
    ///
    /// * `file_id`: file id
    /// * `offset`: the start offset of the file
    /// * `length`: download bytes, 0 for remain bytes from offset
    pub async fn download_file_buf(
        &mut self,
        file_id: &FileId,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let file_id_bytes = encode_file_id(file_id);
        let body_len = 8 + 8 + file_id_bytes.len();
        let header = ProtoHeader::of(StorageCommand::DownloadFile.into()).len(body_len);

        let mut body = BytesMut::with_capacity(body_len);
        body.put_u64(offset);
        body.put_u64(length);
        body.put(file_id_bytes);

        header.send(&mut self.stream).await?;
        self.stream.write_all_buf(&mut body).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        if recv_header.length == 0 {
            return Ok(Bytes::new());
        }

        let mut body = BytesMut::zeroed(recv_header.length as usize);
        self.stream.read_exact(&mut body).await?;

        Ok(body.freeze())
    }

    /// Downloads a file and saves it to the local filesystem
    pub async fn download_file_local(
        &mut self,
        file_id: &FileId,
        local_filename: &str,
    ) -> Result<u64> {
        let path = Path::new(local_filename);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = File::create(local_filename).await?;
        self.download_file(file_id, &mut file, 0, 0).await
    }

    /// Deletes a file from FastDFS
    pub async fn delete_file(&mut self, file_id: &FileId) -> Result<()> {
        let mut file_id_bytes = encode_file_id(file_id);
        let header = ProtoHeader::of(StorageCommand::DeleteFile.into()).len(file_id_bytes.len());

        header.send(&mut self.stream).await?;
        self.stream.write_all_buf(&mut file_id_bytes).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(())
    }

    /// append file to storage server (by stream)
    pub async fn append_file<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        file_id: &FileId,
        stream: &mut R,
        stream_size: u64,
    ) -> Result<()> {
        let mut remote_filename_bytes = file_id.remote_filename.as_bytes();
        let body_len = 8 + 8 + remote_filename_bytes.len() as u64 + stream_size;
        let header = ProtoHeader::of(StorageCommand::AppendFile.into()).length(body_len);

        header.send(&mut self.stream).await?;
        self.stream
            .write_u64(remote_filename_bytes.len() as u64)
            .await?;
        self.stream.write_u64(stream_size).await?;
        self.stream.write_all(&mut remote_filename_bytes).await?;

        let mut reader = stream.take(stream_size);
        copy(&mut reader, &mut self.stream).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(())
    }

    /// append file to storage server (by file buff)
    pub async fn append_file_buf(&mut self, file_id: &FileId, data: &[u8]) -> Result<()> {
        let mut buf = data;
        let size = buf.len() as u64;
        self.append_file(file_id, &mut buf, size).await
    }

    /// append file to storage server (by local filesystem)
    pub async fn append_file_local(
        &mut self,
        file_id: &FileId,
        local_filename: &str,
    ) -> Result<()> {
        let mut file = File::open(local_filename).await?;
        let file_size = file.metadata().await?.len();
        self.append_file(file_id, &mut file, file_size).await
    }

    /// modify appender file to storage server (by stream)
    /// # Arguments
    ///
    /// * `file_offset`: the offset of appender file
    pub async fn modify_file<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        file_id: &FileId,
        file_offset: u64,
        stream: &mut R,
        stream_size: u64,
    ) -> Result<()> {
        let mut remote_filename_bytes = file_id.remote_filename.as_bytes();
        let body_len = 8 + 8 + 8 + remote_filename_bytes.len() as u64 + stream_size;
        let header = ProtoHeader::of(StorageCommand::ModifyFile.into()).length(body_len);

        header.send(&mut self.stream).await?;
        self.stream
            .write_u64(remote_filename_bytes.len() as u64)
            .await?;
        self.stream.write_u64(file_offset).await?;
        self.stream.write_u64(stream_size).await?;
        self.stream.write_all(&mut remote_filename_bytes).await?;

        let mut reader = stream.take(stream_size);
        copy(&mut reader, &mut self.stream).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(())
    }

    /// modify appender file to storage server (by file buff)
    /// # Arguments
    ///
    /// * `file_offset`: the offset of appender file
    pub async fn modify_file_buf(
        &mut self,
        file_id: &FileId,
        file_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let mut buf = data;
        let size = buf.len() as u64;
        self.modify_file(file_id, file_offset, &mut buf, size).await
    }

    /// modify appender file to storage server (by local filesystem)
    /// # Arguments
    ///
    /// * `file_offset`: the offset of appender file
    pub async fn modify_file_local(
        &mut self,
        file_id: &FileId,
        file_offset: u64,
        local_filename: &str,
    ) -> Result<()> {
        let mut file = File::open(local_filename).await?;
        let file_size = file.metadata().await?.len();
        self.modify_file(file_id, file_offset, &mut file, file_size)
            .await
    }

    /// truncate appender file from storage server
    ///
    /// # Arguments
    ///
    /// * `file_id`: the file id of appender file
    /// * `truncated_file_size`: truncated file size
    pub async fn truncate_file(
        &mut self,
        file_id: &FileId,
        truncated_file_size: u64,
    ) -> Result<()> {
        let remote_filename_bytes = file_id.remote_filename.as_bytes();
        let body_len = 8 + 8 + remote_filename_bytes.len();
        let header = ProtoHeader::of(StorageCommand::TruncateFile.into()).len(body_len);

        header.send(&mut self.stream).await?;
        self.stream
            .write_u64(remote_filename_bytes.len() as u64)
            .await?;
        self.stream.write_u64(truncated_file_size).await?;
        self.stream.write_all(&remote_filename_bytes).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(())
    }

    /// truncate appender file to size 0 from storage server
    ///
    /// # Arguments
    ///
    /// * `file_id`: the file id of appender file
    pub async fn truncate_file0(&mut self, file_id: &FileId) -> Result<()> {
        self.truncate_file(file_id, 0).await
    }

    /// regenerate filename for appender file, since `v6.02`
    ///
    /// appender类型文件改名为普通文件
    ///
    /// # Arguments
    ///
    /// * `file_id`: the file id of appender file
    pub async fn regenerate_appender_filename(&mut self, file_id: &FileId) -> Result<FileId> {
        let remote_filename_bytes = file_id.remote_filename.as_bytes();
        let body_len = remote_filename_bytes.len();
        let header =
            ProtoHeader::of(StorageCommand::RegenerateAppenderFilename.into()).len(body_len);

        header.send(&mut self.stream).await?;
        self.stream.write_all(&remote_filename_bytes).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;

        if body.len() <= FDFS_GROUP_NAME_MAX_LEN {
            return Err(FastDFSError::InvalidFileId(format!(
                "body length: {} <= {}",
                body.len(),
                FDFS_GROUP_NAME_MAX_LEN
            )));
        }

        decode_file_id(&body)
    }

    pub async fn set_metadata(
        &mut self,
        file_id: &FileId,
        metadata: &Metadata,
        flag: MetadataFlag,
    ) -> Result<()> {
        // Encode metadata
        let metadata_bytes = encode_metadata(metadata);
        let remote_filename_len = file_id.remote_filename.len();
        let file_id_bytes = encode_file_id(file_id);

        let body_len = 2 * 8 + 1 + file_id_bytes.len() + metadata_bytes.len();
        let header = ProtoHeader::of(StorageCommand::SetMetadata.into()).len(body_len);

        let mut body = BytesMut::with_capacity(body_len);
        body.put_u64(remote_filename_len as u64);
        body.put_u64(metadata_bytes.len() as u64);
        body.put_u8(flag as u8);
        body.put(file_id_bytes);
        body.put(metadata_bytes.as_ref());

        header.send(&mut self.stream).await?;
        self.stream.write_all_buf(&mut body).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(())
    }

    pub async fn get_metadata(&mut self, file_id: &FileId) -> Result<Metadata> {
        let mut file_id_bytes = encode_file_id(file_id);
        let header = ProtoHeader::of(StorageCommand::GetMetadata.into()).len(file_id_bytes.len());

        header.send(&mut self.stream).await?;
        self.stream.write_all_buf(&mut file_id_bytes).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }
        if recv_header.length == 0 {
            return Ok(Metadata::new());
        }
        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;
        let metadata = decode_metadata(&body)?;

        Ok(metadata)
    }

    /// query file info
    pub async fn get_file_info(&mut self, file_id: &FileId) -> Result<FileInfo> {
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
    pub async fn get_file_info_with_flag(
        &mut self,
        file_id: &FileId,
        flag: u8,
    ) -> Result<FileInfo> {
        let mut file_id_bytes = encode_file_id(file_id);
        let header = ProtoHeader::of(StorageCommand::QueryFileInfo.into())
            .len(file_id_bytes.len())
            .status(flag);

        header.send(&mut self.stream).await?;
        self.stream.write_all_buf(&mut file_id_bytes).await?;
        self.stream.flush().await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;
        if body.len() < 8 * 3 + FDFS_IPV4_SIZE {
            return Err(FastDFSError::InvalidResponse(
                "File info response too short".to_string(),
            ));
        }

        let file_info = decode_file_info(&body)?;
        Ok(file_info)
    }
}

//noinspection DuplicatedCode
impl StorageClient {
    pub async fn active_test(&mut self) -> Result<bool> {
        ACTIVE_TEST_HEADER
            .send_timeout(&mut self.stream, Duration::from_secs(5), self.addr)
            .await?;

        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(recv_header.is_ok())
    }

    pub async fn quit(mut self) -> Result<()> {
        self.stream.write_all(QUIT_HEADER.encode().as_ref()).await?;
        self.stream.flush().await?;
        self.stream.shutdown().await?;
        Ok(())
    }
}

pub(crate) struct StorageClientFactor {
    pub version: Version,
    pub connect_timeout: Duration,
}

//noinspection DuplicatedCode
impl KeyedPoolFactory<SocketAddr, StorageClient> for StorageClientFactor {
    fn create<'a>(&'a self, addr: &'a SocketAddr) -> BoxFuture<'a, Result<StorageClient>> {
        Box::pin(async move {
            let result = timeout(self.connect_timeout, StorageClient::connect(addr)).await;

            match result {
                Ok(Ok(mut client)) => {
                    client.version = self.version;
                    Ok(client)
                }
                Ok(Err(e)) => Err(e),
                Err(_) => Err(FastDFSError::ConnectionTimeout(addr.to_string())),
            }
        })
    }

    fn validate<'a>(
        &'a self,
        _addr: &'a SocketAddr,
        client: &'a mut StorageClient,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move { client.active_test().await })
    }

    fn destroy(&'_ self, _addr: &SocketAddr, client: StorageClient) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ok = client.quit().await.ok();
        })
    }
}
