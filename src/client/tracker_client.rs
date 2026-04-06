use crate::errors::map_status_to_error;
use crate::pool::{BoxFuture, KeyedPoolFactory};
use crate::protocol::decode::{
    decode_group_stats, decode_storage_server, decode_storage_servers, decode_storage_stats,
};
use crate::protocol::ProtoHeader;
use crate::protocol::TrackerRequest;
use crate::protocol::ACTIVE_TEST_HEADER;
use crate::protocol::QUIT_HEADER;
use crate::types::{GroupStat, StorageServer, StorageStat};
use crate::FileId;
use crate::Version;
use crate::{FastDFSError, Result};
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::timeout;

pub struct TrackerClient {
    stream: BufStream<TcpStream>,
    addr: Option<SocketAddr>,
    version: Version,
}

impl Debug for TrackerClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerClient")
            .field("addr", &self.addr)
            .field("version", &self.version)
            .finish()
    }
}

//noinspection DuplicatedCode
impl TrackerClient {
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
        }
    }

    pub fn version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }
}

impl TrackerClient {
    /// query storage servers to upload file
    pub async fn get_store_storage<S: AsRef<str>>(
        &mut self,
        group_name: Option<S>,
    ) -> Result<StorageServer> {
        TrackerRequest::query_store_one(group_name)
            .send(&mut self.stream)
            .await?;
        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }
        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;

        decode_storage_server(&body)
    }

    /// query storage servers to upload file
    pub async fn get_store_storages<S: AsRef<str>>(
        &mut self,
        group_name: Option<S>,
    ) -> Result<Vec<StorageServer>> {
        TrackerRequest::query_store_all(group_name)
            .send(&mut self.stream)
            .await?;
        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }
        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;

        decode_storage_servers(&body, true)
    }

    async fn get_storages(&mut self, req: TrackerRequest) -> Result<Vec<StorageServer>> {
        req.send(&mut self.stream).await?;
        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }
        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;

        decode_storage_servers(&body, false)
    }

    /// query storage server to download file
    pub async fn get_fetch_storages(&mut self, file_id: &FileId) -> Result<Vec<StorageServer>> {
        let req = TrackerRequest::query_fetch_all(file_id);
        self.get_storages(req).await
    }

    /// query storage server to download file
    pub async fn get_fetch_storage(&mut self, file_id: &FileId) -> Result<StorageServer> {
        let req = TrackerRequest::query_fetch_one(file_id);
        let servers = self.get_storages(req).await?;
        let Some(server) = servers.get(0) else {
            return Err(FastDFSError::NoStorageServer);
        };
        Ok(server.to_owned())
    }

    /// query storage server to update file (delete file or set meta data)
    pub async fn get_update_storage(&mut self, file_id: &FileId) -> Result<StorageServer> {
        let req = TrackerRequest::query_update(file_id);
        let servers = self.get_storages(req).await?;
        let Some(server) = servers.get(0) else {
            return Err(FastDFSError::NoStorageServer);
        };
        Ok(server.to_owned())
    }

    /// query group stat info
    pub async fn list_groups(&mut self) -> Result<Vec<GroupStat>> {
        let req = TrackerRequest::list_all_groups();
        req.send(&mut self.stream).await?;
        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }
        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;

        decode_group_stats(&body, self.version)
    }

    /// query storage server stat info of the group
    pub async fn list_storages<S0: AsRef<str>, S1: AsRef<str>>(
        &mut self,
        group: S0,
        server_id: Option<S1>,
    ) -> Result<Vec<StorageStat>> {
        let req = TrackerRequest::list_storage(group, server_id);
        req.send(&mut self.stream).await?;
        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        let mut body = vec![0; recv_header.length as usize];
        self.stream.read_exact(&mut body).await?;

        decode_storage_stats(body, self.version)
    }

    /// delete a storage server from the tracker server
    pub async fn delete_storage<S0: AsRef<str>, S1: AsRef<str>>(
        &mut self,
        group: S0,
        server_id: S1,
    ) -> Result<bool> {
        let req = TrackerRequest::delete_storage(group, server_id);
        req.send(&mut self.stream).await?;
        let recv_header = ProtoHeader::read(&mut self.stream).await?;
        if !recv_header.is_ok() {
            if let Some(err) = map_status_to_error(recv_header.status) {
                return Err(err);
            }
        }

        Ok(recv_header.is_ok())
    }
}

//noinspection DuplicatedCode
impl TrackerClient {
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

pub(crate) struct TrackerClientFactor {
    pub version: Version,
    pub connect_timeout: Duration,
}

//noinspection DuplicatedCode
impl KeyedPoolFactory<SocketAddr, TrackerClient> for TrackerClientFactor {
    fn create<'a>(&'a self, addr: &'a SocketAddr) -> BoxFuture<'a, Result<TrackerClient>> {
        Box::pin(async move {
            let result = timeout(self.connect_timeout, TrackerClient::connect(addr)).await;

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
        client: &'a mut TrackerClient,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move { client.active_test().await })
    }

    fn destroy(&'_ self, _addr: &SocketAddr, client: TrackerClient) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ok = client.quit().await.ok();
        })
    }
}
