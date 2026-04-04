use crate::protocol::TrackerCommand;
use crate::protocol::TrackerCommand::*;
use crate::protocol::{ProtoHeader, FDFS_PROTO_HEADER_LEN};
use crate::types::FDFS_GROUP_NAME_MAX_LEN;
use crate::utils::{pad_str_buf, pad_string};
use crate::FileId;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;

pub struct TrackerRequest {
    pub header: ProtoHeader,
    pub body: Bytes,
}

fn some_has_byte<S: AsRef<str>>(s: &Option<S>) -> Option<&[u8]> {
    if let Some(s) = s {
        let s = s.as_ref();
        if s.is_empty() {
            return None;
        }
        Some(s.as_bytes())
    } else {
        None
    }
}

//noinspection DuplicatedCode
#[allow(dead_code)]
impl TrackerRequest {
    pub fn query_store_one<S: AsRef<str>>(group: Option<S>) -> Self {
        let (cmd, body) = if let Some(group) = group {
            let body = pad_string(group.as_ref(), FDFS_GROUP_NAME_MAX_LEN);
            (ServiceQueryStoreWithGroupOne, body)
        } else {
            (ServiceQueryStoreWithoutGroupOne, Bytes::new())
        };

        let header = ProtoHeader::of(cmd.into()).len(body.len());

        Self { header, body }
    }

    pub fn query_store_all<S: AsRef<str>>(group: Option<S>) -> Self {
        let (cmd, body) = if let Some(group) = group {
            let body = pad_string(group.as_ref(), FDFS_GROUP_NAME_MAX_LEN);
            (ServiceQueryStoreWithGroupAll, body)
        } else {
            (ServiceQueryStoreWithoutGroupAll, Bytes::new())
        };

        let header = ProtoHeader::of(cmd.into()).len(body.len());

        Self { header, body }
    }

    fn get_storages(cmd: TrackerCommand, file_id: &FileId) -> Self {
        let body = {
            let len = FDFS_GROUP_NAME_MAX_LEN + file_id.remote_filename.len();
            let mut buf = BytesMut::with_capacity(len);
            pad_str_buf(&mut buf, &file_id.group_name, FDFS_GROUP_NAME_MAX_LEN);
            buf.put(file_id.remote_filename.as_bytes());

            buf.freeze()
        };

        let header = ProtoHeader::of(cmd.into()).len(body.len());
        Self { header, body }
    }

    pub fn query_fetch_one(file_id: &FileId) -> Self {
        Self::get_storages(ServiceQueryFetchOne, file_id)
    }

    pub fn query_fetch_all(file_id: &FileId) -> Self {
        Self::get_storages(ServiceQueryFetchAll, file_id)
    }

    pub fn query_update(file_id: &FileId) -> Self {
        Self::get_storages(ServiceQueryUpdate, file_id)
    }

    pub fn list_all_groups() -> Self {
        let header = ProtoHeader::of(ServerListAllGroups.into());
        let body = Bytes::new();
        Self { header, body }
    }

    pub fn list_storage<S0: AsRef<str>, S1: AsRef<str>>(group: S0, server_id: Option<S1>) -> Self {
        let body = {
            let ip_addr = some_has_byte(&server_id);
            let len = FDFS_GROUP_NAME_MAX_LEN + ip_addr.map(|a| a.len()).unwrap_or(0);
            let mut buf = BytesMut::with_capacity(len);
            pad_str_buf(&mut buf, &group.as_ref(), FDFS_GROUP_NAME_MAX_LEN);
            if let Some(ip_addr) = ip_addr {
                buf.put_slice(ip_addr);
            }

            buf.freeze()
        };

        let header = ProtoHeader::of(ServerListStorage.into()).len(body.len());
        Self { header, body }
    }

    pub fn delete_storage<S0: AsRef<str>, S1: AsRef<str>>(group: S0, server_id: S1) -> Self {
        let body = {
            let ip_addr = server_id.as_ref().as_bytes();
            let len = FDFS_GROUP_NAME_MAX_LEN + ip_addr.len();
            let mut buf = BytesMut::with_capacity(len);
            pad_str_buf(&mut buf, &group.as_ref(), FDFS_GROUP_NAME_MAX_LEN);
            buf.put_slice(ip_addr);

            buf.freeze()
        };

        let header = ProtoHeader::of(ServerDeleteStorage.into()).len(body.len());
        Self { header, body }
    }
}

impl TrackerRequest {
    pub fn encode(self) -> Bytes {
        let len = FDFS_PROTO_HEADER_LEN + self.header.length as usize;
        let mut buf = BytesMut::with_capacity(len);
        self.header.put_buf(&mut buf);
        buf.put(self.body);

        buf.freeze()
    }

    pub async fn send<S: AsyncWriteExt + Unpin>(self, stream: &mut S) -> crate::Result<()> {
        stream.write_all(self.encode().as_ref()).await?;
        stream.flush().await?;
        Ok(())
    }
}
