use crate::protocol::{FDFS_IPV4_SIZE, FDFS_IPV6_SIZE, FDFS_VERSION_SIZE, FDFS_VERSION_SIZE_OLD};
use crate::types::{GroupStat, StorageServer, StorageStat};
use crate::types::{FDFS_GROUP_NAME_MAX_LEN, FDFS_STORAGE_ID_MAX_SIZE};
use crate::utils::secs_to_sys_time as sts;
use crate::utils::unpad_string;
use crate::FastDFSError;
use crate::Version;
use crate::Version::{V6100, V6110, V6120, V6130, V6132, V6150};
use bytes::{Buf, Bytes};

pub fn decode_storage_server(mut buf: &[u8]) -> crate::Result<StorageServer> {
    let body_len = buf.len();
    let ip_port_len = body_len - (FDFS_GROUP_NAME_MAX_LEN + 1);
    let ip_size = ip_port_len - 8;

    let store_path_index = buf[body_len - 1];
    // skip group_name filed
    buf.advance(FDFS_GROUP_NAME_MAX_LEN);
    let ip_buf = &buf[..ip_size];
    let ip_addr = unpad_string(ip_buf);
    buf.advance(ip_size);
    let port = buf.get_u64() as u16;

    Ok(StorageServer {
        ip_addr,
        port,
        store_path_index,
    })
}

pub fn decode_storage_servers(mut buf: &[u8], has_idx: bool) -> crate::Result<Vec<StorageServer>> {
    let body_len = buf.len();
    if body_len == 0 {
        return Ok(Vec::new());
    }
    let ip_port_len = body_len - (FDFS_GROUP_NAME_MAX_LEN + (if has_idx { 1 } else { 0 }));
    let ipv4_record_len = FDFS_IPV4_SIZE + 8 - 1;
    let ipv6_record_len = FDFS_IPV6_SIZE + 8 - 1;

    let ip_size;
    let record_len;
    if ip_port_len % ipv4_record_len == 0 {
        ip_size = FDFS_IPV4_SIZE - 1;
        record_len = ipv4_record_len;
    } else if ip_port_len % ipv6_record_len == 0 {
        ip_size = FDFS_IPV6_SIZE - 1;
        record_len = ipv6_record_len;
    } else {
        return Err(FastDFSError::InvalidResponse(format!(
            "Storage server response too short, body_len: {body_len}, ip_port_len: {ip_port_len}"
        )));
    }

    let server_len = ip_port_len / record_len;
    let mut servers = Vec::with_capacity(server_len);

    let store_path_index = if has_idx { buf[body_len - 1] } else { 0 };
    // skip group_name filed
    buf.advance(FDFS_GROUP_NAME_MAX_LEN);

    for _ in 0..server_len {
        let ip_buf = &buf[..ip_size];
        let ip_addr = unpad_string(ip_buf);
        buf.advance(ip_size);
        let port = buf.get_u64() as u16;

        servers.push(StorageServer {
            ip_addr,
            port,
            store_path_index,
        })
    }

    Ok(servers)
}

pub fn decode_group_stat(mut buf: &[u8], v: Version) -> crate::Result<GroupStat> {
    let group_buf = &buf[..FDFS_GROUP_NAME_MAX_LEN + 1];
    buf.advance(FDFS_GROUP_NAME_MAX_LEN + 1);
    let group_name = unpad_string(group_buf);

    let total_mb = buf.get_u64();
    let free_mb = buf.get_u64();

    let reserved_mb;
    if v > V6130 {
        // since v6.13.1, fdfs_monitor output reserved and available space - 2025/9/4
        reserved_mb = buf.get_u64();
    } else {
        reserved_mb = 0;
    }
    let trunk_free_mb = buf.get_u64();
    let storage_count = buf.get_u64() as u32;
    let storage_port = buf.get_u64() as u32;

    let readable_server_count;
    let writable_server_count;
    if v >= V6130 {
        // since v6.13.0, storage servers support read write separation - 2025/8/28
        readable_server_count = buf.get_u64() as u32;
        writable_server_count = buf.get_u64() as u32;
    } else {
        // since v6.13.0, remove useless HTTP relative codes - 2025/8/26
        buf.advance(8);
        let active_count = buf.get_u64() as u32;
        readable_server_count = active_count;
        writable_server_count = active_count;
    }
    let current_write_server = buf.get_u64() as u32;
    let store_path_count = buf.get_u64() as u32;
    let subdir_count_per_path = buf.get_u64() as u32;
    let current_trunk_file_id = buf.get_u64() as u32;

    Ok(GroupStat {
        group_name,
        total_mb,
        free_mb,
        reserved_mb,
        trunk_free_mb,
        storage_count,
        storage_port,
        readable_server_count,
        writable_server_count,
        current_write_server,
        store_path_count,
        subdir_count_per_path,
        current_trunk_file_id,
    })
}

pub fn decode_group_stats(mut buf: &[u8], mut v: Version) -> crate::Result<Vec<GroupStat>> {
    const FIELDS_TOTAL_SIZE_OLD: usize = FDFS_GROUP_NAME_MAX_LEN + 1 + 8 * 11;
    const FIELDS_TOTAL_SIZE: usize = FDFS_GROUP_NAME_MAX_LEN + 1 + 8 * 12;
    let body_len = buf.len();
    let record_len;
    if body_len % FIELDS_TOTAL_SIZE_OLD == 0 {
        record_len = FIELDS_TOTAL_SIZE_OLD;
        v = v.min(V6130);
    } else if body_len % FIELDS_TOTAL_SIZE == 0 {
        record_len = FIELDS_TOTAL_SIZE;
        v = v.max(V6132);
    } else {
        return Err(FastDFSError::InvalidResponse(format!(
            "Group stats response too short. {v}, decode body_len: {body_len}."
        )));
    }
    let count = body_len / record_len;
    let mut stats = Vec::with_capacity(count);
    for _ in 0..count {
        let stat = decode_group_stat(&buf[..record_len], v)?;
        buf.advance(record_len);
        stats.push(stat);
    }

    Ok(stats)
}

pub fn decode_storage_stats(body: Vec<u8>, mut v: Version) -> crate::Result<Vec<StorageStat>> {
    let body_len = body.len();
    if body_len == 0 {
        return Ok(Vec::new());
    }
    // V6.11 及之前版本
    const FIELDS_SIZE_OLD: usize = 590 + 16 + 6;
    // V6.11 及更高版本为了适配 IPv6, ipPort 长度为 46 。之前版本的长度为 16
    const FIELDS_SIZE_V611: usize = 590 + 46 + 6;
    // V6.12 及更高版本 FDFS_VERSION_SIZE 长度 为 8 。之前版本的长度为 6
    const FIELDS_SIZE_V612: usize = 590 + 46 + 8;
    // V6.13 及更高版本 删除 domain_name 和 storage_http_port 字段，添加 rw_mode 字段
    const FIELDS_SIZE_V613: usize = 590 + 46 + 8 - 128 - 8 + 1;
    // V6.13.1 及更高版本 添加 reserved_mb 字段
    const FIELDS_SIZE_V6131: usize = 590 + 46 + 8 - 128 - 8 + 1 + 8;
    // V6.15 开始支持手动设置ip大小
    const FIELDS_SIZE_V615_IPV4: usize = 590 + 16 + 8 - 128 - 8 + 1 + 8;

    let ip_len: usize;
    let record_len: usize;
    if body_len % FIELDS_SIZE_OLD == 0 {
        record_len = FIELDS_SIZE_OLD;
        ip_len = FDFS_IPV4_SIZE;
        v = v.min(V6100);
    } else if body_len % FIELDS_SIZE_V611 == 0 {
        record_len = FIELDS_SIZE_V611;
        ip_len = FDFS_IPV6_SIZE;
        v = V6110;
    } else if body_len % FIELDS_SIZE_V612 == 0 {
        record_len = FIELDS_SIZE_V612;
        ip_len = FDFS_IPV6_SIZE;
        v = V6120;
    } else if body_len % FIELDS_SIZE_V613 == 0 {
        record_len = FIELDS_SIZE_V613;
        ip_len = FDFS_IPV6_SIZE;
        v = V6130;
    } else if body_len % FIELDS_SIZE_V6131 == 0 {
        record_len = FIELDS_SIZE_V6131;
        ip_len = FDFS_IPV6_SIZE;
        v = v.max(V6132);
    } else if body_len % FIELDS_SIZE_V615_IPV4 == 0 {
        record_len = FIELDS_SIZE_V615_IPV4;
        ip_len = FDFS_IPV4_SIZE;
        v = v.max(V6150);
    } else {
        return Err(FastDFSError::InvalidResponse(format!(
            "Storage stats response too short. {v}, decode body_len: {body_len}."
        )));
    }

    let version_len = if v < V6120 {
        FDFS_VERSION_SIZE_OLD
    } else {
        FDFS_VERSION_SIZE
    };

    let mut buf = Bytes::from(body);
    let count = body_len / record_len;
    let mut stats = Vec::with_capacity(count);
    for _ in 0..count {
        let status = buf.get_u8();
        // since v6.13
        let rw_mode = if v >= V6130 { buf.get_u8() } else { 0 };
        let id = unpad_string(buf.split_to(FDFS_STORAGE_ID_MAX_SIZE).as_ref());
        let ip_addr = unpad_string(buf.split_to(ip_len).as_ref());
        if v < V6130 {
            // domain_name：128字节字符串，域名（V6.13删除该字段）
            buf.advance(128);
        }
        let src_id = unpad_string(buf.split_to(FDFS_STORAGE_ID_MAX_SIZE).as_ref());
        let version = unpad_string(buf.split_to(version_len).as_ref());
        let join_time = sts(buf.get_u64());
        let up_time = sts(buf.get_u64());
        let total_mb = buf.get_u64();
        let free_mb = buf.get_u64();
        // 磁盘预留空间，单位MB since v6.13.1
        let reserved_mb = if v >= V6132 { buf.get_u64() } else { 0 };
        let upload_priority = buf.get_u64() as u32;
        let store_path_count = buf.get_u64() as u32;
        let subdir_count_per_path = buf.get_u64() as u32;
        let current_write_path = buf.get_u64() as u32;
        let storage_port = buf.get_u64() as u32;
        if v < V6130 {
            // storage_http_port: 8字节整数，HTTP服务端口号（V6.13删除该字段）
            buf.advance(8);
        }
        let connection_alloc_count = buf.get_u32();
        let connection_current_count = buf.get_u32();
        let connection_max_count = buf.get_u32();

        let total_upload_count = buf.get_u64();
        let success_upload_count = buf.get_u64();
        let total_append_count = buf.get_u64();
        let success_append_count = buf.get_u64();
        let total_modify_count = buf.get_u64();
        let success_modify_count = buf.get_u64();
        let total_truncate_count = buf.get_u64();
        let success_truncate_count = buf.get_u64();
        let total_set_meta_count = buf.get_u64();
        let success_set_meta_count = buf.get_u64();
        let total_delete_count = buf.get_u64();
        let success_delete_count = buf.get_u64();
        let total_download_count = buf.get_u64();
        let success_download_count = buf.get_u64();
        let total_get_meta_count = buf.get_u64();
        let success_get_meta_count = buf.get_u64();
        let total_create_link_count = buf.get_u64();
        let success_create_link_count = buf.get_u64();
        let total_delete_link_count = buf.get_u64();
        let success_delete_link_count = buf.get_u64();

        let total_upload_bytes = buf.get_u64();
        let success_upload_bytes = buf.get_u64();
        let total_append_bytes = buf.get_u64();
        let success_append_bytes = buf.get_u64();
        let total_modify_bytes = buf.get_u64();
        let success_modify_bytes = buf.get_u64();
        let total_download_bytes = buf.get_u64();
        let success_download_bytes = buf.get_u64();
        let total_sync_in_bytes = buf.get_u64();
        let success_sync_in_bytes = buf.get_u64();
        let total_sync_out_bytes = buf.get_u64();
        let success_sync_out_bytes = buf.get_u64();
        let total_file_open_count = buf.get_u64();
        let success_file_open_count = buf.get_u64();
        let total_file_read_count = buf.get_u64();
        let success_file_read_count = buf.get_u64();
        let total_file_write_count = buf.get_u64();
        let success_file_write_count = buf.get_u64();

        let last_source_update = sts(buf.get_u64());
        let last_sync_update = sts(buf.get_u64());
        let last_synced_timestamp = sts(buf.get_u64());
        let last_heart_beat_time = sts(buf.get_u64());

        let if_trunk_server = buf.get_u8() != 0;

        stats.push(StorageStat {
            status,
            rw_mode,
            id,
            ip_addr,
            src_id,
            version,
            join_time,
            up_time,
            total_mb,
            free_mb,
            reserved_mb,
            upload_priority,
            store_path_count,
            subdir_count_per_path,
            storage_port,
            current_write_path,
            connection_alloc_count,
            connection_current_count,
            connection_max_count,
            total_upload_count,
            success_upload_count,
            total_append_count,
            success_append_count,
            total_modify_count,
            success_modify_count,
            total_truncate_count,
            success_truncate_count,
            total_set_meta_count,
            success_set_meta_count,
            total_delete_count,
            success_delete_count,
            total_download_count,
            success_download_count,
            total_get_meta_count,
            success_get_meta_count,
            total_create_link_count,
            success_create_link_count,
            total_delete_link_count,
            success_delete_link_count,
            total_upload_bytes,
            success_upload_bytes,
            total_append_bytes,
            success_append_bytes,
            total_modify_bytes,
            success_modify_bytes,
            total_download_bytes,
            success_download_bytes,
            total_sync_in_bytes,
            success_sync_in_bytes,
            total_sync_out_bytes,
            success_sync_out_bytes,
            total_file_open_count,
            success_file_open_count,
            total_file_read_count,
            success_file_read_count,
            total_file_write_count,
            success_file_write_count,
            last_source_update,
            last_sync_update,
            last_synced_timestamp,
            last_heart_beat_time,
            if_trunk_server,
        });
    }

    Ok(stats)
}
