# FastDFS Rust SDK

> * support FastDFS `V5.04` ~ `V6.15.4`, or later.
>
> * support IPv4 / IPv6 (since FastDFS V6.11)

[![github](https://img.shields.io/badge/Github-BrightX/fastdfs--rs-blue?logo=github)](https://github.com/BrightX/fastdfs-rs)
[![gitee](https://img.shields.io/badge/Gitee-BrightXu/fastdfs--rs-8da0cb?labelColor=C71D23&logo=gitee)](https://gitee.com/BrightXu/fastdfs-rs)
[![crate](https://img.shields.io/crates/v/fastdfs.svg?logo=rust)](https://crates.io/crates/fastdfs)
[![documentation](https://img.shields.io/badge/docs.rs-fastdfs-66c2a5?labelColor=555555&logo=docs.rs)](https://docs.rs/fastdfs)
[![minimum rustc 1.71](https://img.shields.io/badge/rustc-1.71+-red.svg?logo=rust)](https://rust-lang.github.io/rfcs/2495-min-rust-version.html)
![License](https://img.shields.io/crates/l/fastdfs)

## Install

```toml
# Cargo.toml
fastdfs = "0.1"

tokio = { version = "1", features = ["full"] }
```

## Usage

### 快速入门

```rust
use fastdfs::types::Metadata;
use fastdfs::{ClientOptions, FdfsClient, FileId, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // configure your client
    let client = ClientOptions::new(vec!["127.0.0.1:22122"]).build()?;
    let data = br#"
# hello
> This is a text file, from buffer.
okay :-) .
"#;

    let mut metadata = Metadata::new();
    metadata.insert("filename".to_string(), "test.md".to_string());
    // upload your file from buffer
    let file_id = client
        .upload_file_buf(data, "md", Some(&metadata), false)
        .await?;
    println!("file_id: {:?}", file_id);

    // file exists
    let file_exists = client.file_exists(&file_id).await?;
    println!("file_exists: {}", file_exists);
    assert_eq!(file_exists, true, "file not exists");

    // query file metadata
    let file_metadata = client.get_metadata(&file_id).await?;
    println!("file_metadata: {:?}", file_metadata);

    // download your file to buffer
    let data_buf = client.download_file_buf(&file_id, 0, 0).await?;
    println!("data_buf: {:?}", data_buf);
    assert_eq!(data_buf.as_ref(), data.as_ref(), "file data inconsistent");

    // delete your file
    client.delete_file(&file_id).await?;

    // The deleted file is no longer there
    let file_exists = client.file_exists(&file_id).await?;
    println!("file_exists: {}", file_exists);
    assert_eq!(file_exists, false, "file delete error");

    Ok(())
}
```


