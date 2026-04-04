use super::FdfsClient;
use crate::Result;
use crate::Version;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// List of tracker server addresses in format "host:port"
    pub tracker_addrs: Vec<String>,
    /// Maximum number of connections per tracker server
    pub max_connections: usize,
    /// Timeout for establishing connections
    pub connect_timeout: Duration,
    /// Timeout for network I/O operations
    pub network_timeout: Duration,
    /// Timeout for idle connections in the pool
    pub idle_timeout: Duration,
    /// Number of retries for failed operations
    pub retry_count: usize,
    /// Version for FastDFS server
    pub version: Version,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            tracker_addrs: Default::default(),
            max_connections: 10,
            connect_timeout: Duration::from_secs(5),
            network_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            retry_count: 3,
            version: Version::latest(),
        }
    }
}

impl ClientOptions {
    /// Creates a new client configuration with tracker addresses
    pub fn new<S: AsRef<str>>(tracker_addrs: Vec<S>) -> Self {
        let tracker_addrs = tracker_addrs
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        Self {
            tracker_addrs,
            ..Default::default()
        }
    }

    /// Sets the maximum number of connections per server
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = n;
        self
    }

    /// Sets the connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the network timeout
    pub fn network_timeout(mut self, timeout: Duration) -> Self {
        self.network_timeout = timeout;
        self
    }

    /// Sets the idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sets the retry count
    pub fn retry_count(mut self, n: usize) -> Self {
        self.retry_count = n;
        self
    }

    /// Sets the server version
    pub fn version(mut self, v: Version) -> Self {
        self.version = v;
        self
    }

    pub fn build(self) -> Result<FdfsClient> {
        FdfsClient::new(self)
    }
}
