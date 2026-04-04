#![allow(dead_code)]

use std::time::Duration;

#[derive(Copy, Clone)]
pub struct PoolConfig {
    pub max_total_per_key: usize,
    pub max_wait: Option<Duration>,
    pub test_on_borrow: bool,
    pub test_on_return: bool,
    pub test_while_idle: bool,
    pub test_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_total_per_key: 50,
            max_wait: None,
            test_on_borrow: false,
            test_on_return: false,
            test_while_idle: true,
            test_timeout: Duration::from_secs(3),
        }
    }
}

impl PoolConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_total_per_key(mut self, max_total_per_key: usize) -> Self {
        self.max_total_per_key = max_total_per_key;
        self
    }

    pub fn max_wait(mut self, max_wait: Option<Duration>) -> Self {
        self.max_wait = max_wait;
        self
    }

    pub fn test_on_borrow(mut self, test_on_borrow: bool) -> Self {
        self.test_on_borrow = test_on_borrow;
        self
    }

    pub fn test_on_return(mut self, test_on_return: bool) -> Self {
        self.test_on_return = test_on_return;
        self
    }

    pub fn test_while_idle(mut self, test_while_idle: bool) -> Self {
        self.test_while_idle = test_while_idle;
        self
    }

    pub fn test_timeout(mut self, test_timeout: Duration) -> Self {
        self.test_timeout = test_timeout;
        self
    }
}
