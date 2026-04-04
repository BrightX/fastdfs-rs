mod config;
mod pool;
mod pooled_object;

pub use config::PoolConfig;
pub use pool::KeyedObjectPool;
pub use pooled_object::PooledObject;
use std::future::Future;
use std::pin::Pin;

use crate::Result;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[allow(unused_variables)]
pub trait KeyedPoolFactory<K, T>: Send + Sync + 'static {
    fn create<'a>(&'a self, key: &'a K) -> BoxFuture<'a, Result<T>>;
    fn validate<'a>(&'a self, key: &'a K, obj: &'a mut T) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async { Ok(true) })
    }
    fn destroy<'a>(&'a self, key: &'a K, obj: T) -> BoxFuture<'a, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct MyConn {
        id: u32,
    }
    struct MyFactory;

    impl KeyedPoolFactory<String, MyConn> for MyFactory {
        fn create<'a>(&'a self, key: &'a String) -> BoxFuture<'a, Result<MyConn>> {
            Box::pin(async move {
                println!("-> [Factory] Creating for key: {}", key);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(MyConn {
                    id: std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u32,
                })
            })
        }

        fn destroy<'a>(&'a self, key: &'a String, obj: MyConn) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                println!("-> [Factory] Destroying {} for {}", obj.id, key);
            })
        }
    }

    #[tokio::test]
    async fn test_keyed_pool() {
        let pool_config = PoolConfig::new().max_total_per_key(2);
        let pool = KeyedObjectPool::new(MyFactory, Some(pool_config));
        let key = "db1".to_string();

        let mut handles = vec![];

        // 启动 5 个任务竞争 2 个连接
        for i in 0..5 {
            let p = pool.clone();
            let k = key.clone();

            handles.push(tokio::spawn(async move {
                println!("Task {} borrowing...", i);
                let conn = p.borrow(&k).await.unwrap();
                println!("Task {} got conn id: {}", i, conn.obj.as_ref().unwrap().id);

                // 模拟业务耗时
                tokio::time::sleep(Duration::from_millis(100)).await;

                println!("Task {} returning...", i);
                p.return_object(conn).await;
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        println!("All done.");
    }
}
