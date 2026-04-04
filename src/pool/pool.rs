use crate::pool::config::PoolConfig;
use crate::pool::pooled_object::{PooledObject, PooledObjectState};
use crate::pool::KeyedPoolFactory;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::timeout;

pub(super) struct PoolInner<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    pub pools: RwLock<HashMap<K, Arc<Mutex<VecDeque<PooledObject<K, T, F>>>>>>,
    pub semaphores: RwLock<HashMap<K, Arc<Semaphore>>>,
    pub factory: Arc<F>,
    pub config: PoolConfig,
}

impl<K, T, F> PoolInner<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    async fn get_semaphore(&self, key: &K) -> Arc<Semaphore> {
        // 1. 先尝试读锁获取，避免不必要的写锁阻塞
        {
            let read_guard = self.semaphores.read().await;
            if let Some(sem) = read_guard.get(key) {
                return sem.clone();
            }
        }

        // 2. 如果没找到，获取写锁进行插入
        // 注意：这里存在 TOCTOU (Time Of Check To Time Of Use) 竞态条件
        // 即在释放读锁和获取写锁之间，其他线程可能已经插入了。
        // 所以获取写锁后要再次检查。
        let mut write_guard = self.semaphores.write().await;
        write_guard
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(self.config.max_total_per_key)))
            .clone()
    }

    pub(super) async fn validate_pooled(
        self: &Arc<Self>,
        key: &K,
        pooled: &mut PooledObject<K, T, F>,
        obj: Option<&mut T>,
    ) -> Result<bool> {
        let mut obj = match (obj, pooled.obj.as_mut()) {
            (Some(obj), _) => obj,
            (None, Some(obj)) => obj,
            (_, _) => return Ok(false),
        };

        pooled.state = PooledObjectState::Eviction;
        let result = timeout(
            self.config.test_timeout,
            self.factory.validate(key, &mut obj),
        );

        match result.await {
            Ok(Ok(r)) => {
                if r {
                    pooled.state = PooledObjectState::Idle
                } else {
                    pooled.state = PooledObjectState::Invalid
                }
                Ok(r)
            }
            Ok(Err(e)) => {
                pooled.state = PooledObjectState::Invalid;
                Err(e)
            }
            Err(_) => {
                // validate timeout
                pooled.state = PooledObjectState::Invalid;
                Ok(false)
            }
        }
    }

    /// 借出对象
    pub(super) async fn borrow(self: &Arc<Self>, key: &K) -> Result<PooledObject<K, T, F>> {
        let semaphore = self.get_semaphore(key).await;
        // 获取许可 (如果满了，这里会等待)
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        // 尝试从现有池中获取
        {
            let pools_read = self.pools.read().await;
            if let Some(queue_ref) = pools_read.get(key) {
                let mut queue = queue_ref.lock().await;
                while let Some(mut pooled) = queue.pop_front() {
                    let Some(mut obj) = pooled.take() else {
                        continue;
                    };
                    if !self.config.test_on_borrow
                        || self
                            .validate_pooled(key, &mut pooled, Some(&mut obj))
                            .await
                            .unwrap_or(false)
                    {
                        pooled.state = PooledObjectState::Allocated;
                        pooled.last_borrow = Instant::now();
                        pooled.last_use = Instant::now();
                        pooled.obj = Some(obj);
                        pooled._permit = Some(permit);
                        return Ok(pooled);
                    } else {
                        self.factory.destroy(key, obj).await;
                    }
                }
            }
        }

        // 没有可用对象，创建新的
        let obj = self.factory.create(key).await?;
        {
            let mut pools_write = self.pools.write().await;
            if !pools_write.contains_key(key) {
                pools_write.insert(
                    key.clone(),
                    Arc::new(Mutex::new(VecDeque::with_capacity(
                        self.config.max_total_per_key,
                    ))),
                );
            }
        }
        Ok(PooledObject {
            obj: Some(obj),
            key: Some(key.clone()),
            state: PooledObjectState::Allocated,
            borrowed_count: 1,
            pool: Some(self.clone()),
            _permit: Some(permit),
            ..PooledObject::default()
        })
    }

    /// 归还对象
    pub(super) async fn return_object(self: &Arc<Self>, pooled: PooledObject<K, T, F>) {
        // 无效的、废弃的不回收
        match pooled.state {
            PooledObjectState::Eviction
            | PooledObjectState::Invalid
            | PooledObjectState::Abandoned
            | PooledObjectState::Returning => return,
            _ => {}
        };

        let mut entry = pooled;
        if let Some(mut obj) = entry.take() {
            let Some(key) = entry.key.clone() else {
                return;
            };

            entry._permit = None;
            entry.state = PooledObjectState::Returning;
            entry.last_return = Instant::now();

            let mut new_pooed = entry.drop_return_new();

            let pools_read = self.pools.read().await;
            let Some(queue_arc) = pools_read.get(&key) else {
                // 如果连接池里没有对应的key，说明已被移除，不必归还
                return;
            };

            if self.config.test_on_return
                && !self
                    .validate_pooled(&key, &mut new_pooed, Some(&mut obj))
                    .await
                    .unwrap_or(false)
            {
                return;
            }

            new_pooed.key = Some(key);
            new_pooed.pool = Some(self.clone());
            new_pooed.obj = Some(obj);
            let mut queue = queue_arc.lock().await;
            new_pooed.state = PooledObjectState::Idle;
            queue.push_back(new_pooed);
        }
    }

    pub(super) async fn clear_key(self: &Arc<Self>, key: &K) {
        {
            let mut w_lock = self.pools.write().await;
            if let Some(pools) = w_lock.get_mut(key) {
                let mut guard = pools.lock().await;
                guard.clear();
            }
        }
        {
            let mut w_lock = self.semaphores.write().await;
            if let Some(semaphores) = w_lock.get_mut(key) {
                semaphores.close();
            }
            w_lock.insert(
                key.clone(),
                Arc::new(Semaphore::new(self.config.max_total_per_key)),
            );
        }
    }

    pub(super) async fn remove_key(self: &Arc<Self>, key: &K) {
        {
            let mut w_lock = self.pools.write().await;
            w_lock.remove(key);
        }
        {
            let mut w_lock = self.semaphores.write().await;
            w_lock.remove(key);
        }
    }
}

pub struct KeyedObjectPool<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    inner: Arc<PoolInner<K, T, F>>,
}

#[allow(dead_code)]
impl<K, T, F> KeyedObjectPool<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    pub fn new(factory: F, config: Option<PoolConfig>) -> Self {
        let inner = PoolInner {
            pools: RwLock::new(HashMap::new()),
            semaphores: RwLock::new(HashMap::new()),
            factory: Arc::new(factory),
            config: config.unwrap_or_default(),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn borrow(&self, key: &K) -> Result<PooledObject<K, T, F>> {
        self.inner.borrow(key).await
    }

    pub async fn get(&self, key: &K) -> Result<PooledObject<K, T, F>> {
        self.inner.borrow(key).await
    }

    pub async fn return_object(&self, pooled: PooledObject<K, T, F>) {
        self.inner.return_object(pooled).await
    }

    pub async fn clear_key(&self, key: &K) {
        self.inner.clear_key(key).await
    }

    pub async fn remove_key(&self, key: &K) {
        self.inner.remove_key(key).await
    }

    pub async fn remove_all(&self) {
        self.inner.pools.write().await.clear();
        self.inner.semaphores.write().await.clear();
    }
}

impl<K, T, F> Clone for KeyedObjectPool<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
