#![allow(dead_code)]

use crate::pool::pool::PoolInner;
use crate::pool::KeyedPoolFactory;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::OwnedSemaphorePermit;

/// Provides all possible states of a [`PooledObject`].
#[derive(Eq, PartialEq, Copy, Clone)]
pub enum PooledObjectState {
    /**
     * In the queue, not in use.
     */
    Idle,

    /**
     * In use.
     */
    Allocated,

    /**
     * In the queue, currently being tested for possible eviction.
     */
    Eviction,

    /**
     * Not in the queue, currently being tested for possible eviction. An attempt to borrow the object was made while
     * being tested which removed it from the queue. It should be returned to the head of the queue once eviction
     * testing completes.
     * <p>
     * TODO: Consider allocating object and ignoring the result of the eviction test.
     * </p>
     */
    EvictionReturnToHead,

    /**
     * In the queue, currently being validated.
     */
    Validation,

    /**
     * Not in queue, currently being validated. The object was borrowed while being validated and since testOnBorrow was
     * configured, it was removed from the queue and pre-allocated. It should be allocated once validation completes.
     */
    ValidationPreallocated,

    /**
     * Not in queue, currently being validated. An attempt to borrow the object was made while previously being tested
     * for eviction which removed it from the queue. It should be returned to the head of the queue once validation
     * completes.
     */
    ValidationReturnToHead,

    /**
     * Failed maintenance (e.g. eviction test or validation) and will be / has been destroyed
     */
    Invalid,

    /**
     * Deemed abandoned, to be invalidated.
     */
    Abandoned,

    /**
     * Returning to the pool.
     */
    Returning,
}

pub struct PooledObject<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    pub(super) obj: Option<T>, // 使用 Option 以便取出所有权
    pub(super) key: Option<K>,
    pub(crate) state: PooledObjectState,
    pub(super) create: Instant,
    pub(super) last_borrow: Instant,
    pub(super) last_use: Instant,
    pub(super) last_return: Instant,
    pub(super) borrowed_count: u64,
    // 持有 Pool 的引用，以便 Drop 时能找到归还的地方
    pub(super) pool: Option<Arc<PoolInner<K, T, F>>>,
    // 使用 OwnedSemaphorePermit 可以安全地在结构体中持有并在跨线程移动
    pub(super) _permit: Option<OwnedSemaphorePermit>,
}

impl<K, T, F> PooledObject<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    /// setup last_use instant
    pub fn in_use(&mut self) {
        self.last_use = Instant::now();
    }

    pub(super) fn drop_return_new(&self) -> Self {
        PooledObject {
            state: self.state,
            create: self.create,
            last_borrow: self.last_borrow,
            last_use: self.last_use,
            last_return: self.last_return,
            borrowed_count: self.borrowed_count,
            pool: None,
            _permit: None,
            obj: None,
            key: None,
        }
    }

    pub(super) fn take(&mut self) -> Option<T> {
        self.obj.take()
    }
}

impl<K, T, F> Default for PooledObject<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    fn default() -> Self {
        Self {
            obj: None,
            key: None,
            state: PooledObjectState::Idle,
            create: Instant::now(),
            last_borrow: Instant::now(),
            last_use: Instant::now(),
            last_return: Instant::now(),
            borrowed_count: 0,
            pool: None,
            _permit: None,
        }
    }
}

// 实现 Deref 让使用者像使用 T 一样使用 PooledObject
impl<K, T, F> std::ops::Deref for PooledObject<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.obj.as_ref().unwrap()
    }
}

impl<K, T, F> std::ops::DerefMut for PooledObject<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.obj.as_mut().unwrap()
    }
}

// -------------------------------------------------------------------
// 实现 Drop 自动归还
// -------------------------------------------------------------------
impl<K, T, F> Drop for PooledObject<K, T, F>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    F: KeyedPoolFactory<K, T>,
{
    fn drop(&mut self) {
        // 无效的、废弃的不回收
        match self.state {
            PooledObjectState::Eviction
            | PooledObjectState::Invalid
            | PooledObjectState::Abandoned
            | PooledObjectState::Returning => return,
            _ => {}
        };

        // 1. 取出对象 (如果已经被手动 return 过，这里就是 None)
        if let Some(mut obj) = self.obj.take() {
            let Some(key) = self.key.clone() else {
                return;
            };
            let Some(pool) = self.pool.clone() else {
                return;
            };

            self._permit = None;
            self.state = PooledObjectState::Returning;
            self.last_return = Instant::now();
            let new_pooed = self.drop_return_new();

            // 2. 启动一个后台任务来处理归还逻辑
            // 因为 Drop 是同步的，不能 await，所以必须 spawn
            tokio::spawn(async move {
                let mut new_pooed = new_pooed;
                // 这里的逻辑与之前的 return_object 一致
                let pools_read = pool.pools.read().await;
                let Some(queue_arc) = pools_read.get(&key) else {
                    // 如果连接池里没有对应的key，说明已被移除，不必归还
                    return;
                };

                if pool.config.test_on_return
                    && !pool
                        .validate_pooled(&key, &mut new_pooed, Some(&mut obj))
                        .await
                        .unwrap_or(false)
                {
                    return;
                }

                new_pooed.key = Some(key);
                new_pooed.pool = Some(pool.clone());
                new_pooed.obj = Some(obj);
                let mut queue = queue_arc.lock().await;
                new_pooed.state = PooledObjectState::Idle;
                queue.push_back(new_pooed);

                // 注意：函数结束时，_permit 会自动 Drop，信号量许可归还
            });
        }
        // 如果 obj 是 None，说明用户手动调用了 return_object 或者对象被丢弃了
        // 此时什么都不做，permit 会随着 PooledObject 的 drop 自动释放
    }
}
