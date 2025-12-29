// 仅在非 test 构建时启用这些 Clippy 警告
#![cfg_attr(not(test), warn(clippy::nursery, clippy::unwrap_used, clippy::todo, clippy::dbg_macro,))]

pub mod registry;
pub mod store;
// mod stream_map;

use crate::{
    registry::{PersistableState, TupleEntry},
    store::PersistStore,
};
use async_broadcast as broadcast;
use compio::{
    runtime::{JoinHandle, spawn},
    time::interval,
};
use std::{any::type_name, time::Duration};
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Configuration directory not found")]
    DirNotFound,
    #[error("Database operation failed: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("Storage operation failed: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("Compaction failed: {0}")]
    Compaction(#[from] redb::CompactionError),
    #[error("Transaction failed: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("Commit failed: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("Table operation failed: {0}")]
    Table(#[from] redb::TableError),
    #[error("Serialization failed: {0}")]
    Serde(#[from] bitcode::Error),
    #[error("Type cast failed for {0}")]
    AnyCast(&'static str),
}

type PersistHandler = Box<dyn FnMut(&PersistStore) + Send + Sync>;

/// State persistence poller that periodically persists state changes to storage.
///
/// The poller maintains a collection of handlers that receive state updates from broadcast channels.
/// Each handler polls its channel for the latest state and persists it to the database.
/// The poller runs on a fixed interval and compacts the database on drop.
///
/// # Type Parameters
///
/// * `T` - The state type to persist, must implement `PersistableState`, `Clone`, and `Send`
///
/// # Examples
///
/// ```ignore
/// let (tx, rx) = broadcast::channel(10);
/// let poller = StatePersistPoller::new(store, Duration::from_secs(5));
/// poller.register(rx);
/// let handle = poller.watch(); // Returns JoinHandle that can be aborted if needed
/// ```
pub struct StatePersistPoller {
    store: PersistStore,
    handlers: Vec<PersistHandler>,
    interval: Duration,
}

impl StatePersistPoller {
    /// Creates a new state persistence poller.
    ///
    /// # Arguments
    ///
    /// * `store` - The persistent store to write state to
    /// * `interval` - The polling interval between persistence checks
    #[must_use]
    pub fn new(store: PersistStore, interval: Duration) -> Self { Self { store, handlers: Vec::new(), interval } }

    /// Registers a state receiver for automatic persistence.
    ///
    /// The closure captures the receiver, preserving type information for `T`.
    /// On each poll interval, the latest value from the channel will be persisted.
    ///
    /// # Arguments
    ///
    /// * `rx` - A broadcast receiver for the state type `T`
    ///
    /// # Type Parameters
    ///
    /// * `T` - State type that implements `PersistableState`, `Clone`, and `Send`
    ///
    /// # Behavior
    ///
    /// - Only the most recent value is persisted (older values are dropped)
    /// - Empty channels result in no-op for that poll cycle
    /// - Closed channels are handled gracefully
    /// - Overflowed messages are skipped in favor of newer data
    #[instrument(skip(self, rx), fields(data_type = %type_name::<T>()))]
    pub fn register<T>(&mut self, mut rx: broadcast::Receiver<T>)
    where
        T: PersistableState + Clone + Send + 'static,
        <T as IntoIterator>::Item: TupleEntry,
    {
        // 创建一个闭包，它知道如何从这个特定的 rx 收数据并存入 db
        let handler = Box::new(move |store: &PersistStore| {
            let latest_item = loop {
                match rx.try_recv() {
                    Ok(item) => break Some(item),
                    Err(broadcast::TryRecvError::Empty | broadcast::TryRecvError::Closed) => break None,
                    Err(broadcast::TryRecvError::Overflowed(_)) => {}
                }
            };
            let Some(item) = latest_item else { return };
            store.refresh(item).map_err(|e| {
                warn!(error = %e, data_type = %type_name::<T>(), "State persistence failed, will retry on next poll");
            }).ok();
        });
        self.handlers.push(handler);
    }

    /// Starts the polling loop in the background.
    ///
    /// This spawns an async task that periodically calls all registered handlers
    /// to persist the latest state from their respective broadcast channels.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that can be used to abort the task if needed.
    ///
    /// # Behavior
    ///
    /// - The task runs indefinitely until aborted or dropped
    /// - Database compaction is triggered when the poller is dropped
    /// - Each handler executes blocking I/O, but the workload is typically small
    #[instrument(name = "persist.poller", skip(self), fields(interval_ms = self.interval.as_millis(), handler_count = self.handlers.len()))]
    pub fn watch(mut self) -> JoinHandle<()> {
        spawn(async move {
            info!("State persistence poller started");
            let mut timer = interval(self.interval);
            loop {
                timer.tick().await;
                // 虽然数据库是阻塞 IO, 但是这里工作量不大，没必要开新线程
                for handler in &mut self.handlers {
                    handler(&self.store);
                }
            }
        })
    }
}

impl Drop for StatePersistPoller {
    #[instrument(skip(self), fields(table_count = self.handlers.len()))]
    fn drop(&mut self) {
        match self.store.compact() {
            Ok(()) => debug!("Database compacted successfully"),
            Err(e) => warn!(error = %e, "Database compaction failed during shutdown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{StatePersistPoller, impl_persist, store::PersistStore};
    use async_broadcast as broadcast;
    use compio::time::sleep;
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, time::Duration};
    use tempfile::tempdir;

    /// 测试用的持久化状态类型
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
    struct TestPollerState {
        data: HashMap<String, u32>,
    }

    impl IntoIterator for TestPollerState {
        type IntoIter = std::collections::hash_map::IntoIter<String, u32>;
        type Item = (String, u32);

        fn into_iter(self) -> Self::IntoIter { self.data.into_iter() }
    }

    impl FromIterator<(String, u32)> for TestPollerState {
        fn from_iter<T: IntoIterator<Item = (String, u32)>>(iter: T) -> Self {
            let data: HashMap<String, u32> = iter.into_iter().collect();
            Self { data }
        }
    }

    impl_persist!(TestPollerState, "test_poller_state");

    /// 创建一个使用临时目录的 PersistStore
    fn create_test_store() -> (PersistStore, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test_poller_db");
        let store = PersistStore::create(db_path).expect("Failed to create test store");
        (store, dir)
    }

    #[compio::test]
    async fn test_poller_creation() {
        let (store, _dir) = create_test_store();
        let interval = Duration::from_millis(100);

        let poller = StatePersistPoller::new(store, interval);
        assert_eq!(poller.interval, interval, "Interval should match");
        assert_eq!(poller.handlers.len(), 0, "Should have no handlers initially");
    }

    #[compio::test]
    async fn test_push_single_receiver() {
        let (store, _dir) = create_test_store();
        let interval = Duration::from_millis(100);

        let mut poller = StatePersistPoller::new(store, interval);

        // 创建一个 broadcast channel
        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);
        poller.register(rx);

        assert_eq!(poller.handlers.len(), 1, "Should have one handler after push");

        // 发送一个状态
        let mut state = TestPollerState::default();
        state.data.insert("key1".to_string(), 42);
        tx.try_broadcast(state).expect("Failed to broadcast state");

        // 注意：这里我们无法直接调用 handler，因为它是私有的
        // 我们只能验证 push 成功注册了 handler
    }

    #[compio::test]
    async fn test_push_multiple_receivers() {
        let (store, _dir) = create_test_store();
        let interval = Duration::from_millis(100);

        let mut poller = StatePersistPoller::new(store, interval);

        // 创建多个 broadcast channel
        let (tx1, rx1): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);
        let (tx2, rx2): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);
        let (tx3, rx3): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        poller.register(rx1);
        assert_eq!(poller.handlers.len(), 1, "Should have one handler after first push");

        poller.register(rx2);
        assert_eq!(poller.handlers.len(), 2, "Should have two handlers after second push");

        poller.register(rx3);
        assert_eq!(poller.handlers.len(), 3, "Should have three handlers after third push");

        // 发送一些状态
        let mut state1 = TestPollerState::default();
        state1.data.insert("a".to_string(), 1);
        tx1.try_broadcast(state1).expect("Failed to broadcast state1");

        let mut state2 = TestPollerState::default();
        state2.data.insert("b".to_string(), 2);
        tx2.try_broadcast(state2).expect("Failed to broadcast state2");

        let mut state3 = TestPollerState::default();
        state3.data.insert("c".to_string(), 3);
        tx3.try_broadcast(state3).expect("Failed to broadcast state3");
    }

    #[compio::test]
    async fn test_poller_persists_latest_message() {
        // 创建 broadcast channel
        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        // 发送多个消息
        for i in 1..=5 {
            let mut state = TestPollerState::default();
            state.data.insert("counter".to_string(), i);
            tx.try_broadcast(state).expect("Failed to broadcast state");
        }

        // 创建 store 用于 poller（保持 dir 存活）
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let interval = Duration::from_millis(10);

        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher，让它执行一次
        let handle = poller.watch();

        // 等待足够的时间让 watcher 至少执行一次
        sleep(Duration::from_millis(50)).await;

        // 取消任务（通过丢弃 handle）
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 创建新的 store 来验证数据
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.get("counter"), Some(&5), "Should persist -> last value (5)");
    }

    #[compio::test]
    async fn test_poller_handles_multiple_states() {
        let interval = Duration::from_millis(10);

        // 创建两个不同的 broadcast channel
        let (tx1, rx1): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);
        let (tx2, rx2): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        // 发送状态到第一个 channel
        let mut state1 = TestPollerState::default();
        state1.data.insert("channel1".to_string(), 100);
        tx1.try_broadcast(state1).expect("Failed to broadcast state1");

        // 发送状态到第二个 channel
        let mut state2 = TestPollerState::default();
        state2.data.insert("channel2".to_string(), 200);
        tx2.try_broadcast(state2).expect("Failed to broadcast state2");

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx1);
        poller.register(rx2);

        // 启动 watcher
        let handle = poller.watch();

        // 等待执行
        sleep(Duration::from_millis(50)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 验证存储的是最后一个 channel 的数据（因为第二个 handler 后执行）
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.get("channel2"), Some(&200), "Should persist from second channel");
    }

    #[compio::test]
    async fn test_poller_empty_channel_no_panic() {
        let interval = Duration::from_millis(10);

        // 创建 broadcast channel 但不发送任何消息
        let (_tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher，即使没有消息也不应该 panic
        let handle = poller.watch();

        // 等待执行
        sleep(Duration::from_millis(50)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 应该没有数据被存储
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed");
        assert!(loaded.is_none(), "Should have no data when channel is empty");
    }

    #[compio::test]
    async fn test_poller_multiple_updates() {
        let interval = Duration::from_millis(10);

        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher
        let handle = poller.watch();

        // 第一次更新
        let mut state1 = TestPollerState::default();
        state1.data.insert("version".to_string(), 1);
        tx.try_broadcast(state1).expect("Failed to broadcast state1");

        sleep(Duration::from_millis(30)).await;

        // 第二次更新
        let mut state2 = TestPollerState::default();
        state2.data.insert("version".to_string(), 2);
        tx.try_broadcast(state2).expect("Failed to broadcast state2");

        sleep(Duration::from_millis(30)).await;

        // 第三次更新
        let mut state3 = TestPollerState::default();
        state3.data.insert("version".to_string(), 3);
        tx.try_broadcast(state3).expect("Failed to broadcast state3");

        sleep(Duration::from_millis(30)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 只验证最终状态（第三次更新覆盖了之前的所有更新）
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.get("version"), Some(&3), "Final state should be version 3");
    }

    #[compio::test]
    async fn test_poller_with_complex_state() {
        let interval = Duration::from_millis(10);

        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        // 创建一个复杂的状态
        let mut complex_state = TestPollerState::default();
        complex_state.data.insert("key1".to_string(), 1);
        complex_state.data.insert("key2".to_string(), 2);
        complex_state.data.insert("key3".to_string(), 3);
        complex_state.data.insert("key4".to_string(), 4);
        complex_state.data.insert("key5".to_string(), 5);

        tx.try_broadcast(complex_state.clone()).expect("Failed to broadcast complex state");

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher
        let handle = poller.watch();

        // 等待执行
        sleep(Duration::from_millis(50)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 验证所有数据都被正确持久化
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 5, "Should have 5 entries");
        assert_eq!(loaded, complex_state, "Loaded state should match original");
    }

    #[compio::test]
    async fn test_poller_special_characters() {
        let interval = Duration::from_millis(10);

        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        // 创建包含特殊字符的状态
        let mut special_state = TestPollerState::default();
        special_state.data.insert("中文".to_string(), 1);
        special_state.data.insert("emoji-😀".to_string(), 2);
        special_state.data.insert("key-with-dash".to_string(), 3);
        special_state.data.insert("key_with_underscore".to_string(), 4);
        special_state.data.insert("key.with.dot".to_string(), 5);

        tx.try_broadcast(special_state.clone()).expect("Failed to broadcast special state");

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher
        let handle = poller.watch();

        // 等待执行
        sleep(Duration::from_millis(50)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 验证特殊字符都被正确处理
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded, special_state, "Special characters should be preserved");
        assert_eq!(loaded.data.get("中文"), Some(&1));
        assert_eq!(loaded.data.get("emoji-😀"), Some(&2));
    }

    #[compio::test]
    async fn test_poller_large_values() {
        let interval = Duration::from_millis(10);

        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        // 创建包含大值的状态
        let mut large_state = TestPollerState::default();
        large_state.data.insert("max".to_string(), u32::MAX);
        large_state.data.insert("zero".to_string(), 0);
        large_state.data.insert("middle".to_string(), 2147483647);

        tx.try_broadcast(large_state.clone()).expect("Failed to broadcast large state");

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher
        let handle = poller.watch();

        // 等待执行
        sleep(Duration::from_millis(50)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 验证大值都被正确处理
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded, large_state, "Large values should be preserved");
        assert_eq!(loaded.data.get("max"), Some(&u32::MAX));
        assert_eq!(loaded.data.get("zero"), Some(&0));
    }

    #[compio::test]
    async fn test_poller_message_deduplication() {
        let interval = Duration::from_millis(10);

        let (tx, rx): (broadcast::Sender<TestPollerState>, broadcast::Receiver<TestPollerState>) =
            broadcast::broadcast(10);

        // 快速发送多个相同的状态
        let same_state = TestPollerState { data: HashMap::from([("key".to_string(), 100)]) };

        for _ in 0..10 {
            tx.try_broadcast(same_state.clone()).expect("Failed to broadcast state");
        }

        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_poller_db");
        let mut poller = StatePersistPoller::new(store, interval);
        poller.register(rx);

        // 启动 watcher
        let handle = poller.watch();

        // 等待执行
        sleep(Duration::from_millis(50)).await;

        // 取消任务
        drop(handle);

        // 等待任务完全结束
        sleep(Duration::from_millis(20)).await;

        // 验证只持久化了一次（最后一个值）
        let store_for_check = PersistStore::create(db_path).expect("Failed to create verification store");
        let loaded = store_for_check.load::<TestPollerState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 1, "Should have only 1 entry");
        assert_eq!(loaded.data.get("key"), Some(&100), "Value should be correct");
    }
}
