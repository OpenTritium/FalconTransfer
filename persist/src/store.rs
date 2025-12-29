use crate::{
    Error as PersistError,
    registry::{PersistableState, TupleEntry},
};
use falcon_config::get_config_path;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError, TableHandle};
use std::{collections::HashMap, path::Path};
use tracing::{debug, info, instrument, trace, warn};
use xxhash_rust::xxh3::Xxh3Builder;

/// Persistent storage abstraction for state recovery and persistence.
///
/// `PersistStore` provides a typed interface for persisting and loading application state
/// using `redb` as the underlying database. It supports content-based deduplication via
/// xxHash to avoid unnecessary writes when data hasn't changed.
///
/// # Features
///
/// - Content-addressed storage using xxHash for change detection
/// - Automatic serialization/deserialization via `bitcode`
/// - Type-safe state persistence through the `PersistableState` trait
/// - Configurable database location (defaults to config directory)
///
/// # Examples
///
/// ```ignore
/// // Create a store at a specific path
/// let store = PersistStore::create("/path/to/db.redb")?;
///
/// // Or create in the default config directory
/// let store = PersistStore::new("app_state.redb")?;
///
/// // Persist some state
/// store.refresh(my_state)?;
///
/// // Load it back
/// let loaded: Option<MyState> = store.load()?;
/// ```
pub struct PersistStore {
    db: Database,
}

impl PersistStore {
    /// Table definition for storing content hashes (`table_name` -> hash).
    const HASH_METADATA_TABLE: TableDefinition<'_, &str, u64> = TableDefinition::new("hash_metadata");

    /// Creates a new persistent store at the specified path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the database file will be created
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created at the specified path.
    #[instrument(fields(path = %path.as_ref().display()))]
    pub fn create(path: impl AsRef<Path>) -> Result<Self, PersistError> {
        let db = Database::create(path.as_ref())?;
        debug!("Persist store created");
        Ok(Self { db })
    }

    /// Creates a new persistent store in the configuration directory.
    ///
    /// # Arguments
    ///
    /// * `db_name` - Name of the database file (will be created in the config directory)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The configuration directory cannot be located
    /// - The database cannot be created
    #[instrument(fields(db_name = %db_name))]
    pub fn new(db_name: &str) -> Result<Self, PersistError> {
        let cfg_path = get_config_path().map_err(|_| PersistError::DirNotFound)?;
        let cfg_dir = cfg_path.parent().ok_or(PersistError::DirNotFound)?;
        let path = cfg_dir.join(db_name);
        let db = Database::create(&path)?;
        debug!("Persist store created in config directory");
        Ok(Self { db })
    }

    /// Compacts the database to reclaim space.
    ///
    /// This operation can be expensive and should be called sparingly,
    /// typically during application shutdown or maintenance windows.
    ///
    /// # Errors
    ///
    /// Returns an error if compaction fails.
    #[instrument(skip(self))]
    pub fn compact(&mut self) -> Result<(), PersistError> {
        self.db.compact()?;
        debug!("Database compaction completed");
        Ok(())
    }

    /// Persists or updates state in the store.
    ///
    /// This method uses content hashing (xxHash) to detect changes:
    /// - If the content hash matches the stored hash, the write is skipped
    /// - Otherwise, the old table is deleted and replaced with new data
    ///
    /// # Arguments
    ///
    /// * `content` - The state to persist
    ///
    /// # Type Parameters
    ///
    /// * `T` - Type implementing `PersistableState`
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or database operations fail.
    ///
    /// # Performance
    ///
    /// The method computes a hash over all items, serializes them upfront,
    /// and only writes to disk if the content has changed.
    #[instrument(skip(self, content), fields(table_name = %T::TABLE_DEF.name()))]
    #[inline]
    pub fn refresh<T>(&self, content: T) -> Result<(), PersistError>
    where
        T: PersistableState,
        T::Item: TupleEntry,
    {
        //  生成 hash 并提前序列化（主要是没有借用迭代器导致的，不过考虑到影响并不大）
        let hasher = Xxh3Builder::new().build();
        let mut hash = 0u64;
        let mut serde_map: HashMap<Box<[u8]>, Box<[u8]>> = HashMap::new();
        for item in content {
            let mut hasher = hasher.clone();
            let (k, v) = item.unzip();
            let kb = bitcode::serialize(&k)?.into_boxed_slice();
            hasher.update(&kb);
            let vb = bitcode::serialize(&v)?.into_boxed_slice();
            hasher.update(&vb);
            hash ^= hasher.digest();
            serde_map.insert(kb, vb);
        }
        debug!(content_hash = %hash, entry_count = serde_map.len(), "Computed content hash");

        // 检查是否需要更新
        let should_update = {
            let txn = self.db.begin_read()?;
            match txn.open_table(Self::HASH_METADATA_TABLE) {
                // 表存在就进行 hash 对比
                Ok(t) => t.get(T::TABLE_DEF.name())?.map_or_else(
                    || {
                        // hash 记录不存在，需要更新
                        debug!("Hash record not found, performing initial persist");
                        true
                    },
                    |hash_acc| {
                        let old_hash = hash_acc.value();
                        if old_hash == hash {
                            debug!("Content unchanged, skipping update");
                            false
                        } else {
                            debug!(old_hash = %old_hash, new_hash = %hash, "Content hash changed, update required");
                            true
                        }
                    },
                ),
                Err(TableError::TableDoesNotExist(_)) => {
                    debug!("Hash metadata table does not exist, performing initial persist");
                    true
                }
                Err(e) => return Err(e.into()),
            }
        };
        if !should_update {
            return Ok(());
        }
        // 删除旧表、创建新表、更新 hash
        trace!(entry_count = serde_map.len(), "Writing items to table");
        let txn = self.db.begin_write()?;
        txn.delete_table(T::TABLE_DEF)?;
        let mut table = txn.open_table(T::TABLE_DEF)?;
        for (k, v) in &serde_map {
            table.insert(k.as_ref(), v.as_ref())?;
        }
        drop(table);
        let mut hash_table = txn.open_table(Self::HASH_METADATA_TABLE)?;
        hash_table.insert(T::TABLE_DEF.name(), hash)?;
        drop(hash_table);
        txn.commit()?;

        info!(table_name = %T::TABLE_DEF.name(), entry_count = serde_map.len(), "Table persisted successfully");
        Ok(())
    }

    /// Loads state from the store.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(T))` - State was successfully loaded
    /// * `Ok(None)` - Table doesn't exist (no data stored yet)
    /// * `Err(e)` - Deserialization or database error occurred
    ///
    /// # Type Parameters
    ///
    /// * `T` - Type implementing `PersistableState`
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails for any entry.
    /// Failed entries are logged and skipped, with partial data returned.
    #[instrument(skip(self), fields(table_name = %T::TABLE_DEF.name()))]
    pub fn load<T>(&self) -> Result<Option<T>, PersistError>
    where
        T: PersistableState,
        T::Item: TupleEntry,
    {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(T::TABLE_DEF) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => {
                debug!("Table does not exist, no data to load");
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };
        let iter = table.iter()?.filter_map(|res| {
            let (k_acc, v_acc) = match res {
                Ok(pair) => pair,
                Err(err) => {
                    warn!(error = %err, "Failed to iterate database entry");
                    return None;
                }
            };
            let k = match bitcode::deserialize(k_acc.value()) {
                Ok(k) => k,
                Err(err) => {
                    warn!(error = %err, "Failed to deserialize database key");
                    return None;
                }
            };
            let v = match bitcode::deserialize(v_acc.value()) {
                Ok(v) => v,
                Err(err) => {
                    warn!(error = %err, "Failed to deserialize database value");
                    return None;
                }
            };
            Some(TupleEntry::zip(k, v))
        });
        let state = iter.collect::<T>();
        info!("Table loaded successfully");
        Ok(Some(state))
    }
}

#[cfg(test)]
mod tests {
    use crate::{impl_persist, store::PersistStore};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use tempfile::tempdir;

    /// 测试用的持久化状态类型
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
    struct TestState {
        data: HashMap<String, u32>,
    }

    impl IntoIterator for TestState {
        type IntoIter = std::collections::hash_map::IntoIter<String, u32>;
        type Item = (String, u32);

        fn into_iter(self) -> Self::IntoIter { self.data.into_iter() }
    }

    impl FromIterator<(String, u32)> for TestState {
        fn from_iter<T: IntoIterator<Item = (String, u32)>>(iter: T) -> Self {
            let data: HashMap<String, u32> = iter.into_iter().collect();
            Self { data }
        }
    }

    impl_persist!(TestState, "test_state");

    /// 创建一个使用临时目录的 `PersistStore`
    fn create_test_store() -> (PersistStore, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test_db");
        let store = PersistStore::create(db_path).expect("Failed to create test store");
        (store, dir)
    }

    #[test]
    fn test_store_creation() {
        let (_store, _dir) = create_test_store();
        // 如果没有 panic，说明创建成功
    }

    #[test]
    fn test_refresh_empty_state() {
        let (store, _dir) = create_test_store();
        let empty_state = TestState::default();

        let result = store.refresh(empty_state);
        assert!(result.is_ok(), "Failed to refresh empty state");
    }

    #[test]
    fn test_refresh_single_entry() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();
        state.data.insert("key1".to_string(), 42);

        let result = store.refresh(state);
        assert!(result.is_ok(), "Failed to refresh state with single entry");
    }

    #[test]
    fn test_refresh_multiple_entries() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();
        state.data.insert("key1".to_string(), 1);
        state.data.insert("key2".to_string(), 2);
        state.data.insert("key3".to_string(), 3);

        let result = store.refresh(state);
        assert!(result.is_ok(), "Failed to refresh state with multiple entries");
    }

    #[test]
    fn test_refresh_replace_existing_data() {
        let (store, _dir) = create_test_store();

        // 第一次存储
        let mut state1 = TestState::default();
        state1.data.insert("key1".to_string(), 1);
        state1.data.insert("key2".to_string(), 2);
        store.refresh(state1).expect("First refresh failed");

        // 第二次存储（应该替换）
        let mut state2 = TestState::default();
        state2.data.insert("key3".to_string(), 3);
        store.refresh(state2).expect("Second refresh failed");

        // 加载并验证只有第二次的数据
        let loaded = store.load::<TestState>().expect("Load failed");
        assert!(loaded.is_some(), "Loaded state is None");
        let loaded_state = loaded.unwrap();
        assert_eq!(loaded_state.data.len(), 1, "Expected 1 entry after replacement");
        assert!(loaded_state.data.contains_key("key3"), "Expected key3 to exist");
        assert_eq!(*loaded_state.data.get("key3").unwrap(), 3, "Expected value 3 for key3");
    }

    #[test]
    fn test_load_nonexistent_table() {
        let (store, _dir) = create_test_store();

        // 尝试加载不存在的表
        let result = store.load::<TestState>();
        assert!(result.is_ok(), "Load should not error for nonexistent table");
        assert!(result.unwrap().is_none(), "Should return None for nonexistent table");
    }

    #[test]
    fn test_load_empty_table() {
        let (store, _dir) = create_test_store();
        let empty_state = TestState::default();

        // 存储空状态
        store.refresh(empty_state).expect("Failed to refresh empty state");

        // 加载
        let result = store.load::<TestState>();
        assert!(result.is_ok(), "Load failed for empty table");
        let loaded = result.unwrap();
        assert!(loaded.is_some(), "Expected Some for empty table");
        let loaded_state = loaded.unwrap();
        assert_eq!(loaded_state.data.len(), 0, "Expected empty data");
    }

    #[test]
    fn test_load_single_entry() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();
        state.data.insert("key1".to_string(), 42);

        store.refresh(state.clone()).expect("Failed to refresh state");

        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded, state, "Loaded state does not match original");
    }

    #[test]
    fn test_load_multiple_entries() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();
        state.data.insert("key1".to_string(), 100);
        state.data.insert("key2".to_string(), 200);
        state.data.insert("key3".to_string(), 300);

        store.refresh(state.clone()).expect("Failed to refresh state");

        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded, state, "Loaded state does not match original");
    }

    #[test]
    fn test_store_load_roundtrip() {
        let (store, _dir) = create_test_store();

        // 准备测试数据
        let mut original = TestState::default();
        original.data.insert("alpha".to_string(), 1);
        original.data.insert("beta".to_string(), 2);
        original.data.insert("gamma".to_string(), 3);
        original.data.insert("delta".to_string(), 4);

        // 存储
        store.refresh(original.clone()).expect("Failed to store state");

        // 加载
        let loaded = store.load::<TestState>().expect("Failed to load state").unwrap();

        // 验证
        assert_eq!(loaded, original, "Roundtrip state does not match original");
        assert_eq!(loaded.data.len(), 4, "Expected 4 entries");
        assert_eq!(*loaded.data.get("alpha").unwrap(), 1);
        assert_eq!(*loaded.data.get("beta").unwrap(), 2);
        assert_eq!(*loaded.data.get("gamma").unwrap(), 3);
        assert_eq!(*loaded.data.get("delta").unwrap(), 4);
    }

    #[test]
    fn test_multiple_updates() {
        let (store, _dir) = create_test_store();

        // 第一次更新
        let mut state1 = TestState::default();
        state1.data.insert("a".to_string(), 1);
        store.refresh(state1).expect("First update failed");

        // 第二次更新
        let mut state2 = TestState::default();
        state2.data.insert("b".to_string(), 2);
        state2.data.insert("c".to_string(), 3);
        store.refresh(state2).expect("Second update failed");

        // 第三次更新
        let mut state3 = TestState::default();
        state3.data.insert("x".to_string(), 10);
        state3.data.insert("y".to_string(), 20);
        state3.data.insert("z".to_string(), 30);
        store.refresh(state3).expect("Third update failed");

        // 验证最终状态是最后一次更新的数据
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 3, "Expected 3 entries after multiple updates");
        assert!(loaded.data.contains_key("x"));
        assert!(loaded.data.contains_key("y"));
        assert!(loaded.data.contains_key("z"));
        assert!(!loaded.data.contains_key("a"));
        assert!(!loaded.data.contains_key("b"));
    }

    #[test]
    fn test_special_characters_in_keys() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();
        state.data.insert("key-with-dash".to_string(), 1);
        state.data.insert("key_with_underscore".to_string(), 2);
        state.data.insert("key.with.dot".to_string(), 3);
        state.data.insert("key with spaces".to_string(), 4);
        state.data.insert("中文键".to_string(), 5);

        store.refresh(state.clone()).expect("Failed to refresh state with special chars");

        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded, state, "Failed to handle special characters in keys");
    }

    #[test]
    fn test_large_values() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();
        state.data.insert("large_value".to_string(), u32::MAX);
        state.data.insert("zero".to_string(), 0);

        store.refresh(state.clone()).expect("Failed to refresh state with large values");

        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded, state, "Failed to handle large values");
    }

    #[test]
    fn test_many_entries() {
        let (store, _dir) = create_test_store();
        let mut state = TestState::default();

        // 插入 1000 个条目
        for i in 0..1000 {
            state.data.insert(format!("key_{}", i), i);
        }

        store.refresh(state.clone()).expect("Failed to refresh state with many entries");

        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 1000, "Expected 1000 entries");

        // 验证一些随机条目
        assert_eq!(*loaded.data.get("key_0").unwrap(), 0);
        assert_eq!(*loaded.data.get("key_500").unwrap(), 500);
        assert_eq!(*loaded.data.get("key_999").unwrap(), 999);
    }

    #[test]
    fn test_same_content_not_duplicated() {
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_db");

        // 创建相同的测试数据
        let mut state = TestState::default();
        state.data.insert("key1".to_string(), 100);
        state.data.insert("key2".to_string(), 200);
        state.data.insert("key3".to_string(), 300);

        // 第一次写入
        let result1 = store.refresh(state.clone());
        assert!(result1.is_ok(), "First refresh failed");

        // 获取第一次写入后的数据库大小
        let size_after_first = std::fs::metadata(&db_path).unwrap().len();

        // 第二次写入相同内容（应该被 hash 机制跳过）
        let result2 = store.refresh(state.clone());
        assert!(result2.is_ok(), "Second refresh should succeed");

        // 获取第二次写入后的数据库大小
        let size_after_second = std::fs::metadata(&db_path).unwrap().len();

        // 数据库大小应该没有变化（或者只有非常小的metadata变化）
        assert_eq!(
            size_after_first, size_after_second,
            "Database size should not change when writing identical content. First: {} bytes, Second: {} bytes",
            size_after_first, size_after_second
        );

        // 验证数据没有被破坏
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 3, "Expected 3 entries");
        assert_eq!(loaded, state, "State should remain unchanged");
    }

    #[test]
    fn test_same_content_multiple_times() {
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_db");

        // 创建测试数据
        let mut state = TestState::default();
        state.data.insert("a".to_string(), 1);
        state.data.insert("b".to_string(), 2);

        // 第一次写入
        store.refresh(state.clone()).expect("First refresh failed");
        let initial_size = std::fs::metadata(&db_path).unwrap().len();

        // 连续写入 9 次相同内容（总共10次）
        for i in 1..10 {
            let result = store.refresh(state.clone());
            assert!(result.is_ok(), "Refresh {} should succeed", i);

            // 检查每次写入后大小是否变化
            let current_size = std::fs::metadata(&db_path).unwrap().len();
            assert_eq!(
                initial_size, current_size,
                "Database size should not change on write {}. Expected: {} bytes, Got: {} bytes",
                i, initial_size, current_size
            );
        }

        // 验证数据完整性
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 2, "Expected 2 entries");
        assert_eq!(loaded, state, "State should remain unchanged after multiple identical writes");
    }

    #[test]
    fn test_identical_different_instances() {
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_db");

        // 创建第一个状态实例
        let mut state1 = TestState::default();
        state1.data.insert("key1".to_string(), 42);

        // 创建第二个完全相同的状态实例（不同的对象，相同的内容）
        let mut state2 = TestState::default();
        state2.data.insert("key1".to_string(), 42);

        assert_ne!(std::ptr::addr_of!(state1), std::ptr::addr_of!(state2), "Should be different instances");

        // 写入第一个实例
        store.refresh(state1).expect("First refresh failed");
        let size_after_first = std::fs::metadata(&db_path).unwrap().len();

        // 写入第二个实例（内容相同）
        store.refresh(state2).expect("Second refresh should succeed");
        let size_after_second = std::fs::metadata(&db_path).unwrap().len();

        // 数据库大小应该没有变化
        assert_eq!(
            size_after_first, size_after_second,
            "Database size should not change when writing identical content from different instances"
        );

        // 验证数据正确
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 1, "Expected 1 entry");
        assert_eq!(*loaded.data.get("key1").unwrap(), 42, "Value should be 42");
    }

    #[test]
    fn test_empty_state_not_duplicated() {
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_db");

        let empty_state = TestState::default();

        // 第一次写入
        store.refresh(empty_state.clone()).expect("First refresh failed");
        let initial_size = std::fs::metadata(&db_path).unwrap().len();

        // 连续写入 4 次空状态（总共5次）
        for i in 1..5 {
            let result = store.refresh(empty_state.clone());
            assert!(result.is_ok(), "Refresh {} should succeed", i);

            // 检查大小是否变化
            let current_size = std::fs::metadata(&db_path).unwrap().len();
            assert_eq!(
                initial_size, current_size,
                "Database size should not change on empty write {}. Expected: {} bytes, Got: {} bytes",
                i, initial_size, current_size
            );
        }

        // 验证数据仍然是空的
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 0, "Expected empty data after multiple empty writes");
    }

    #[test]
    fn test_large_dataset_not_duplicated() {
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_db");

        // 创建大数据集
        let mut state = TestState::default();
        for i in 0..5000 {
            state.data.insert(format!("large_key_{}", i), i as u32);
        }

        // 第一次写入
        store.refresh(state.clone()).expect("First refresh failed");
        let size_after_first = std::fs::metadata(&db_path).unwrap().len();

        // 第二次写入相同大数据集
        store.refresh(state.clone()).expect("Second refresh should succeed");
        let size_after_second = std::fs::metadata(&db_path).unwrap().len();

        // 数据库大小应该没有变化（即使数据量很大）
        assert_eq!(
            size_after_first, size_after_second,
            "Database size should not change when writing identical large dataset. First: {} bytes, Second: {} bytes",
            size_after_first, size_after_second
        );

        // 验证数据完整性
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 5000, "Expected 5000 entries");
        assert_eq!(loaded, state, "Large dataset should remain unchanged");
    }

    #[test]
    fn test_same_after_change_and_revert() {
        let (store, dir) = create_test_store();
        let db_path = dir.path().join("test_db");

        // 初始状态
        let mut state1 = TestState::default();
        state1.data.insert("key1".to_string(), 1);
        store.refresh(state1.clone()).expect("First refresh failed");

        // 改变状态
        let mut state2 = TestState::default();
        state2.data.insert("key2".to_string(), 2);
        store.refresh(state2).expect("Second refresh failed");

        // 恢复到初始状态
        store.refresh(state1.clone()).expect("Third refresh failed");
        let size_after_third = std::fs::metadata(&db_path).unwrap().len();

        // 再次写入相同的初始状态（应该被跳过）
        store.refresh(state1.clone()).expect("Fourth refresh should succeed");
        let size_after_fourth = std::fs::metadata(&db_path).unwrap().len();

        // 验证第三次和第四次的大小相同（相同内容，连续写入）
        assert_eq!(
            size_after_third, size_after_fourth,
            "Database size should be the same after consecutive writes of same content. Third: {} bytes, Fourth: {} \
             bytes",
            size_after_third, size_after_fourth
        );

        // 验证最终是初始状态
        let loaded = store.load::<TestState>().expect("Load failed").unwrap();
        assert_eq!(loaded.data.len(), 1, "Expected 1 entry");
        assert!(loaded.data.contains_key("key1"), "Expected key1 to exist");
        assert!(!loaded.data.contains_key("key2"), "Expected key2 to not exist");
        assert_eq!(*loaded.data.get("key1").unwrap(), 1, "Value should be 1");
    }
}
