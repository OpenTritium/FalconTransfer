use redb::TableDefinition;
use serde::{Serialize, de::DeserializeOwned};

/// Trait for types that can be used as key-value entries in persistent storage.
///
/// This trait abstracts over tuple types `(K, V)` where both key and value
/// are serializable and deserializable, allowing them to be stored in the database.
pub trait TupleEntry {
    /// The key type, must be serializable and deserializable.
    type K: Serialize + DeserializeOwned;
    /// The value type, must be serializable and deserializable.
    type V: Serialize + DeserializeOwned;

    /// Splits the entry into its key and value components.
    fn unzip(self) -> (Self::K, Self::V);

    /// Combines a key and value into an entry.
    fn zip(k: Self::K, v: Self::V) -> Self;
}

impl<K, V> TupleEntry for (K, V)
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type K = K;
    type V = V;

    #[inline]
    fn unzip(self) -> (K, V) { self }

    #[inline]
    fn zip(k: K, v: V) -> Self { (k, v) }
}

/// Trait for state types that can be persisted to and loaded from storage.
///
/// This trait should be implemented for complex state types that can be
/// represented as a collection of key-value pairs.
///
/// # Requirements
///
/// - The type must be convertible to/from an iterator of `(K, V)` pairs
/// - Each pair must implement `TupleEntry`
/// - A table definition must be provided for database storage
///
/// # Examples
///
/// ```ignore
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct MyState {
///     data: HashMap<String, u32>,
/// }
///
/// impl IntoIterator for MyState {
///     type Item = (String, u32);
///     type IntoIter = std::collections::hash_map::IntoIter<String, u32>;
///
///     fn into_iter(self) -> Self::IntoIter {
///         self.data.into_iter()
///     }
/// }
///
/// impl FromIterator<(String, u32)> for MyState {
///     fn from_iter<T: IntoIterator<Item = (String, u32)>>(iter: T) -> Self {
///         Self { data: iter.into_iter().collect() }
///     }
/// }
///
/// impl_persist!(MyState, "my_state_table");
/// ```
pub trait PersistableState: IntoIterator + FromIterator<Self::Item>
where
    Self::Item: TupleEntry,
{
    /// The table definition for this state type in the database.
    const TABLE_DEF: TableDefinition<'static, &'static [u8], &'static [u8]>;
}

/// Macro to implement `PersistableState` for a given type.
///
/// # Arguments
///
/// * `$struct_type` - The type to implement the trait for
/// * `$table_name` - The name of the table in the database
///
/// # Examples
///
/// ```ignore
/// struct MyState { /* ... */ }
/// impl_persist!(MyState, "my_state_table");
/// ```
#[macro_export]
macro_rules! impl_persist {
    ($struct_type:ty, $table_name:expr) => {
        impl $crate::registry::PersistableState for $struct_type {
            const TABLE_DEF: redb::TableDefinition<'static, &'static [u8], &'static [u8]> =
                redb::TableDefinition::new($table_name);
        }
    };
}
