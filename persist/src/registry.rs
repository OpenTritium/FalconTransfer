use redb::TableDefinition;
use serde::{Serialize, de::DeserializeOwned};

/// Trait for types that can be used as key-value entries in persistent storage.
///
/// Abstracts over tuple types `(K, V)` where both key and value are serializable.
pub trait TupleEntry {
    /// The key type, must be serializable and deserializable.
    type K: Serialize + DeserializeOwned;
    /// The value type, must be serializable and deserializable.
    type V: Serialize + DeserializeOwned;

    /// Splits the entry into key and value components.
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
/// Should be implemented for state types that can be represented as a collection of key-value pairs.
pub trait PersistableState: IntoIterator + FromIterator<Self::Item>
where
    Self::Item: TupleEntry,
{
    /// The table definition for this state type in the database.
    const TABLE_DEF: TableDefinition<'static, &'static [u8], &'static [u8]>;
}

/// Macro to implement `PersistableState` for a given type.
#[macro_export]
macro_rules! impl_persist {
    ($struct_type:ty, $table_name:expr) => {
        impl $crate::registry::PersistableState for $struct_type {
            const TABLE_DEF: redb::TableDefinition<'static, &'static [u8], &'static [u8]> =
                redb::TableDefinition::new($table_name);
        }
    };
}
