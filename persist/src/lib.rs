use std::sync::LazyLock;

use falcon_config::{Config, get_config_path};
use redb::Database;

pub static DB: LazyLock<Database> = LazyLock::new(|| {
    Database::create(
        get_config_path()
            .ok()
            .as_ref()
            .and_then(|p| p.parent())
            .expect("Failed to locate config directory")
            .join("tasks.db"),
    )
    .unwrap()
});

#[cfg(test)]
mod tests {
    use redb::ReadableDatabase;

    use super::*;
    #[test]
    fn test_db_creation() {
        println!("{:?}", DB.cache_stats());
    }
}
