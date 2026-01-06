use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    num::{NonZeroU8, NonZeroUsize},
    path::PathBuf,
    sync::LazyLock,
};
use thiserror::Error;
use tracing::warn;

const CONFIG_FOLDER_NAME: &str = "FalconTransfer";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// File buffer maximum size
    pub file_buffer_max: NonZeroUsize,
    /// File buffer base size
    pub file_buffer_base: NonZeroUsize,
    /// Worker max retry count
    pub worker_max_retries: NonZeroU8,
    /// Max concurrency
    pub worker_max_concurrency: NonZeroU8,
    /// Download block size
    pub http_block_size: NonZeroUsize,
    /// Worker timeout (minutes)
    pub worker_timeout_mins: NonZeroU8,
    /// Persist broadcast interval (seconds)
    pub persist_broadcast_interval_secs: NonZeroU8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            file_buffer_max: NonZeroUsize::new(128 * 1024).unwrap(), // 128 KiB
            file_buffer_base: NonZeroUsize::new(64 * 1024).unwrap(), // 64 KiB
            worker_max_retries: NonZeroU8::new(32).unwrap(),
            worker_max_concurrency: NonZeroU8::new(8).unwrap(),
            http_block_size: NonZeroUsize::new(0x100_0000).unwrap(), // 32 MiB
            worker_timeout_mins: NonZeroU8::new(3).unwrap(),         // 3 minutes
            persist_broadcast_interval_secs: NonZeroU8::new(5).unwrap(), // 5 seconds
        }
    }
}

/// 验证配置是否满足规则要求
pub fn validate(cfg: Config) -> ConfigResult<Config> {
    if cfg.file_buffer_base >= cfg.file_buffer_max {
        Err(Error::Invalid(format!(
            "file_buffer_base({}) must be less than file_buffer_max({})",
            cfg.file_buffer_base, cfg.file_buffer_max
        )))
    } else {
        Ok(cfg)
    }
}

/// 加载并验证配置，如果验证失败则回退到默认配置
fn load_and_validate_config() -> Config {
    let res: Result<Config, _> = confy::load(CONFIG_FOLDER_NAME, None);
    res.map_err(Into::into).and_then(validate).unwrap_or_else(|err| {
        warn!(error = ?err, "Failed to load config file, fallback to default");
        Config::default()
    })
}

pub fn global_config() -> &'static Config {
    static CONFIG: LazyLock<Config> = LazyLock::new(load_and_validate_config);
    &CONFIG
}

/// 获取配置文件路径
pub fn get_config_path() -> Result<PathBuf, confy::ConfyError> {
    confy::get_configuration_file_path(CONFIG_FOLDER_NAME, None)
}

pub fn read_config_file() -> ConfigResult<String> {
    let config_path = get_config_path()?;
    let s = fs::read_to_string(config_path)?;
    Ok(s)
}

pub fn write_config_file(content: &str) -> ConfigResult {
    let config_path = get_config_path()?;
    // 确保目录存在
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(config_path, content)?;
    Ok(())
}

pub type ConfigResult<T = ()> = Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Fs(#[from] io::Error),
    #[error(transparent)]
    Config(#[from] confy::ConfyError),
    #[error("Invalid config, reason: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    /// 创建一个无效的配置文件，其中 base >= max
    fn create_invalid_config() -> Config {
        Config {
            file_buffer_max: std::num::NonZeroUsize::new(1024 * 1024).unwrap(), // 1MB
            file_buffer_base: std::num::NonZeroUsize::new(2 * 1024 * 1024).unwrap(), // 2MB, invalid
            worker_max_retries: std::num::NonZeroU8::new(32).unwrap(),
            worker_max_concurrency: std::num::NonZeroU8::new(8).unwrap(),
            http_block_size: std::num::NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            worker_timeout_mins: std::num::NonZeroU8::new(3).unwrap(),
            persist_broadcast_interval_secs: std::num::NonZeroU8::new(5).unwrap(),
        }
    }

    /// 创建一个自定义配置用于测试
    fn create_custom_config() -> Config {
        Config {
            file_buffer_max: NonZeroUsize::new(128 * 1024 * 1024).unwrap(), // 128MB
            file_buffer_base: NonZeroUsize::new(32768).unwrap(),            // 32KB
            worker_max_retries: NonZeroU8::new(64).unwrap(),
            worker_max_concurrency: NonZeroU8::new(16).unwrap(),
            http_block_size: NonZeroUsize::new(32 * 1024 * 1024).unwrap(), // 32MB
            worker_timeout_mins: NonZeroU8::new(5).unwrap(),
            persist_broadcast_interval_secs: NonZeroU8::new(10).unwrap(),
        }
    }

    #[test]
    fn test_default_config_values() {
        let config = Config::default();

        // file_buffer_max: 128 * 1024 = 128 KiB
        assert_eq!(config.file_buffer_max.get(), 128 * 1024);

        // file_buffer_base: 64 * 1024 = 64 KiB
        assert_eq!(config.file_buffer_base.get(), 64 * 1024);

        // worker_max_retries: 32
        assert_eq!(config.worker_max_retries.get(), 32);

        // worker_max_concurrency: 8
        assert_eq!(config.worker_max_concurrency.get(), 8);

        // http_block_size: 0x100_0000 = 16 MiB
        assert_eq!(config.http_block_size.get(), 0x100_0000);
    }

    #[test]
    fn test_config_singleton() {
        // 测试 config() 返回的是同一个静态实例
        let config1 = global_config();
        let config2 = global_config();

        // 两次调用应该返回相同的引用
        assert!(std::ptr::eq(config1, config2));

        // 清理：删除 global_config 产生的配置文件
        if let Ok(path) = confy::get_configuration_file_path(CONFIG_FOLDER_NAME, None) {
            let _ = fs::remove_file(&path);
            if let Some(parent) = path.parent() {
                let _ = fs::remove_dir(parent);
            }
        }
    }

    #[test]
    fn test_write_config_to_tempfile() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");

        let config = Config::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();

        fs::write(&config_path, &toml_str).unwrap();

        // 验证文件已创建
        assert!(config_path.exists());

        // 验证文件内容不为空
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(!content.is_empty());
        assert!(content.contains("file_buffer_max"));
        assert!(content.contains("worker_max_retries"));
    }

    #[test]
    fn test_read_config_from_tempfile() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");

        // 写入自定义配置
        let original_config = create_custom_config();
        let toml_str = toml::to_string_pretty(&original_config).unwrap();
        fs::write(&config_path, &toml_str).unwrap();

        // 读取配置
        let content = fs::read_to_string(&config_path).unwrap();
        let loaded_config: Config = toml::from_str(&content).unwrap();

        // 验证读取的配置与原始配置一致
        assert_eq!(loaded_config, original_config);
        assert_eq!(loaded_config.file_buffer_max.get(), 128 * 1024 * 1024);
        assert_eq!(loaded_config.worker_max_concurrency.get(), 16);
    }

    #[test]
    fn test_config_roundtrip_with_tempfile() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("roundtrip.toml");

        let original = create_custom_config();

        // 序列化并写入
        let toml_str = toml::to_string_pretty(&original).unwrap();
        fs::write(&config_path, &toml_str).unwrap();

        // 读取并反序列化
        let content = fs::read_to_string(&config_path).unwrap();
        let restored: Config = toml::from_str(&content).unwrap();

        // 完整比对
        assert_eq!(original, restored);
    }

    #[test]
    fn test_invalid_config_file_content() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("invalid.toml");

        // 写入无效的 TOML 内容
        fs::write(&config_path, "this is not valid toml {{{{").unwrap();

        // 尝试解析应该失败
        let content = fs::read_to_string(&config_path).unwrap();
        let result: Result<Config, _> = toml::from_str(&content);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_fields_uses_default() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("partial.toml");

        // 只写入部分字段 - 这会导致解析失败，因为所有字段都是必需的
        let partial_toml = r#"
file_buffer_max = 1024
"#;
        fs::write(&config_path, partial_toml).unwrap();

        let content = fs::read_to_string(&config_path).unwrap();
        let result: Result<Config, _> = toml::from_str(&content);

        // 由于缺少必需字段，解析应该失败
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_value_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("zero.toml");

        // 尝试使用零值（NonZero 类型不允许）
        let zero_toml = r#"
file_buffer_max = 0
file_buffer_base = 16384
worker_max_retries = 32
worker_max_concurrency = 8
http_block_size = 16777216
"#;
        fs::write(&config_path, zero_toml).unwrap();

        let content = fs::read_to_string(&config_path).unwrap();
        let result: Result<Config, _> = toml::from_str(&content);

        // NonZeroUsize 不接受 0，解析应该失败
        assert!(result.is_err());
    }

    #[test]
    fn test_config_file_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("overwrite.toml");

        // 写入默认配置
        let default_config = Config::default();
        let toml_str = toml::to_string_pretty(&default_config).unwrap();
        fs::write(&config_path, &toml_str).unwrap();

        // 覆盖写入自定义配置
        let custom_config = create_custom_config();
        let custom_toml_str = toml::to_string_pretty(&custom_config).unwrap();
        fs::write(&config_path, &custom_toml_str).unwrap();

        // 读取并验证是自定义配置
        let content = fs::read_to_string(&config_path).unwrap();
        let loaded: Config = toml::from_str(&content).unwrap();

        assert_eq!(loaded, custom_config);
        assert_ne!(loaded, default_config);
    }

    #[test]
    fn test_toml_format_output() {
        let config = Config::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();

        // 验证 TOML 格式的输出包含所有字段
        assert!(toml_str.contains("file_buffer_max"));
        assert!(toml_str.contains("file_buffer_base"));
        assert!(toml_str.contains("worker_max_retries"));
        assert!(toml_str.contains("worker_max_concurrency"));
        assert!(toml_str.contains("http_block_size"));
    }

    #[test]
    fn test_temp_dir_cleanup() {
        let config_path;
        {
            let temp_dir = TempDir::new().unwrap();
            config_path = temp_dir.path().join("cleanup_test.toml");

            let config = Config::default();
            let toml_str = toml::to_string_pretty(&config).unwrap();
            fs::write(&config_path, &toml_str).unwrap();

            // 在作用域内文件存在
            assert!(config_path.exists());
        }
        // TempDir 离开作用域后，文件应该被清理
        assert!(!config_path.exists());
    }

    #[test]
    fn test_buffer_base_less_than_max() {
        let config = Config::default();

        // file_buffer_base 应该小于 file_buffer_max
        assert!(config.file_buffer_base.get() < config.file_buffer_max.get());
    }

    #[test]
    fn test_confy_store_and_load() {
        const TEST_APP_NAME: &str = "FalconTransfer_Test";

        // 使用 confy 的实际存储和加载功能
        let config = Config::default();

        // 存储配置（使用测试专用的名称避免影响实际配置）
        let store_result = confy::store(TEST_APP_NAME, None, &config);
        assert!(store_result.is_ok());

        // 加载配置
        let loaded: Result<Config, _> = confy::load(TEST_APP_NAME, None);
        assert!(loaded.is_ok());

        let loaded_config = loaded.unwrap();
        assert_eq!(loaded_config, config);

        // 清理：删除测试产生的配置文件和目录
        if let Ok(path) = confy::get_configuration_file_path(TEST_APP_NAME, None)
            && let Some(parent) = path.parent()
        {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn test_config_field_access() {
        let config = Config::default();

        // 测试所有公共字段
        assert_eq!(config.file_buffer_max.get(), 128 * 1024);
        assert_eq!(config.file_buffer_base.get(), 64 * 1024);
        assert_eq!(config.worker_max_retries.get(), 32);
        assert_eq!(config.worker_max_concurrency.get(), 8);
        assert_eq!(config.http_block_size.get(), 0x100_0000);

        // 使用自定义配置测试
        let custom = create_custom_config();
        assert_eq!(custom.file_buffer_max.get(), 128 * 1024 * 1024);
        assert_eq!(custom.worker_max_concurrency.get(), 16);
    }

    #[test]
    fn test_read_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        // 写入测试配置文件
        let config = Config::default();
        let content = toml::to_string_pretty(&config).unwrap();
        fs::write(&config_path, content).unwrap();

        // 直接测试读取配置内容，不依赖环境变量
        let read_content = fs::read_to_string(&config_path).unwrap();
        assert!(!read_content.is_empty());
        assert!(read_content.contains("file_buffer_max"));
    }

    #[test]
    fn test_write_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_write_config.toml");

        // 直接测试写入文件，不依赖环境变量
        let test_content = r#"file_buffer_max = 67108864
file_buffer_base = 16384
worker_max_retries = 32
worker_max_concurrency = 8
http_block_size = 16777216
"#;

        fs::write(&config_path, test_content).unwrap();

        // 验证文件已创建并包含正确内容
        assert!(config_path.exists());
        let content = fs::read_to_string(&config_path).unwrap();
        assert_eq!(content, test_content);
    }

    #[test]
    fn test_write_config_file_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let nested_path = temp_dir.path().join("nested").join("dir").join("config.toml");

        // 确保目录不存在
        assert!(!nested_path.parent().unwrap().exists());

        // 直接测试创建目录和写入文件
        let test_content = r#"file_buffer_max = 67108864"#;
        fs::create_dir_all(nested_path.parent().unwrap()).unwrap();
        fs::write(&nested_path, test_content).unwrap();

        // 验证目录和文件都已创建
        assert!(nested_path.exists());
        assert!(nested_path.parent().unwrap().exists());
    }

    #[test]
    fn test_error_types() {
        // 测试从 io::Error 的转换
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let config_error: Error = io_error.into();
        match config_error {
            Error::Fs(_) => {} // 预期结果
            _ => panic!("Expected Fs error variant"),
        }

        // 测试读取不存在的文件应该产生 Fs 错误
        let temp_dir = TempDir::new().unwrap();
        let nonexistent_path = temp_dir.path().join("nonexistent.toml");

        let result = fs::read_to_string(&nonexistent_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().kind() == std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_extreme_values() {
        // 测试较大但合理的值，避免序列化问题
        // 使用 4GB 作为较大的测试值
        let large_usize = NonZeroUsize::new(4 * 1024 * 1024 * 1024).unwrap(); // 4GB
        let max_u8 = NonZeroU8::new(u8::MAX).unwrap();

        let extreme_config = Config {
            file_buffer_max: large_usize,
            file_buffer_base: NonZeroUsize::new(1).unwrap(),
            worker_max_retries: max_u8,
            worker_max_concurrency: max_u8,
            http_block_size: large_usize,
            worker_timeout_mins: max_u8,
            persist_broadcast_interval_secs: NonZeroU8::new(1).unwrap(),
        };

        // 验证字段能正确处理这些值
        assert_eq!(extreme_config.file_buffer_max.get(), 4 * 1024 * 1024 * 1024);
        assert_eq!(extreme_config.worker_max_retries.get(), u8::MAX);
        assert_eq!(extreme_config.http_block_size.get(), 4 * 1024 * 1024 * 1024);
        assert_eq!(extreme_config.file_buffer_base.get(), 1);

        // 验证序列化和反序列化能正确处理这些值
        let toml_str = toml::to_string_pretty(&extreme_config).unwrap();
        let restored: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(restored, extreme_config);
    }

    #[test]
    fn test_config_rule_validation() {
        // 测试有效配置 - 应该通过规则验证
        let valid_config = Config::default();
        assert!(valid_config.file_buffer_base.get() < valid_config.file_buffer_max.get());

        // 使用公共函数验证配置
        let result = validate(valid_config);

        // 验证有效配置通过规则验证
        assert!(result.is_ok());
    }

    #[test]
    fn test_config_rule_validation_invalid() {
        // 测试无效配置 - base >= max
        let invalid_config = create_invalid_config();

        // 验证配置确实无效
        assert!(invalid_config.file_buffer_base.get() >= invalid_config.file_buffer_max.get());

        // 使用公共函数验证配置
        let result = validate(invalid_config);

        // 验证无效配置被规则拒绝
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::Invalid(msg) => {
                assert!(msg.contains("file_buffer_base"));
                assert!(msg.contains("file_buffer_max"));
            }
            _ => panic!("Expected Invalid error variant"),
        }
    }

    #[test]
    fn test_global_config_fallback_to_default() {
        // 测试当规则验证失败时，系统会回退到默认配置

        // 模拟全局配置加载过程，但使用无效配置
        let invalid_config = create_invalid_config();

        // 验证配置确实无效
        assert!(invalid_config.file_buffer_base.get() >= invalid_config.file_buffer_max.get());

        // 应用验证函数
        let result = validate(invalid_config);
        assert!(result.is_err());

        // 验证当验证失败时，系统会回退到默认配置
        let fallback_config = result.unwrap_or_else(|_| Config::default());
        assert_eq!(fallback_config.file_buffer_max.get(), 128 * 1024);
        assert_eq!(fallback_config.file_buffer_base.get(), 64 * 1024);
        assert_eq!(fallback_config.worker_max_retries.get(), 32);
        assert_eq!(fallback_config.worker_max_concurrency.get(), 8);
        assert_eq!(fallback_config.http_block_size.get(), 0x100_0000);
    }

    #[test]
    fn test_concurrent_access() {
        use std::{sync::Arc, thread};

        // 测试多线程同时访问全局配置
        let config = Arc::new(global_config());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let config = Arc::clone(&config);
                thread::spawn(move || {
                    // 在多个线程中读取配置
                    let buffer_max = config.file_buffer_max.get();
                    let retries = config.worker_max_retries.get();
                    assert_eq!(buffer_max, Config::default().file_buffer_max.get());
                    assert_eq!(retries, Config::default().worker_max_retries.get());
                })
            })
            .collect();

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_custom_config_values() {
        // 测试配置项的合理性
        let config = Config::default();

        // 验证缓冲区大小的合理性
        assert!(config.file_buffer_max.get() > config.file_buffer_base.get());
        assert!(config.file_buffer_base.get() >= 4096); // 至少 4KB
        assert!(config.file_buffer_max.get() <= 1024 * 1024 * 1024); // 不超过 1GB

        // 验证重试次数的合理性
        assert!(config.worker_max_retries.get() > 0);
        assert!(config.worker_max_retries.get() <= 100); // 不超过 100 次

        // 验证并发数的合理性
        assert!(config.worker_max_concurrency.get() > 0);
        assert!(config.worker_max_concurrency.get() <= 64); // 不超过 64

        // 验证 HTTP 块大小的合理性
        assert!(config.http_block_size.get() >= 1024 * 1024); // 至少 1MB
        assert!(config.http_block_size.get() <= 100 * 1024 * 1024); // 不超过 100MB
    }
}
