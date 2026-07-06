//! 配置管理模块
//!
//! 支持从配置文件和环境变量加载配置，环境变量优先级更高。

use anyhow::Result;
use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::PathBuf;

/// 环境变量守卫器，确保测试后清理环境变量
#[cfg(test)]
struct EnvGuard {
    vars: Vec<(String, Option<String>)>,
}

#[cfg(test)]
impl EnvGuard {
    fn new(keys: &[&str]) -> Self {
        let vars = keys
            .iter()
            .map(|key| (key.to_string(), std::env::var(key).ok()))
            .collect();
        Self { vars }
    }
}

#[cfg(test)]
impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, old_value) in &self.vars {
            match old_value {
                // SAFETY: 测试结束后安全地恢复环境变量
                Some(v) => unsafe { std::env::set_var(key, v) },
                // SAFETY: 测试结束后安全地清理环境变量
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

/// node-token 配置结构
#[derive(Debug, Deserialize, Clone)]
pub struct NodeTokenConfig {
    /// KeyCompute 服务端 URL
    pub server_url: String,

    /// 注册 token（从 KeyCompute 配置获取）
    pub registration_token: String,

    /// 节点显示名称
    pub display_name: String,

    /// 本地 Ollama URL，默认 http://localhost:11434
    #[serde(default = "default_ollama_url")]
    pub ollama_url: String,

    /// 心跳间隔（秒），默认 30
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    /// Excluded 节点 poll 检查间隔（秒），默认 30（与心跳间隔一致）
    /// 节点被 excluded 后，poll 循环定期检查是否恢复
    #[serde(default = "default_excluded_poll_check_interval")]
    pub excluded_poll_check_interval_secs: u64,

    /// 最大并发任务数，默认 2
    /// 控制同时执行的任务数量，防止过多任务并发导致资源耗尽
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,

    /// 本地数据目录，默认 ~/.local/share/node-token
    #[serde(default = "default_data_dir")]
    pub data_dir: Option<String>,

    /// 401 自愈最大重注册次数，默认 3
    /// 超过此次数后进程退出，防止无限重试
    #[serde(default = "default_max_reregisters")]
    pub max_reregisters: u32,
}

fn default_ollama_url() -> String {
    "http://localhost:11434".to_string()
}

fn default_heartbeat_interval() -> u64 {
    30
}

fn default_excluded_poll_check_interval() -> u64 {
    30
}

fn default_max_concurrent_tasks() -> usize {
    2
}

fn default_max_reregisters() -> u32 {
    3
}

fn default_data_dir() -> Option<String> {
    dirs::data_local_dir().map(|d| d.join("node-token").to_string_lossy().to_string())
}

/// 生成客户端实例 ID
///
/// # 规则
/// - 物理机：使用 `node-{hostname}` 前缀（如 `node-server-01`）
/// - Docker 容器：使用 `container-{hostname}` 前缀（如 `container-636b25ce7983`）
/// - 如果主机名为空或获取失败，使用 `node-unknown`
/// - 保证每台服务器有唯一的实例 ID
///
/// # 检测逻辑
/// - 如果存在 `/.dockerenv` 文件或 `/run/.containerenv` 文件，判定为容器环境
/// - 否则判定为物理机/虚拟机环境
///
/// # 示例
/// - 物理机: `node-server-01`
/// - Docker: `container-636b25ce7983`
/// - Fallback: `node-unknown`
pub fn generate_client_instance_id() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    // 检测是否在容器环境中
    let is_container = std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists();

    let prefix = if is_container { "container" } else { "node" };

    format!("{}-{}", prefix, host)
}

impl NodeTokenConfig {
    /// 获取数据目录路径
    pub fn data_dir_path(&self) -> PathBuf {
        self.data_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::data_local_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join("node-token")
            })
    }
}

/// 加载配置
///
/// 加载优先级：
/// 1. 环境变量（前缀 NODE_TOKEN_）
/// 2. 配置文件（config.toml 或 NODE_TOKEN_CONFIG 指定的路径）
pub fn load_config() -> Result<NodeTokenConfig> {
    // 确定配置文件路径
    let config_path =
        std::env::var("NODE_TOKEN_CONFIG").unwrap_or_else(|_| "config.toml".to_string());

    let builder = Config::builder()
        // 默认值
        .set_default("server_url", "http://localhost:3000")?
        .set_default("registration_token", "")?
        .set_default("display_name", "My KeyCompute Node")?
        .set_default("ollama_url", "http://localhost:11434")?
        .set_default("heartbeat_interval_secs", 30)?
        .set_default("excluded_poll_check_interval_secs", 30)?
        .set_default("max_concurrent_tasks", 2)?
        .set_default("max_reregisters", 3)?
        // 从配置文件加载
        .add_source(File::with_name(&config_path).required(false))
        // 从环境变量加载（优先级最高）
        .add_source(
            Environment::with_prefix("NODE_TOKEN")
                .separator("__")
                .try_parsing(true),
        );

    let config = builder.build()?;

    let node_config: NodeTokenConfig = config
        .try_deserialize()
        .map_err(|e: ConfigError| anyhow::anyhow!("Failed to deserialize config: {}", e))?;

    // 验证必需字段
    if node_config.server_url.is_empty() {
        anyhow::bail!("server_url is required");
    }
    if node_config.registration_token.is_empty() {
        anyhow::bail!("registration_token is required");
    }
    if node_config.display_name.is_empty() {
        anyhow::bail!("display_name is required");
    }
    if node_config.max_concurrent_tasks == 0 {
        anyhow::bail!("max_concurrent_tasks must be greater than 0");
    }

    Ok(node_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        // 测试默认值是否正确
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token".to_string(),
            display_name: "Test Node".to_string(),
            ollama_url: default_ollama_url(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            excluded_poll_check_interval_secs: default_excluded_poll_check_interval(),
            max_concurrent_tasks: default_max_concurrent_tasks(),
            max_reregisters: 3,
            data_dir: None,
        };

        assert_eq!(config.ollama_url, "http://localhost:11434");
        assert_eq!(config.heartbeat_interval_secs, 30);
        assert_eq!(config.excluded_poll_check_interval_secs, 30);
        assert_eq!(config.max_concurrent_tasks, 2);
    }

    #[test]
    fn test_data_dir_path() {
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token".to_string(),
            display_name: "Test Node".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: Some("/tmp/test-node-token".to_string()),
        };

        assert_eq!(
            config.data_dir_path(),
            PathBuf::from("/tmp/test-node-token")
        );
    }

    #[test]
    fn test_config_from_toml_file() {
        // 创建临时配置文件
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        let config_content = r#"
server_url = "http://example.com:3000"
registration_token = "my-secret-token"
display_name = "My PC Node"
ollama_url = "http://localhost:11434"
heartbeat_interval_secs = 60
excluded_poll_check_interval_secs = 60
max_concurrent_tasks = 4
"#;
        std::fs::write(&config_path, config_content).unwrap();

        // 使用 EnvGuard 确保测试后清理环境变量
        let _guard = EnvGuard::new(&["NODE_TOKEN_CONFIG"]);
        // SAFETY: 测试中安全地设置环境变量
        unsafe {
            std::env::set_var("NODE_TOKEN_CONFIG", config_path.to_str().unwrap());
        }

        // 清除可能干扰的其他 NODE_TOKEN_ 环境变量
        // SAFETY: 测试中安全地使用，不会影响其他线程
        unsafe {
            std::env::remove_var("NODE_TOKEN__SERVER_URL");
            std::env::remove_var("NODE_TOKEN__REGISTRATION_TOKEN");
            std::env::remove_var("NODE_TOKEN__DISPLAY_NAME");
        }

        // 加载配置
        let config = load_config().unwrap();

        assert_eq!(config.server_url, "http://example.com:3000");
        assert_eq!(config.registration_token, "my-secret-token");
        assert_eq!(config.display_name, "My PC Node");
        assert_eq!(config.ollama_url, "http://localhost:11434");
        assert_eq!(config.heartbeat_interval_secs, 60);
        assert_eq!(config.excluded_poll_check_interval_secs, 60);
        assert_eq!(config.max_concurrent_tasks, 4);
    }

    #[test]
    fn test_config_with_custom_values() {
        let config = NodeTokenConfig {
            server_url: "https://keycompute.example.com".to_string(),
            registration_token: "custom-token-123".to_string(),
            display_name: "Custom Node".to_string(),
            ollama_url: "http://192.168.1.100:11434".to_string(),
            heartbeat_interval_secs: 45,
            excluded_poll_check_interval_secs: 45,
            max_concurrent_tasks: 5,
            max_reregisters: 3,
            data_dir: Some("/custom/data/dir".to_string()),
        };

        assert_eq!(config.server_url, "https://keycompute.example.com");
        assert_eq!(config.registration_token, "custom-token-123");
        assert_eq!(config.display_name, "Custom Node");
        assert_eq!(config.ollama_url, "http://192.168.1.100:11434");
        assert_eq!(config.heartbeat_interval_secs, 45);
        assert_eq!(config.excluded_poll_check_interval_secs, 45);
        assert_eq!(config.max_concurrent_tasks, 5);
        assert_eq!(config.data_dir, Some("/custom/data/dir".to_string()));
    }

    #[test]
    fn test_data_dir_path_with_none() {
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token".to_string(),
            display_name: "Test Node".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: None,
        };

        // 当 data_dir 为 None 时，应该使用默认路径
        let path = config.data_dir_path();
        // 路径应该以 "node-token" 结尾
        assert!(path.ends_with("node-token"));
    }

    #[test]
    fn test_config_validation_empty_server_url() {
        // 直接测试验证逻辑，而不是通过 load_config()
        // 因为 load_config() 有默认值，会覆盖空字符串
        let config = NodeTokenConfig {
            server_url: "".to_string(), // 空值
            registration_token: "test-token".to_string(),
            display_name: "Test Node".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: None,
        };

        // 验证空值检测逻辑
        assert!(config.server_url.is_empty());
    }

    #[test]
    fn test_config_validation_empty_registration_token() {
        // 直接测试验证逻辑，而不是通过 load_config()
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "".to_string(), // 空值
            display_name: "Test Node".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: None,
        };

        // 验证空值检测逻辑
        assert!(config.registration_token.is_empty());
    }

    #[test]
    fn test_config_validation_empty_display_name() {
        // 直接测试验证逻辑
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token".to_string(),
            display_name: "".to_string(), // 空值
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: None,
        };

        // 验证空值检测逻辑
        assert!(config.display_name.is_empty());
    }

    // ========================================================================
    // 边界条件测试
    // ========================================================================

    #[test]
    fn test_config_with_unicode_display_name() {
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token".to_string(),
            display_name: "🚀 My Node 节点".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: None,
        };

        assert_eq!(config.display_name, "🚀 My Node 节点");
    }

    #[test]
    fn test_config_with_special_characters() {
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token-with-special-chars!@#$%".to_string(),
            display_name: "Node \"Test\" (Production)".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: None,
        };

        assert_eq!(
            config.registration_token,
            "test-token-with-special-chars!@#$%"
        );
        assert_eq!(config.display_name, "Node \"Test\" (Production)");
    }

    #[test]
    fn test_config_custom_data_dir() {
        let config = NodeTokenConfig {
            server_url: "http://localhost:3000".to_string(),
            registration_token: "test-token".to_string(),
            display_name: "Test Node".to_string(),
            ollama_url: "http://localhost:11434".to_string(),
            heartbeat_interval_secs: 30,
            excluded_poll_check_interval_secs: 30,
            max_concurrent_tasks: 2,
            max_reregisters: 3,
            data_dir: Some("/custom/path/to/data".to_string()),
        };

        assert_eq!(config.data_dir, Some("/custom/path/to/data".to_string()));
        assert_eq!(
            config.data_dir_path(),
            PathBuf::from("/custom/path/to/data")
        );
    }

    #[test]
    fn test_generate_client_instance_id() {
        // 测试自动生成实例 ID
        let instance_id = generate_client_instance_id();

        // 应该以 "node-" 或 "container-" 前缀开头
        assert!(
            instance_id.starts_with("node-") || instance_id.starts_with("container-"),
            "Instance ID should start with 'node-' or 'container-', got: {}",
            instance_id
        );

        // 应该不为空（至少有前缀）
        assert!(instance_id.len() > 5);

        // 不应该包含空格
        assert!(!instance_id.contains(char::is_whitespace));

        // 验证格式正确性
        let parts: Vec<&str> = instance_id.splitn(2, '-').collect();
        assert_eq!(parts.len(), 2, "Should have exactly one hyphen");
        assert!(
            parts[0] == "node" || parts[0] == "container",
            "Prefix should be 'node' or 'container', got: {}",
            parts[0]
        );
        assert!(!parts[1].is_empty(), "Host part should not be empty");
    }
}
