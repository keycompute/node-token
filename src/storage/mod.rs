//! 本地持久化模块
//!
//! 负责本地 session 信息的持久化，支持重启后恢复。
//! 存储在 `~/.local/share/node-token/session.json`（Linux）或配置指定的目录。

use crate::error::{NodeTokenError, StorageResult};
use crate::protocol::types::{NodeCapabilities, NodeId, NodeSessionId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(not(unix))]
use tracing::warn;
use tracing::{debug, info};

/// Session 数据（本地持久化）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionData {
    /// 节点 ID
    pub node_id: NodeId,
    /// 会话 ID
    pub session_id: NodeSessionId,
    /// 会话 Token（敏感信息，不得输出到日志）
    pub session_token: String,
    /// 节点能力（注册时上报的模型列表）
    pub capabilities: NodeCapabilities,
    /// 服务端轮询超时（秒），用于计算无任务时的等待间隔
    pub poll_timeout_secs: u64,
}

/// 本地存储管理器
pub struct LocalStorage {
    /// 数据目录
    data_dir: PathBuf,
    /// Session 文件路径
    session_file: PathBuf,
}

impl LocalStorage {
    /// 创建新的本地存储管理器
    ///
    /// # Arguments
    /// * `data_dir` - 数据目录路径，如果为 None 则使用默认目录
    pub fn new(data_dir: Option<&str>) -> StorageResult<Self> {
        let data_dir = match data_dir {
            Some(dir) => PathBuf::from(dir),
            None => Self::default_data_dir()?,
        };

        let session_file = data_dir.join("session.json");

        // 确保数据目录存在
        if !data_dir.exists() {
            fs::create_dir_all(&data_dir).map_err(|e| {
                NodeTokenError::Storage(format!("Failed to create data directory: {}", e))
            })?;
            info!("Created data directory: {:?}", data_dir);
        }

        debug!(
            "LocalStorage initialized: data_dir={:?}, session_file={:?}",
            data_dir, session_file
        );

        Ok(Self {
            data_dir,
            session_file,
        })
    }

    /// 获取默认数据目录
    /// Linux: ~/.local/share/node-token
    /// macOS: ~/Library/Application Support/node-token
    /// Windows: %APPDATA%\node-token
    fn default_data_dir() -> StorageResult<PathBuf> {
        let data_dir = dirs::data_dir()
            .ok_or_else(|| {
                NodeTokenError::Storage("Failed to get system data directory".to_string())
            })?
            .join("node-token");

        Ok(data_dir)
    }

    /// 保存 session 到文件
    ///
    /// # Security
    /// - 文件权限设置为 600（仅 owner 可读写）
    /// - 日志中不得输出 session_token 明文
    /// - 使用原子写入（先写临时文件再 rename），防止崩溃导致数据损坏
    pub fn save_session(&self, session: &SessionData) -> StorageResult<()> {
        debug!(
            "Saving session to file: node_id={}, session_id={}",
            session.node_id, session.session_id
        );

        // 序列化为 JSON
        let json = serde_json::to_string_pretty(session).map_err(|e| {
            NodeTokenError::Storage(format!("Failed to serialize session data: {}", e))
        })?;

        // 原子写入：先写临时文件，再 rename
        // 防止进程崩溃时留下损坏的 session.json
        let tmp_file = self.session_file.with_extension("json.tmp");
        fs::write(&tmp_file, &json).map_err(|e| {
            NodeTokenError::Storage(format!("Failed to write session temp file: {}", e))
        })?;

        // 设置文件权限为 600（仅 owner 可读写）
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&tmp_file)
                .map_err(|e| {
                    NodeTokenError::Storage(format!("Failed to read temp file metadata: {}", e))
                })?
                .permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&tmp_file, perms).map_err(|e| {
                NodeTokenError::Storage(format!("Failed to set temp file permissions: {}", e))
            })?;
        }

        #[cfg(not(unix))]
        {
            warn!("File permissions setting is not supported on this platform");
        }

        // 原子 rename（POSIX 保证同一文件系统内 rename 是原子的）
        fs::rename(&tmp_file, &self.session_file).map_err(|e| {
            NodeTokenError::Storage(format!("Failed to rename session file: {}", e))
        })?;

        info!("Session saved successfully to {:?}", self.session_file);
        Ok(())
    }

    /// 加载 session
    ///
    /// 如果文件不存在或解析失败，返回 None
    pub fn load_session(&self) -> StorageResult<Option<SessionData>> {
        if !self.session_file.exists() {
            debug!("Session file does not exist: {:?}", self.session_file);
            return Ok(None);
        }

        // 读取文件
        let json = fs::read_to_string(&self.session_file)
            .map_err(|e| NodeTokenError::Storage(format!("Failed to read session file: {}", e)))?;

        // 反序列化
        let session: SessionData = serde_json::from_str(&json).map_err(|e| {
            NodeTokenError::Storage(format!("Failed to deserialize session data: {}", e))
        })?;

        debug!(
            "Session loaded from file: node_id={}, session_id={}",
            session.node_id, session.session_id
        );
        Ok(Some(session))
    }

    /// 清除 session
    ///
    /// 删除 session 文件，如果文件不存在则忽略
    pub fn clear_session(&self) -> StorageResult<()> {
        if self.session_file.exists() {
            fs::remove_file(&self.session_file).map_err(|e| {
                NodeTokenError::Storage(format!("Failed to remove session file: {}", e))
            })?;
            info!("Session cleared from {:?}", self.session_file);
        } else {
            debug!("Session file does not exist, nothing to clear");
        }
        Ok(())
    }

    /// 获取数据目录路径
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// 获取 session 文件路径
    pub fn session_file(&self) -> &Path {
        &self.session_file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::NodeModelCapability;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_session() -> SessionData {
        SessionData {
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            session_token: "test-session-token-secret".to_string(),
            capabilities: NodeCapabilities {
                runtime: "ollama".to_string(),
                models: vec![
                    NodeModelCapability {
                        model: "deepseek-chat".to_string(),
                    },
                    NodeModelCapability {
                        model: "llama3".to_string(),
                    },
                ],
            },
            poll_timeout_secs: 30,
        }
    }

    #[test]
    fn test_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

        assert!(storage.data_dir().exists());
        assert_eq!(storage.session_file(), temp_dir.path().join("session.json"));
    }

    #[test]
    fn test_storage_default_dir() {
        // 测试默认目录（应该不会失败）
        let result = LocalStorage::default_data_dir();
        assert!(result.is_ok());

        let default_dir = result.unwrap();
        assert!(default_dir.ends_with("node-token"));
    }

    #[test]
    fn test_save_and_load_session() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

        let session = create_test_session();

        // 保存 session
        storage.save_session(&session).unwrap();

        // 验证文件存在
        assert!(storage.session_file().exists());

        // 加载 session
        let loaded = storage.load_session().unwrap();
        assert!(loaded.is_some());

        let loaded = loaded.unwrap();
        assert_eq!(loaded.node_id, session.node_id);
        assert_eq!(loaded.session_id, session.session_id);
        assert_eq!(loaded.session_token, session.session_token);
        assert_eq!(loaded.capabilities.runtime, session.capabilities.runtime);
        assert_eq!(
            loaded.capabilities.models.len(),
            session.capabilities.models.len()
        );
    }

    #[test]
    fn test_load_nonexistent_session() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

        // 文件不存在时应返回 None
        let loaded = storage.load_session().unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_clear_session() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

        let session = create_test_session();

        // 保存 session
        storage.save_session(&session).unwrap();
        assert!(storage.session_file().exists());

        // 清除 session
        storage.clear_session().unwrap();
        assert!(!storage.session_file().exists());

        // 再次清除（应该不报错）
        storage.clear_session().unwrap();
    }

    #[test]
    fn test_session_file_permissions() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let temp_dir = TempDir::new().unwrap();
            let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

            let session = create_test_session();
            storage.save_session(&session).unwrap();

            // 验证文件权限
            let metadata = fs::metadata(storage.session_file()).unwrap();
            let perms = metadata.permissions();
            let mode = perms.mode() & 0o777; // 只取权限位

            assert_eq!(mode, 0o600, "File permissions should be 600");
        }
    }

    #[test]
    fn test_corrupted_session_file() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

        // 写入损坏的 JSON
        fs::write(storage.session_file(), "not valid json").unwrap();

        // 加载应该失败
        let result = storage.load_session();
        assert!(result.is_err());
    }

    #[test]
    fn test_session_data_serialization() {
        let session = create_test_session();

        // 序列化
        let json = serde_json::to_string(&session).unwrap();

        // 反序列化
        let deserialized: SessionData = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.node_id, session.node_id);
        assert_eq!(deserialized.session_id, session.session_id);
        assert_eq!(deserialized.session_token, session.session_token);
    }
}
