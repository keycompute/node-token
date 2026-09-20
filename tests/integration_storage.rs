//! 集成测试 - 本地持久化
//!
//! 验证 session 数据的保存、加载和清除功能。

use node_token::protocol::types::{NodeCapabilities, NodeModelCapability};
use node_token::storage::LocalStorage;
use node_token::storage::SessionData;
use std::str::FromStr;
use tempfile::TempDir;
use uuid::Uuid;

#[test]
/// 测试 session 完整生命周期
fn test_session_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

    // 初始状态应该没有 session
    assert!(storage.load_session().unwrap().is_none());

    // 创建并保存 session
    let session = SessionData {
        node_id: Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap(),
        session_id: Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap(),
        session_token: "test-session-token".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            models: vec![
                NodeModelCapability {
                    model: "deepseek-chat:latest".to_string(),
                },
                NodeModelCapability {
                    model: "llama3:latest".to_string(),
                },
            ],
        },
        poll_timeout_secs: 10,
    };

    storage.save_session(&session).unwrap();

    // 加载 session 并验证
    let loaded = storage.load_session().unwrap().unwrap();
    assert_eq!(loaded.node_id, session.node_id);
    assert_eq!(loaded.session_id, session.session_id);
    assert_eq!(loaded.session_token, session.session_token);
    assert_eq!(loaded.capabilities.runtime, "ollama");
    assert_eq!(loaded.capabilities.models.len(), 2);
    assert_eq!(loaded.poll_timeout_secs, 10);

    // 清除 session
    storage.clear_session().unwrap();

    // 验证 session 已被清除
    assert!(storage.load_session().unwrap().is_none());
}

#[test]
/// 测试 session 覆盖（多次保存）
fn test_session_overwrite() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

    // 保存第一个 session
    let session1 = SessionData {
        node_id: Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap(),
        session_id: Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap(),
        session_token: "token-1".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            models: vec![],
        },
        poll_timeout_secs: 10,
    };
    storage.save_session(&session1).unwrap();

    // 保存第二个 session（应该覆盖第一个）
    let session2 = SessionData {
        node_id: Uuid::from_str("00000000-0000-0000-0000-000000000003").unwrap(),
        session_id: Uuid::from_str("00000000-0000-0000-0000-000000000004").unwrap(),
        session_token: "token-2".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            models: vec![NodeModelCapability {
                model: "new-model:latest".to_string(),
            }],
        },
        poll_timeout_secs: 20,
    };
    storage.save_session(&session2).unwrap();

    // 验证加载的是第二个 session
    let loaded = storage.load_session().unwrap().unwrap();
    assert_eq!(loaded.node_id, session2.node_id);
    assert_eq!(loaded.session_token, "token-2");
    assert_eq!(loaded.poll_timeout_secs, 20);
}

#[test]
/// 测试损坏的 session 文件
fn test_corrupted_session() {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

    // 手动写入损坏的 JSON
    let session_path = temp_dir.path().join("session.json");
    std::fs::write(&session_path, "this is not valid json").unwrap();

    // 加载应该返回错误
    let result = storage.load_session();
    assert!(result.is_err());
}

#[test]
/// 测试默认数据目录
fn test_default_data_dir() {
    // 不指定 data_dir，应该使用默认路径
    let storage = LocalStorage::new(None).unwrap();
    let path = storage.data_dir();

    // 路径应该以 "node-token" 结尾
    assert!(path.ends_with("node-token"));
}
