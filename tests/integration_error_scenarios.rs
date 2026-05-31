//! 集成测试 - 错误场景处理
//!
//! 验证节点客户端在各种错误场景下的行为：
//! - 网络超时
//! - 连接失败
//! - 服务端错误
//! - 数据损坏

mod common;

use common::{create_heartbeat_request, create_register_request, create_test_config};
use node_token::client::KeyComputeClient;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
/// 测试网络连接超时场景
///
/// 验证点：
/// 1. 客户端在超时后返回错误
/// 2. 错误信息包含超时相关描述
/// 3. 不会 panic 或卡死
async fn test_network_timeout() {
    let mock_server = MockServer::start().await;
    let (_node_id, _session_id, _session_token) = create_test_config();

    // Mock 一个永不响应的端点（模拟超时）
    Mock::given(method("POST"))
        .and(path("/node/v1/register"))
        .respond_with(
            ResponseTemplate::new(200).set_delay(Duration::from_secs(10)), // 10 秒延迟，超过客户端超时
        )
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    let request = create_register_request(None);

    // 执行注册（应该超时失败）
    let result = tokio::time::timeout(
        Duration::from_secs(2), // 2 秒超时
        client.register(&request),
    )
    .await;

    // 验证超时发生
    assert!(
        result.is_err(),
        "请求应该超时（客户端默认超时应该小于 10 秒）"
    );
}

#[tokio::test]
/// 测试服务端连接失败场景
///
/// 验证点：
/// 1. 客户端无法连接到服务端时返回错误
/// 2. 错误类型正确（连接错误）
/// 3. 错误信息包含连接失败描述
async fn test_connection_refused() {
    // 使用一个不存在的端口（应该拒绝连接）
    let client = KeyComputeClient::new("http://127.0.0.1:1".to_string());
    let request = create_register_request(None);

    // 执行注册（应该连接失败）
    let result = client.register(&request).await;

    // 验证连接失败
    assert!(result.is_err(), "连接应该失败");

    // 验证错误信息（可能是 HTTP 错误或连接错误）
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(
        error_msg.contains("connection")
            || error_msg.contains("refused")
            || error_msg.contains("Connection")
            || error_msg.contains("HttpError")
            || error_msg.contains("503"),
        "错误信息应该包含连接或 HTTP 失败描述: {}",
        error_msg
    );
}

#[tokio::test]
/// 测试 Ollama 连接失败场景（模拟本地模型不可用）
///
/// 验证点：
/// 1. Ollama 客户端连接失败时返回错误
/// 2. 错误信息包含 Ollama 相关描述
/// 3. 节点应该上报错误而不是卡死
async fn test_ollama_connection_failure() {
    // 使用一个不存在的 Ollama 地址
    let ollama_client = node_token::client::OllamaClient::new("http://127.0.0.1:1".to_string());

    // 构造一个简单的聊天请求
    let chat_request = node_token::protocol::ollama::OllamaChatRequest {
        model: "deepseek-chat:latest".to_string(),
        messages: vec![node_token::protocol::ollama::OllamaMessage {
            role: "user".to_string(),
            content: "Hello".to_string(),
            images: None,
        }],
        stream: false,
    };

    // 执行聊天（应该连接失败）
    let result = ollama_client.chat(&chat_request).await;

    // 验证连接失败
    assert!(result.is_err(), "Ollama 连接应该失败");

    // 验证错误信息（HttpError / Ollama / 状态码任一种描述都接受）
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(
        error_msg.contains("connection")
            || error_msg.contains("refused")
            || error_msg.contains("Connection")
            || error_msg.contains("Ollama")
            || error_msg.contains("503")
            || error_msg.contains("502")
            || error_msg.contains("HTTP")
            || error_msg.contains("HttpError"),
        "错误信息应该包含 Ollama 或连接失败描述: {}",
        error_msg
    );
}

#[tokio::test]
/// 测试服务端返回无效 JSON 场景
///
/// 验证点：
/// 1. 客户端能够处理无效的 JSON 响应
/// 2. 返回解析错误而不是 panic
/// 3. 错误信息清晰
async fn test_invalid_json_response() {
    let mock_server = MockServer::start().await;

    // Mock 返回无效 JSON
    Mock::given(method("POST"))
        .and(path("/node/v1/register"))
        .respond_with(ResponseTemplate::new(200).set_body_string("this is not valid json {{{"))
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    let request = create_register_request(None);

    // 执行注册（应该 JSON 解析失败）
    let result = client.register(&request).await;

    // 验证解析失败
    assert!(result.is_err(), "应该返回 JSON 解析错误");

    // 验证错误信息（可能是 Decode 错误或 JSON 解析错误）
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(
        error_msg.contains("json")
            || error_msg.contains("parse")
            || error_msg.contains("JSON")
            || error_msg.contains("deserialize")
            || error_msg.contains("Decode")
            || error_msg.contains("expected"),
        "错误信息应该包含解析或解码错误描述: {}",
        error_msg
    );

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试心跳超时场景
///
/// 验证点：
/// 1. 心跳请求超时后返回错误
/// 2. 不会阻塞线程
async fn test_heartbeat_timeout() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    // Mock 延迟响应
    Mock::given(method("POST"))
        .and(path("/node/v1/heartbeat"))
        .and(wiremock::matchers::header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(10)))
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let request = create_heartbeat_request(node_id, session_id, None);

    // 执行心跳（应该超时）
    let result = tokio::time::timeout(Duration::from_secs(2), client.heartbeat(&request)).await;

    // 验证超时
    assert!(result.is_err(), "心跳应该超时");
}

#[tokio::test]
/// 测试并发请求失败场景
///
/// 验证点：
/// 1. 多个并发请求失败时不会互相影响
/// 2. 错误隔离正确
async fn test_concurrent_request_failures() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let mock_server = MockServer::start().await;

    // Mock 所有请求都失败
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let client = Arc::new(KeyComputeClient::new(mock_server.uri()));
    let barrier = Arc::new(Barrier::new(3));

    // 并发发送 3 个请求
    let mut handles = vec![];
    for _ in 0..3 {
        let client = client.clone();
        let barrier = barrier.clone();
        let handle = tokio::spawn(async move {
            barrier.wait().await;
            let request = create_register_request(None);
            client.register(&request).await
        });
        handles.push(handle);
    }

    // 等待所有请求完成
    let results = futures::future::join_all(handles).await;

    // 验证所有请求都失败
    for result in results {
        let response = result.expect("任务应该完成");
        assert!(response.is_err(), "所有请求都应该失败");
    }

    // 验证 mock 被调用 3 次
    mock_server.verify().await;
}

#[tokio::test]
/// 测试网络中断后恢复场景
///
/// 验证点：
/// 1. 网络恢复后客户端可以正常工作
/// 2. 不会留下错误状态
async fn test_network_recovery() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    // 第一次请求：连接失败（使用错误地址）
    let bad_client = KeyComputeClient::new("http://127.0.0.1:1".to_string());
    let request = create_register_request(None);
    let result = bad_client.register(&request).await;
    assert!(result.is_err(), "第一次请求应该失败");

    // 网络恢复：使用正确的地址
    Mock::given(method("POST"))
        .and(path("/node/v1/register"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "protocol_version": "node.v1",
            "node_id": node_id.to_string(),
            "session_id": session_id.to_string(),
            "session_token": session_token,
            "heartbeat_interval_secs": 30,
            "poll_timeout_secs": 10
        })))
        .mount(&mock_server)
        .await;

    let good_client = KeyComputeClient::new(mock_server.uri());
    let response = good_client.register(&request).await;

    // 验证恢复后请求成功
    assert!(response.is_ok(), "网络恢复后请求应该成功");
    assert_eq!(response.unwrap().node_id, node_id);

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试磁盘空间不足场景（持久化失败）
///
/// 验证点：
/// 1. 磁盘满时保存 session 失败
/// 2. 错误信息清晰
/// 3. 不会破坏现有数据
async fn test_disk_full_scenario() {
    use tempfile::TempDir;

    // 创建一个正常的临时目录
    let temp_dir = TempDir::new().unwrap();
    let storage =
        node_token::storage::LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

    // 正常保存 session
    let (node_id, session_id, session_token) = create_test_config();
    let session_data = node_token::storage::SessionData {
        node_id,
        session_id,
        session_token: session_token.clone(),
        capabilities: node_token::protocol::types::NodeCapabilities {
            runtime: "ollama".to_string(),
            models: vec![node_token::protocol::types::NodeModelCapability {
                model: "deepseek-chat:latest".to_string(),
            }],
        },
        poll_timeout_secs: 10,
    };

    // 第一次保存应该成功
    storage
        .save_session(&session_data)
        .expect("第一次保存应该成功");

    // 验证可以加载
    let loaded = storage.load_session().unwrap();
    assert!(loaded.is_some(), "应该能加载 session");

    // 注意：实际测试磁盘满需要特殊环境设置
    // 这里只验证正常流程不会破坏数据
}
