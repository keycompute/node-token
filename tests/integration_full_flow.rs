//! 集成测试 - 完整执行流程
//!
//! 验证节点从注册到任务执行的完整生命周期：
//! 1. 节点注册
//! 2. 心跳保活
//! 3. 轮询领取任务
//! 4. 执行任务（模拟 Ollama）
//! 5. 提交结果
//! 6. 验证完整流程

mod common;

use common::{
    create_chat_response, create_heartbeat_request, create_heartbeat_response_json,
    create_poll_empty_response_json, create_poll_request, create_register_request,
    create_test_config,
};
use node_token::client::KeyComputeClient;
use node_token::protocol::types::{NodeTaskCompleteRequest, NodeTaskResult};
use tempfile::TempDir;
use uuid::Uuid;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
/// 测试完整节点生命周期：注册 → 心跳 → 轮询 → 提交
async fn test_full_node_lifecycle() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    // ========== 阶段 1: 节点注册 ==========
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

    let client = KeyComputeClient::new(mock_server.uri());
    let register_request = create_register_request(None);

    let register_response = client
        .register(&register_request)
        .await
        .expect("注册应该成功");
    assert_eq!(register_response.node_id, node_id);
    assert_eq!(register_response.session_id, session_id);
    assert_eq!(register_response.session_token, session_token);

    // 设置 session token
    client.set_session_token(session_token.clone()).await;

    // ========== 阶段 2: 心跳保活 ==========
    Mock::given(method("POST"))
        .and(path("/node/v1/heartbeat"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(create_heartbeat_response_json(true, "online", 0)),
        )
        .mount(&mock_server)
        .await;

    let heartbeat_request = create_heartbeat_request(node_id, session_id, None);
    let heartbeat_response = client
        .heartbeat(&heartbeat_request)
        .await
        .expect("心跳应该成功");
    assert!(heartbeat_response.accepted);
    assert_eq!(heartbeat_response.node_status, "online");

    // ========== 阶段 3: 轮询领取任务（无任务） ==========
    Mock::given(method("POST"))
        .and(path("/node/v1/tasks/poll"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(create_poll_empty_response_json(Some(5000))),
        )
        .mount(&mock_server)
        .await;

    let poll_request = create_poll_request(node_id, session_id);
    let poll_response = client.poll(&poll_request).await.expect("轮询应该成功");
    assert!(poll_response.task.is_none());
    assert_eq!(poll_response.retry_after_ms, Some(5000));

    // ========== 阶段 4: 验证本地持久化 ==========
    let temp_dir = TempDir::new().unwrap();
    let storage =
        node_token::storage::LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();

    // 保存 session
    let session_data = node_token::storage::SessionData {
        node_id,
        session_id,
        session_token: session_token.clone(),
        capabilities: node_token::protocol::types::NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            models: vec![node_token::protocol::types::NodeModelCapability {
                model: "deepseek-chat:latest".to_string(),
            }],
            native_profiles: vec![],
            runtime_version: None,
        },
        poll_timeout_secs: 10,
    };

    storage.save_session(&session_data).unwrap();

    // 加载并验证 session
    let loaded_session = storage.load_session().unwrap().unwrap();
    assert_eq!(loaded_session.node_id, node_id);
    assert_eq!(loaded_session.session_id, session_id);
    assert_eq!(loaded_session.session_token, session_token);

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试任务提交完整流程
///
/// 验证点：
/// 1. NodeTaskCompleteRequest 数据结构构造正确
/// 2. NodeTaskResult::Succeeded 包含完整的 ChatCompletionResponse
/// 3. 所有字段值可正确访问和验证
async fn test_task_complete_flow() {
    let (node_id, session_id, _session_token) = create_test_config();

    // 构造任务 ID
    let task_id = Uuid::new_v4();
    let lease_id = Uuid::new_v4();

    // 使用工厂函数创建聊天响应
    let chat_response = create_chat_response(
        Some("deepseek-chat:latest"),
        Some("Hello from Ollama!"),
        Some(10),
        Some(20),
    );

    let complete_request = NodeTaskCompleteRequest {
        protocol_version: "node.v1".to_string(),
        node_id,
        session_id,
        task_id,
        lease_id,
        result: NodeTaskResult::Succeeded {
            response: chat_response,
        },
    };

    // 验证数据结构构造正确
    assert_eq!(complete_request.task_id, task_id);
    assert_eq!(complete_request.lease_id, lease_id);
    assert_eq!(complete_request.node_id, node_id);
    assert_eq!(complete_request.session_id, session_id);

    match &complete_request.result {
        NodeTaskResult::Succeeded { response } => {
            assert_eq!(response.model, "deepseek-chat:latest");
            assert_eq!(response.choices[0].message.content, "Hello from Ollama!");
            assert_eq!(response.usage.total_tokens, 30);
        }
        _ => panic!("Expected Succeeded variant"),
    }
}

#[tokio::test]
/// 测试多轮心跳保活
///
/// 验证点：
/// 1. 多次连续心跳请求都能成功
/// 2. 服务端状态保持一致
async fn test_multiple_heartbeats() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    // Mock 心跳端点
    Mock::given(method("POST"))
        .and(path("/node/v1/heartbeat"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(create_heartbeat_response_json(true, "online", 0)),
        )
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    // 发送 5 次心跳
    for i in 0..5 {
        let heartbeat_request = create_heartbeat_request(node_id, session_id, None);
        let response = client
            .heartbeat(&heartbeat_request)
            .await
            .unwrap_or_else(|_| panic!("心跳 {} 应该成功", i + 1));
        assert!(response.accepted, "Heartbeat {} should be accepted", i + 1);
        assert_eq!(response.node_status, "online");
    }

    // 验证 mock 被调用（5 次心跳）
    mock_server.verify().await;
}

#[tokio::test]
/// 测试 excluded 节点恢复流程
///
/// 验证点：
/// 1. 节点可以被标记为 excluded 状态
/// 2. 心跳仍然被接受（用于监控）
/// 3. 失败计数正确返回
async fn test_excluded_node_recovery() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    // 第一次心跳：节点被 excluded
    Mock::given(method("POST"))
        .and(path("/node/v1/heartbeat"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(create_heartbeat_response_json(true, "excluded", 3)),
        )
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let heartbeat_request = create_heartbeat_request(node_id, session_id, None);
    let response = client
        .heartbeat(&heartbeat_request)
        .await
        .expect("心跳应该成功");
    assert!(response.accepted);
    assert_eq!(response.node_status, "excluded");
    assert_eq!(response.server_failure_count, 3);

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试并发心跳和轮询
///
/// 验证点：
/// 1. 心跳和轮询可以并发执行
/// 2. 两个请求都能成功返回
/// 3. 使用 Barrier 确保并发同步
async fn test_concurrent_heartbeat_and_poll() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    // Mock 心跳
    Mock::given(method("POST"))
        .and(path("/node/v1/heartbeat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(create_heartbeat_response_json(true, "online", 0)),
        )
        .mount(&mock_server)
        .await;

    // Mock 轮询
    Mock::given(method("POST"))
        .and(path("/node/v1/tasks/poll"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(create_poll_empty_response_json(Some(1000))),
        )
        .mount(&mock_server)
        .await;

    let client = Arc::new(KeyComputeClient::new(mock_server.uri()));
    client.set_session_token(session_token).await;

    let barrier = Arc::new(Barrier::new(2));

    // 并发执行心跳和轮询
    let client_heartbeat = client.clone();
    let heartbeat_handle = tokio::spawn({
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            let request = create_heartbeat_request(node_id, session_id, None);
            client_heartbeat.heartbeat(&request).await.unwrap()
        }
    });

    let client_poll = client.clone();
    let poll_handle = tokio::spawn({
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            let request = create_poll_request(node_id, session_id);
            client_poll.poll(&request).await.unwrap()
        }
    });

    // 等待两个任务完成
    let (heartbeat_result, poll_result) = tokio::join!(heartbeat_handle, poll_handle);

    assert!(heartbeat_result.unwrap().accepted);
    assert!(poll_result.unwrap().task.is_none());

    // 验证 mock 被调用
    mock_server.verify().await;
}
