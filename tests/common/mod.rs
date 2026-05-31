//! 共享测试辅助函数
//!
//! 提供测试数据工厂函数和通用工具，消除测试代码重复。
//!
//! 注意：这些函数在集成测试中使用，clippy 可能会报告 "unused" 警告，
//! 但这是正常的，因为每个测试文件独立编译。

#![allow(dead_code)]

use node_token::protocol::types::{
    ChatCompletionResponse, ChoiceMessage, CompletionChoice, NodeCapabilities,
    NodeHeartbeatRequest, NodeModelCapability, NodePollRequest, NodeRegisterRequest, Usage,
};
use uuid::Uuid;

/// 测试配置三元组
pub type TestConfig = (Uuid, Uuid, String);

/// 创建测试用的 node_id, session_id, session_token
///
/// # 返回
/// - `(node_id, session_id, session_token)` 三元组
pub fn create_test_config() -> TestConfig {
    let node_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let session_token = format!("test-token-{}", node_id);
    (node_id, session_id, session_token)
}

/// 创建标准的注册请求
///
/// # 参数
/// - `client_instance_id`: 客户端实例 ID（可选，默认 "test-instance-001"）
///
/// # 返回
/// - 配置好的 `NodeRegisterRequest`
pub fn create_register_request(client_instance_id: Option<&str>) -> NodeRegisterRequest {
    NodeRegisterRequest {
        protocol_version: "node.v1".to_string(),
        client_instance_id: client_instance_id
            .unwrap_or("test-instance-001")
            .to_string(),
        display_name: "Test Node".to_string(),
        registration_token: "test-token".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            models: vec![
                NodeModelCapability {
                    model: "deepseek-chat:latest".to_string(),
                },
                NodeModelCapability {
                    model: "llama3:latest".to_string(),
                },
            ],
        },
    }
}

/// 创建标准的心跳请求
///
/// # 参数
/// - `node_id`: 节点 ID
/// - `session_id`: 会话 ID
/// - `accepted_models`: 接受的模型列表（可选，默认包含 deepseek-chat:latest）
///
/// # 返回
/// - 配置好的 `NodeHeartbeatRequest`
pub fn create_heartbeat_request(
    node_id: Uuid,
    session_id: Uuid,
    accepted_models: Option<Vec<String>>,
) -> NodeHeartbeatRequest {
    NodeHeartbeatRequest {
        protocol_version: "node.v1".to_string(),
        node_id,
        session_id,
        accepted_models: accepted_models
            .unwrap_or_else(|| vec!["deepseek-chat:latest".to_string()]),
    }
}

/// 创建标准的轮询请求
///
/// # 参数
/// - `node_id`: 节点 ID
/// - `session_id`: 会话 ID
///
/// # 返回
/// - 配置好的 `NodePollRequest`
pub fn create_poll_request(node_id: Uuid, session_id: Uuid) -> NodePollRequest {
    NodePollRequest {
        protocol_version: "node.v1".to_string(),
        node_id,
        session_id,
    }
}

/// 创建标准的聊天完成响应
///
/// # 参数
/// - `model`: 模型名称（可选，默认 "deepseek-chat:latest"）
/// - `content`: 响应内容（可选，默认 "Hello from Ollama!"）
/// - `prompt_tokens`: prompt token 数（可选，默认 10）
/// - `completion_tokens`: completion token 数（可选，默认 20）
///
/// # 返回
/// - 配置好的 `ChatCompletionResponse`
pub fn create_chat_response(
    model: Option<&str>,
    content: Option<&str>,
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
) -> ChatCompletionResponse {
    let model = model.unwrap_or("deepseek-chat:latest").to_string();
    let content = content.unwrap_or("Hello from Ollama!").to_string();
    let prompt_tokens = prompt_tokens.unwrap_or(10);
    let completion_tokens = completion_tokens.unwrap_or(20);

    ChatCompletionResponse {
        id: "chatcmpl-test".to_string(),
        object: "chat.completion".to_string(),
        created: chrono::Utc::now().timestamp(),
        model,
        choices: vec![CompletionChoice {
            index: 0,
            message: ChoiceMessage {
                role: "assistant".to_string(),
                content,
            },
            finish_reason: Some("stop".to_string()),
        }],
        usage: Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        },
    }
}

/// 创建标准的注册响应 JSON
///
/// # 参数
/// - `node_id`: 节点 ID
/// - `session_id`: 会话 ID
/// - `session_token`: 会话 token
///
/// # 返回
/// - 注册响应的 serde_json::Value
pub fn create_register_response_json(
    node_id: Uuid,
    session_id: Uuid,
    session_token: &str,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": "node.v1",
        "node_id": node_id.to_string(),
        "session_id": session_id.to_string(),
        "session_token": session_token,
        "heartbeat_interval_secs": 30,
        "poll_timeout_secs": 10
    })
}

/// 创建标准的心跳响应 JSON
///
/// # 参数
/// - `accepted`: 是否接受（默认 true）
/// - `node_status`: 节点状态（默认 "online"）
/// - `failure_count`: 失败计数（默认 0）
///
/// # 返回
/// - 心跳响应的 serde_json::Value
pub fn create_heartbeat_response_json(
    accepted: bool,
    node_status: &str,
    failure_count: u32,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": "node.v1",
        "accepted": accepted,
        "node_status": node_status,
        "server_failure_count": failure_count,
        "failure_threshold": 3
    })
}

/// 创建标准的轮询响应 JSON（无任务）
///
/// # 参数
/// - `retry_after_ms`: 重试延迟毫秒（可选，默认 5000）
///
/// # 返回
/// - 轮询响应的 serde_json::Value
pub fn create_poll_empty_response_json(retry_after_ms: Option<u64>) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": "node.v1",
        "task": null,
        "retry_after_ms": retry_after_ms.unwrap_or(5000)
    })
}
