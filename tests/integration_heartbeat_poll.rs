//! 集成测试 - 心跳和轮询流程
//!
//! 验证节点心跳保活和任务轮询的端到端功能。
//!
//! ## 测试覆盖
//! - 心跳成功流程
//! - 轮询有任务场景
//! - 轮询无任务场景
//! - Vision 多模态任务轮询场景

mod common;

use common::{
    create_heartbeat_request, create_heartbeat_response_json, create_poll_empty_response_json,
    create_poll_request, create_test_config,
};
use node_token::client::KeyComputeClient;
use node_token::protocol::types::MessageRole;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
/// 测试心跳完整流程
///
/// 验证点：
/// 1. 客户端成功发送心跳请求
/// 2. 携带正确的 Authorization header
/// 3. 服务端返回 accepted=true 和节点状态
async fn test_heartbeat_flow() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

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

    let request = create_heartbeat_request(node_id, session_id, None);
    let response = client.heartbeat(&request).await.expect("心跳请求应该成功");

    assert!(response.accepted, "心跳应该被接受");
    assert_eq!(response.node_status, "online");
    assert_eq!(response.server_failure_count, 0);
    assert_eq!(response.failure_threshold, 3);

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试轮询领取任务（有任务场景）
///
/// 验证点：
/// 1. 客户端成功发送轮询请求
/// 2. 服务端返回任务信封
/// 3. 任务数据完整（task_id, lease_id, model, payload）
async fn test_poll_with_task() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    Mock::given(method("POST"))
        .and(path("/node/v1/tasks/poll"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "protocol_version": "node.v1",
            "task": {
                "task_id": "00000000-0000-0000-0000-000000000010",
                "lease_id": "00000000-0000-0000-0000-000000000011",
                "model": "deepseek-chat:latest",
                "deadline_unix_ms": 9999999999999i64,
                "complete_grace_until_unix_ms": 9999999999999i64,
                "payload": {
                    "request_id": "00000000-0000-0000-0000-000000000012",
                    "chat": {
                        "model": "deepseek-chat:latest",
                        "messages": [{"role": "user", "content": "Hello"}],
                        "stream": false
                    }
                }
            },
            "retry_after_ms": null
        })))
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let request = create_poll_request(node_id, session_id);
    let response = client.poll(&request).await.expect("轮询请求应该成功");

    assert!(response.task.is_some(), "应该返回任务");
    let task = response.task.unwrap();
    assert_eq!(task.model, "deepseek-chat:latest");
    assert_eq!(
        task.payload.chat.as_ref().unwrap().messages[0]
            .content
            .extract_text(),
        "Hello"
    );

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试轮询领取任务（无任务场景）
///
/// 验证点：
/// 1. 客户端成功发送轮询请求
/// 2. 服务端返回 task=null
/// 3. 包含 retry_after_ms 建议
async fn test_poll_no_task() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

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

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let request = create_poll_request(node_id, session_id);
    let response = client.poll(&request).await.expect("轮询请求应该成功");

    assert!(response.task.is_none(), "不应该返回任务");
    assert_eq!(response.retry_after_ms, Some(5000));

    // 验证 mock 被调用
    mock_server.verify().await;
}

#[tokio::test]
/// 测试轮询领取 Vision 多模态任务
///
/// 验证点：
/// 1. 服务端返回的 chat 任务包含 Vision 多模态消息（ContentPart）
/// 2. 反序列化正确解析 Parts 变体（含 text + image_url）
/// 3. extract_text() 和 extract_images() 工作正常
async fn test_poll_with_vision_task() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    Mock::given(method("POST"))
        .and(path("/node/v1/tasks/poll"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "protocol_version": "node.v1",
            "task": {
                "task_id": "00000000-0000-0000-0000-000000000020",
                "lease_id": "00000000-0000-0000-0000-000000000021",
                "model": "llava:latest",
                "deadline_unix_ms": 9999999999999i64,
                "complete_grace_until_unix_ms": 9999999999999i64,
                "payload": {
                    "request_id": "00000000-0000-0000-0000-000000000022",
                    "chat": {
                        "model": "llava:latest",
                        "messages": [{
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "Describe this image"},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
                                    }
                                }
                            ]
                        }],
                        "stream": false
                    }
                }
            },
            "retry_after_ms": null
        })))
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let request = create_poll_request(node_id, session_id);
    let response = client.poll(&request).await.expect("轮询请求应该成功");

    assert!(response.task.is_some(), "应该返回 Vision 任务");
    let task = response.task.unwrap();
    assert_eq!(task.model, "llava:latest");

    let chat_req = task.payload.chat.as_ref().unwrap();
    assert_eq!(chat_req.messages.len(), 1);
    assert_eq!(chat_req.messages[0].role, MessageRole::User);

    // 验证多模态内容解析
    let text = chat_req.messages[0].content.extract_text();
    assert_eq!(text, "Describe this image");

    let images = chat_req.messages[0].content.extract_images();
    assert_eq!(images.len(), 1);
    assert!(images[0].starts_with("data:image/png;base64,"));

    mock_server.verify().await;
}

#[tokio::test]
/// 测试轮询领取图片生成任务
///
/// 验证点：
/// 1. 服务端返回的图片生成任务能正确反序列化
/// 2. NodeTaskPayload.image_generation 字段解析正确
/// 3. is_image_generation() 方法工作正常
async fn test_poll_with_image_generation_task() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    Mock::given(method("POST"))
        .and(path("/node/v1/tasks/poll"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "protocol_version": "node.v1",
            "task": {
                "task_id": "00000000-0000-0000-0000-000000000030",
                "lease_id": "00000000-0000-0000-0000-000000000031",
                "model": "stable-diffusion",
                "deadline_unix_ms": 9999999999999i64,
                "complete_grace_until_unix_ms": 9999999999999i64,
                "payload": {
                    "request_id": "00000000-0000-0000-0000-000000000032",
                    "image_generation": {
                        "prompt": "a beautiful sunset over mountains",
                        "n": 2,
                        "size": "1024x1024"
                    }
                }
            },
            "retry_after_ms": null
        })))
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let request = create_poll_request(node_id, session_id);
    let response = client.poll(&request).await.expect("轮询请求应该成功");

    assert!(response.task.is_some(), "应该返回图片生成任务");
    let task = response.task.unwrap();
    assert_eq!(task.model, "stable-diffusion");
    assert!(task.payload.is_image_generation(), "应为图片生成任务");
    assert!(!task.payload.is_chat());
    assert!(!task.payload.is_image_edit());

    let img_req = task.payload.image_generation.as_ref().unwrap();
    assert_eq!(img_req.prompt, "a beautiful sunset over mountains");
    assert_eq!(img_req.n, Some(2));
    assert_eq!(img_req.size, Some("1024x1024".to_string()));

    mock_server.verify().await;
}

#[tokio::test]
/// 测试轮询领取图片编辑任务
///
/// 验证点：
/// 1. 服务端返回的图片编辑任务能正确反序列化
/// 2. NodeTaskPayload.image_edit 字段解析正确（含 base64 image/mask）
async fn test_poll_with_image_edit_task() {
    let mock_server = MockServer::start().await;
    let (node_id, session_id, session_token) = create_test_config();

    Mock::given(method("POST"))
        .and(path("/node/v1/tasks/poll"))
        .and(header(
            "Authorization",
            format!("Bearer {}", session_token).as_str(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "protocol_version": "node.v1",
            "task": {
                "task_id": "00000000-0000-0000-0000-000000000040",
                "lease_id": "00000000-0000-0000-0000-000000000041",
                "model": "stable-diffusion",
                "deadline_unix_ms": 9999999999999i64,
                "complete_grace_until_unix_ms": 9999999999999i64,
                "payload": {
                    "request_id": "00000000-0000-0000-0000-000000000042",
                    "image_edit": {
                        "prompt": "add a blue sky",
                        "image": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk",
                        "mask": "iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAFElEQVR42mNk",
                        "n": 1
                    }
                }
            },
            "retry_after_ms": null
        })))
        .mount(&mock_server)
        .await;

    let client = KeyComputeClient::new(mock_server.uri());
    client.set_session_token(session_token).await;

    let request = create_poll_request(node_id, session_id);
    let response = client.poll(&request).await.expect("轮询请求应该成功");

    assert!(response.task.is_some(), "应该返回图片编辑任务");
    let task = response.task.unwrap();
    assert_eq!(task.model, "stable-diffusion");
    assert!(task.payload.is_image_edit(), "应为图片编辑任务");
    assert!(!task.payload.is_chat());
    assert!(!task.payload.is_image_generation());

    let edit_req = task.payload.image_edit.as_ref().unwrap();
    assert_eq!(edit_req.prompt, "add a blue sky");
    assert!(!edit_req.image.is_empty());
    assert!(edit_req.mask.is_some());
    assert_eq!(edit_req.n, Some(1));

    mock_server.verify().await;
}

#[test]
/// 测试 Vision 消息空数组拒绝（防静默数据丢失）
///
/// 验证点：
/// 1. 消息 content 为空数组 [] 时反序列化必须失败
/// 2. 防止多模态数据被静默丢弃
fn test_vision_message_empty_parts_rejected() {
    // 纯文本 content 正常工作
    let json = r#"{"role":"user","content":"Hello"}"#;
    let msg: node_token::protocol::types::Message = serde_json::from_str(json).unwrap();
    assert_eq!(msg.content.extract_text(), "Hello");

    // 非空 Parts 正常工作
    let json = r#"{
        "role": "user",
        "content": [{"type": "text", "text": "Hi"}]
    }"#;
    let msg: node_token::protocol::types::Message = serde_json::from_str(json).unwrap();
    assert_eq!(msg.content.extract_text(), "Hi");

    // 空数组 [] 必须拒绝
    let json = r#"{"role":"user","content":[]}"#;
    let result: Result<node_token::protocol::types::Message, _> = serde_json::from_str(json);
    assert!(
        result.is_err(),
        "Empty content array must be rejected to prevent silent data loss"
    );
}
