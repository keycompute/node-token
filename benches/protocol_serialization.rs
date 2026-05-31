//! 协议类型序列化/反序列化性能基准测试
//!
//! 测试节点协议 DTO 的序列化和反序列化性能，
//! 检测性能回归并确保协议处理效率。

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use node_token::protocol::types::{
    ChatCompletionRequest, Message, MessageContent, MessageRole, NodeCapabilities,
    NodeHeartbeatRequest, NodeModelCapability, NodeRegisterRequest, NodeTaskEnvelope,
    NodeTaskPayload,
};
use uuid::Uuid;

/// 创建测试用的注册请求
fn create_register_request() -> NodeRegisterRequest {
    NodeRegisterRequest {
        protocol_version: "node.v1".to_string(),
        client_instance_id: "test-instance-001".to_string(),
        display_name: "Benchmark Node".to_string(),
        registration_token: "test-token".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            models: vec![
                NodeModelCapability {
                    model: "deepseek-chat:latest".to_string(),
                },
                NodeModelCapability {
                    model: "llama3:70b".to_string(),
                },
                NodeModelCapability {
                    model: "qwen2.5:72b".to_string(),
                },
            ],
        },
    }
}

/// 创建测试用的心跳请求
fn create_heartbeat_request() -> NodeHeartbeatRequest {
    NodeHeartbeatRequest {
        protocol_version: "node.v1".to_string(),
        node_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        accepted_models: vec!["deepseek-chat:latest".to_string(), "llama3:70b".to_string()],
    }
}

/// 创建测试用的聊天完成请求
fn create_chat_request() -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: "deepseek-chat:latest".to_string(),
        messages: vec![
            Message {
                role: MessageRole::System,
                content: MessageContent::Text("You are a helpful assistant.".to_string()),
            },
            Message {
                role: MessageRole::User,
                content: MessageContent::Text("Hello, how are you?".to_string()),
            },
        ],
        temperature: Some(0.7),
        max_tokens: Some(1024),
        stream: Some(false),
        top_p: None,
        n: None,
        stop: None,
    }
}

/// 创建测试用的任务信封
fn create_task_envelope() -> NodeTaskEnvelope {
    NodeTaskEnvelope {
        task_id: Uuid::new_v4(),
        lease_id: Uuid::new_v4(),
        model: "deepseek-chat:latest".to_string(),
        deadline_unix_ms: 1700000000000,
        complete_grace_until_unix_ms: 1700000060000,
        payload: NodeTaskPayload {
            request_id: Uuid::new_v4(),
            chat: Some(create_chat_request()),
            image_generation: None,
            image_edit: None,
        },
    }
}

// ========== 序列化基准测试 ==========

/// 基准测试：注册请求序列化
fn bench_register_request_serialize(c: &mut Criterion) {
    let request = create_register_request();

    c.bench_function("serialize/register_request", |b| {
        b.iter(|| serde_json::to_string(black_box(&request)).unwrap())
    });
}

/// 基准测试：心跳请求序列化
fn bench_heartbeat_request_serialize(c: &mut Criterion) {
    let request = create_heartbeat_request();

    c.bench_function("serialize/heartbeat_request", |b| {
        b.iter(|| serde_json::to_string(black_box(&request)).unwrap())
    });
}

/// 基准测试：聊天请求序列化
fn bench_chat_request_serialize(c: &mut Criterion) {
    let request = create_chat_request();

    c.bench_function("serialize/chat_request", |b| {
        b.iter(|| serde_json::to_string(black_box(&request)).unwrap())
    });
}

/// 基准测试：任务信封序列化
fn bench_task_envelope_serialize(c: &mut Criterion) {
    let envelope = create_task_envelope();

    c.bench_function("serialize/task_envelope", |b| {
        b.iter(|| serde_json::to_string(black_box(&envelope)).unwrap())
    });
}

// ========== 反序列化基准测试 ==========

/// 基准测试：注册请求反序列化
fn bench_register_request_deserialize(c: &mut Criterion) {
    let request = create_register_request();
    let json = serde_json::to_string(&request).unwrap();

    c.bench_function("deserialize/register_request", |b| {
        b.iter(|| serde_json::from_str::<NodeRegisterRequest>(black_box(&json)).unwrap())
    });
}

/// 基准测试：心跳请求反序列化
fn bench_heartbeat_request_deserialize(c: &mut Criterion) {
    let request = create_heartbeat_request();
    let json = serde_json::to_string(&request).unwrap();

    c.bench_function("deserialize/heartbeat_request", |b| {
        b.iter(|| serde_json::from_str::<NodeHeartbeatRequest>(black_box(&json)).unwrap())
    });
}

/// 基准测试：聊天请求反序列化
fn bench_chat_request_deserialize(c: &mut Criterion) {
    let request = create_chat_request();
    let json = serde_json::to_string(&request).unwrap();

    c.bench_function("deserialize/chat_request", |b| {
        b.iter(|| serde_json::from_str::<ChatCompletionRequest>(black_box(&json)).unwrap())
    });
}

/// 基准测试：任务信封反序列化
fn bench_task_envelope_deserialize(c: &mut Criterion) {
    let envelope = create_task_envelope();
    let json = serde_json::to_string(&envelope).unwrap();

    c.bench_function("deserialize/task_envelope", |b| {
        b.iter(|| serde_json::from_str::<NodeTaskEnvelope>(black_box(&json)).unwrap())
    });
}

// ========== 大型负载基准测试 ==========

/// 基准测试:大消息列表序列化
fn bench_large_chat_request_serialize(c: &mut Criterion) {
    let mut messages = vec![Message {
        role: MessageRole::System,
        content: MessageContent::Text("You are an expert programmer.".to_string()),
    }];

    // 添加 50 条用户/助手消息
    for i in 0..25 {
        messages.push(Message {
            role: MessageRole::User,
            content: MessageContent::Text(format!("Question {}?", i + 1)),
        });
        messages.push(Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text(format!("Answer {} with detailed explanation.", i + 1)),
        });
    }

    let request = ChatCompletionRequest {
        model: "deepseek-chat:latest".to_string(),
        messages,
        temperature: Some(0.7),
        max_tokens: Some(2048),
        stream: Some(false),
        top_p: None,
        n: None,
        stop: None,
    };

    c.bench_function("serialize/large_chat_request_50_messages", |b| {
        b.iter(|| serde_json::to_string(black_box(&request)).unwrap())
    });
}

// ========== 协议往返测试 ==========

/// 基准测试：序列化 + 反序列化往返
fn bench_roundtrip_task_envelope(c: &mut Criterion) {
    let envelope = create_task_envelope();

    c.bench_function("roundtrip/task_envelope", |b| {
        b.iter(|| {
            let json = serde_json::to_string(black_box(&envelope)).unwrap();
            serde_json::from_str::<NodeTaskEnvelope>(&json).unwrap()
        })
    });
}

// ========== Criterion 分组 ==========

criterion_group!(
    benches,
    bench_register_request_serialize,
    bench_heartbeat_request_serialize,
    bench_chat_request_serialize,
    bench_task_envelope_serialize,
    bench_register_request_deserialize,
    bench_heartbeat_request_deserialize,
    bench_chat_request_deserialize,
    bench_task_envelope_deserialize,
    bench_large_chat_request_serialize,
    bench_roundtrip_task_envelope,
);

criterion_main!(benches);
