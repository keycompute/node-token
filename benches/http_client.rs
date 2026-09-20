//! HTTP 客户端操作性能基准测试
//!
//! 测试 HTTP 请求构建、发送和响应解析的性能，
//! 检测网络操作中的性能瓶颈。

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use node_token::client::KeyComputeClient;
use node_token::protocol::types::{
    NodeCapabilities, NodeHeartbeatRequest, NodeModelCapability, NodePollRequest,
    NodeRegisterRequest,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

/// 创建测试用的注册请求
fn create_register_request() -> NodeRegisterRequest {
    NodeRegisterRequest {
        protocol_version: "node.v1".to_string(),
        client_instance_id: "bench-instance-001".to_string(),
        display_name: "Benchmark Node".to_string(),
        registration_token: "test-token".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            models: vec![NodeModelCapability {
                model: "deepseek-chat:latest".to_string(),
            }],
        },
    }
}

/// 基准测试：客户端创建性能
fn bench_client_creation(c: &mut Criterion) {
    c.bench_function("http/client_creation", |b| {
        b.iter(|| KeyComputeClient::new(black_box("http://localhost:3000".to_string())))
    });
}

/// 基准测试：注册请求构建
fn bench_register_request_building(c: &mut Criterion) {
    c.bench_function("http/build_register_request", |b| {
        b.iter(create_register_request)
    });
}

/// 基准测试：心跳请求构建
fn bench_heartbeat_request_building(c: &mut Criterion) {
    let node_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();

    c.bench_function("http/build_heartbeat_request", |b| {
        b.iter(|| NodeHeartbeatRequest {
            protocol_version: "node.v1".to_string(),
            node_id: black_box(node_id),
            session_id: black_box(session_id),
            accepted_models: vec!["deepseek-chat:latest".to_string()],
        })
    });
}

/// 基准测试：轮询请求构建
fn bench_poll_request_building(c: &mut Criterion) {
    let node_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();

    c.bench_function("http/build_poll_request", |b| {
        b.iter(|| NodePollRequest {
            protocol_version: "node.v1".to_string(),
            node_id: black_box(node_id),
            session_id: black_box(session_id),
        })
    });
}

/// 基准测试：Session Token 设置
fn bench_session_token_setting(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let client = KeyComputeClient::new("http://localhost:3000".to_string());

    c.bench_function("http/set_session_token", |b| {
        b.iter(|| {
            rt.block_on(async {
                client
                    .set_session_token(black_box("test-session-token-12345".to_string()))
                    .await;
            })
        })
    });
}

/// 基准测试：请求头构建（模拟）
fn bench_header_construction(c: &mut Criterion) {
    let token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test";

    c.bench_function("http/build_auth_header", |b| {
        b.iter(|| format!("Bearer {}", black_box(token)))
    });
}

/// 基准测试：URI 拼接
fn bench_uri_construction(c: &mut Criterion) {
    let base_url = "http://localhost:3000";

    c.bench_function("http/build_uri_register", |b| {
        b.iter(|| format!("{}/node/v1/register", black_box(base_url)))
    });
}

/// 基准测试：JSON 请求体构建
fn bench_json_body_construction(c: &mut Criterion) {
    let request = create_register_request();

    c.bench_function("http/build_json_body", |b| {
        b.iter(|| serde_json::to_value(black_box(&request)).unwrap())
    });
}

criterion_group!(
    benches,
    bench_client_creation,
    bench_register_request_building,
    bench_heartbeat_request_building,
    bench_poll_request_building,
    bench_session_token_setting,
    bench_header_construction,
    bench_uri_construction,
    bench_json_body_construction,
);

criterion_main!(benches);
