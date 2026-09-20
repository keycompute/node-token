//! 本地存储操作性能基准测试
//!
//! 测试会话数据的保存、加载和清除操作性能，
//! 检测文件系统 I/O 瓶颈。

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use node_token::protocol::types::{NodeCapabilities, NodeModelCapability};
use node_token::storage::{LocalStorage, SessionData};
use tempfile::TempDir;
use uuid::Uuid;

/// 创建测试用的会话数据
fn create_session_data() -> SessionData {
    SessionData {
        node_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        session_token: "test-session-token-abc123xyz".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            native_profiles: vec![],
            runtime_version: None,
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
                NodeModelCapability {
                    model: "mistral:8x7b".to_string(),
                },
            ],
        },
        poll_timeout_secs: 30,
    }
}

/// 基准测试：会话数据保存
///
/// 测试首次写入会话数据的性能（每次迭代使用新目录）
fn bench_session_save(c: &mut Criterion) {
    let session = create_session_data();

    c.bench_function("storage/save_session", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
            storage.save_session(black_box(&session)).unwrap()
        })
    });
}

/// 基准测试：会话数据加载
///
/// 测试从文件加载已存在的会话数据
fn bench_session_load(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
    let session = create_session_data();
    storage.save_session(&session).unwrap();

    c.bench_function("storage/load_session", |b| {
        b.iter(|| storage.load_session().unwrap())
    });
}

/// 基准测试：保存 + 加载往返
///
/// 测试完整的保存和加载循环（每次迭代使用新目录）
fn bench_session_roundtrip(c: &mut Criterion) {
    let session = create_session_data();

    c.bench_function("storage/save_load_roundtrip", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
            storage.save_session(black_box(&session)).unwrap();
            storage.load_session().unwrap()
        })
    });
}

/// 基准测试：会话清除
///
/// 测试删除会话文件的性能
fn bench_session_clear(c: &mut Criterion) {
    c.bench_function("storage/clear_session", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
            let session = create_session_data();
            storage.save_session(&session).unwrap();
            storage.clear_session().unwrap()
        })
    });
}

/// 基准测试：存储初始化
///
/// 测试创建 LocalStorage 实例的性能
fn bench_storage_creation(c: &mut Criterion) {
    c.bench_function("storage/create_storage", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            LocalStorage::new(Some(black_box(temp_dir.path().to_str().unwrap()))).unwrap()
        })
    });
}

/// 基准测试：加载不存在的会话
///
///测试加载不存在的会话文件时的性能（应快速返回 None）
fn bench_load_nonexistent_session(c: &mut Criterion) {
    c.bench_function("storage/load_nonexistent_session", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
            storage.load_session()
        })
    });
}

/// 基准测试：大型会话数据保存
///
/// 测试保存包含 50 个模型的会话数据性能
fn bench_large_session_save(c: &mut Criterion) {
    // 创建包含大量模型的会话
    let mut models = Vec::new();
    for i in 0..50 {
        models.push(NodeModelCapability {
            model: format!("model-{}:latest", i),
        });
    }

    let session = SessionData {
        node_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        session_token: "large-session-token-xyz789".to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".to_string(),
            native_operations: vec![],
            native_profiles: vec![],
            runtime_version: None,
            models,
        },
        poll_timeout_secs: 30,
    };

    c.bench_function("storage/save_large_session_50_models", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
            storage.save_session(black_box(&session)).unwrap()
        })
    });
}

/// 基准测试：连续多次保存（模拟高频更新）
///
/// 测试在同一文件上连续保存 10 次的性能（测试覆盖写入）
fn bench_consecutive_saves(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let storage = LocalStorage::new(Some(temp_dir.path().to_str().unwrap())).unwrap();
    let session = create_session_data();

    c.bench_function("storage/10_consecutive_saves", |b| {
        b.iter(|| {
            for _ in 0..10 {
                storage.save_session(black_box(&session)).unwrap();
            }
        })
    });
}

/// 基准测试：JSON 序列化性能（单独测试）
///
/// 测试会话数据的 JSON 序列化性能（不含文件 I/O）
fn bench_session_json_serialize(c: &mut Criterion) {
    let session = create_session_data();

    c.bench_function("storage/json_serialize_session", |b| {
        b.iter(|| serde_json::to_string(black_box(&session)).unwrap())
    });
}

/// 基准测试：JSON 反序列化性能（单独测试）
///
/// 测试会话数据的 JSON 反序列化性能（不含文件 I/O）
fn bench_session_json_deserialize(c: &mut Criterion) {
    let session = create_session_data();
    let json = serde_json::to_string(&session).unwrap();

    c.bench_function("storage/json_deserialize_session", |b| {
        b.iter(|| serde_json::from_str::<SessionData>(black_box(&json)).unwrap())
    });
}

criterion_group!(
    benches,
    bench_session_save,
    bench_session_load,
    bench_session_roundtrip,
    bench_session_clear,
    bench_storage_creation,
    bench_load_nonexistent_session,
    bench_large_session_save,
    bench_consecutive_saves,
    bench_session_json_serialize,
    bench_session_json_deserialize,
);

criterion_main!(benches);
