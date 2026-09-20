//! Real-loopback cancellation contracts; never uses an installed Ollama service.
use node_token::{
    client::{api::KeyComputeClient, ollama::OllamaClient},
    protocol::{
        node_capability::{NativeFeature, NativeModelProfile},
        node_native::{NodeNativeOperation, NodeNativeRequest},
        types::*,
    },
    runtime::executor::TaskExecutor,
    storage::SessionData,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use uuid::Uuid;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
const MODEL: &str = "local-cancellation-model";
const TOKEN: &str = "isolated-issued-session";
fn success() -> Value {
    json!({"id":"c","object":"chat.completion","model":MODEL,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}})
}
fn setup(
    api: &MockServer,
    upstream: String,
    required: bool,
    capable: bool,
) -> (TaskExecutor, NodeTaskEnvelope, Arc<KeyComputeClient>) {
    let node_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let features = if capable {
        vec![NativeFeature::Cancellation]
    } else {
        vec![]
    };
    let session = SessionData {
        node_id,
        session_id,
        session_token: TOKEN.into(),
        poll_timeout_secs: 1,
        capabilities: NodeCapabilities {
            runtime: "ollama".into(),
            runtime_version: Some("0.14.0".into()),
            models: vec![NodeModelCapability {
                model: MODEL.into(),
            }],
            native_operations: vec![NodeNativeOperation::Chat],
            native_profiles: vec![NativeModelProfile::plain_chat(MODEL).with_features(features)],
        },
    };
    let task = NodeTaskEnvelope {
        requires_cancellation: required,
        task_id: Uuid::new_v4(),
        lease_id: Uuid::new_v4(),
        model: MODEL.into(),
        deadline_unix_ms: chrono::Utc::now().timestamp_millis() + 20000,
        complete_grace_until_unix_ms: chrono::Utc::now().timestamp_millis() + 22000,
        payload: NodeTaskPayload {
            request_id: Uuid::new_v4(),
            chat: None,
            image_generation: None,
            image_edit: None,
            native: Some(NodeNativeRequest {
                operation: NodeNativeOperation::Chat,
                headers: vec![],
                body: json!({"model":MODEL,"messages":[{"role":"user","content":"Hello"}],"stream":false}),
            }),
        },
    };
    let client = Arc::new(KeyComputeClient::new_with_token(api.uri(), TOKEN.into()));
    (
        TaskExecutor::new(
            client.clone(),
            Arc::new(OllamaClient::new(upstream)),
            session,
            Arc::new(AtomicBool::new(false)),
        ),
        task,
        client,
    )
}
async fn complete_ack(api: &MockServer, task: &NodeTaskEnvelope, status: u16) {
    Mock::given(method("POST")).and(path(format!("/node/v1/tasks/{}/complete",task.task_id))).and(header("authorization",format!("Bearer {TOKEN}").as_str()))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({"action":"failed","task_status":"failed","node_status":"online","server_failure_count":0,"failure_threshold":3}))).expect(1).mount(api).await;
}
async fn hanging_upstream() -> (String, oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![];
        let mut buffer = [0; 4096];
        let end = loop {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(i) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let len = String::from_utf8_lossy(&bytes[..end])
            .lines()
            .find_map(|s| {
                s.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|s| s.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        while bytes.len() < end + len {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2000\r\n\r\n").await.unwrap();
        match socket.read(&mut buffer).await {
            Ok(0) | Err(_) => {
                let _ = closed_tx.send(());
            }
            Ok(_) => panic!("unexpected extra request on inference connection"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "cancellation must not cause another inference connection"
        );
    });
    (format!("http://{address}"), closed_rx, server)
}
#[tokio::test]
async fn inactive_lease_prevents_inference_and_rotation_does_not_change_issued_credentials() {
    let api = MockServer::start().await;
    let upstream = MockServer::start().await;
    let (executor, task, parent) = setup(&api, upstream.uri(), true, true);
    complete_ack(&api, &task, 200).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/node/v1/tasks/{}/lease-status",
            task.task_id
        )))
        .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"active":false,"status":"failed"})),
        )
        .expect(1)
        .mount(&api)
        .await;
    parent
        .set_session_token("a-different-current-session".into())
        .await;
    tokio::time::timeout(Duration::from_secs(3), executor.execute(task))
        .await
        .unwrap();
    assert!(upstream.received_requests().await.unwrap().is_empty());
    let sent = api.received_requests().await.unwrap();
    let complete = sent
        .iter()
        .find(|r| r.url.path().ends_with("/complete"))
        .unwrap();
    let body: Value = serde_json::from_slice(&complete.body).unwrap();
    assert_eq!(body["result"]["code"], "native_task_cancelled");
}
#[tokio::test]
async fn cancellation_closes_a_held_native_response_and_does_not_retry_rejected_completion() {
    let api = MockServer::start().await;
    let (upstream, closed, server) = hanging_upstream().await;
    let (executor, task, _) = setup(&api, upstream, true, true);
    complete_ack(&api, &task, 409).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    Mock::given(method("POST"))
        .and(path(format!(
            "/node/v1/tasks/{}/lease-status",
            task.task_id
        )))
        .respond_with(move |_: &wiremock::Request| {
            let n = count.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_body_json(json!({"active":n==0,"status":if n==0{"leased"}else{"failed"}}))
        })
        .mount(&api)
        .await;
    let start = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(6), executor.execute(task))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), closed)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn lease_uncertainty_is_bounded_without_repeating_inference() {
    let api = MockServer::start().await;
    let (upstream, closed, server) = hanging_upstream().await;
    let (executor, task, _) = setup(&api, upstream, true, true);
    complete_ack(&api, &task, 200).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    Mock::given(method("POST"))
        .and(path(format!(
            "/node/v1/tasks/{}/lease-status",
            task.task_id
        )))
        .respond_with(move |_: &wiremock::Request| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200).set_body_json(json!({"active":true,"status":"leased"}))
            } else {
                ResponseTemplate::new(503)
            }
        })
        .mount(&api)
        .await;
    tokio::time::timeout(Duration::from_secs(10), executor.execute(task))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), closed)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}
#[tokio::test]
async fn unadvertised_cancellation_is_rejected_and_plain_tasks_do_not_poll() {
    for required in [true, false] {
        let api = MockServer::start().await;
        let upstream = MockServer::start().await;
        let (executor, task, _) = setup(&api, upstream.uri(), required, false);
        complete_ack(&api, &task, 200).await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success()))
            .expect(if required { 0 } else { 1 })
            .mount(&upstream)
            .await;
        tokio::time::timeout(Duration::from_secs(3), executor.execute(task))
            .await
            .unwrap();
        let calls = api.received_requests().await.unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].url.path().ends_with("/complete"));
        let result: Value = serde_json::from_slice(&calls[0].body).unwrap();
        assert_eq!(
            result["result"]["status"],
            if required {
                "failed"
            } else {
                "native_succeeded"
            }
        );
    }
}
#[tokio::test]
async fn successful_cancellable_inference_stops_its_control_monitor() {
    let api = MockServer::start().await;
    let upstream = MockServer::start().await;
    let (executor, task, _) = setup(&api, upstream.uri(), true, true);
    complete_ack(&api, &task, 200).await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success()))
        .expect(1)
        .mount(&upstream)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/node/v1/tasks/{}/lease-status",
            task.task_id
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"active":true,"status":"leased"})),
        )
        .expect(1)
        .mount(&api)
        .await;
    tokio::time::timeout(Duration::from_secs(3), executor.execute(task))
        .await
        .unwrap();
    let calls = api.received_requests().await.unwrap();
    let sent = calls
        .iter()
        .find(|r| r.url.path().ends_with("/complete"))
        .unwrap();
    let result: Value = serde_json::from_slice(&sent.body).unwrap();
    assert_eq!(result["result"]["response"]["body"], success());
}
