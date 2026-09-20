//! Native fidelity and single-attempt transport regressions; loopback mocks only.
use node_token::{
    client::{api::KeyComputeClient, ollama::OllamaClient},
    protocol::{
        node_capability::{NativeFeature, NativeModelProfile},
        node_native::*,
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
fn request() -> NodeNativeRequest {
    serde_json::from_str(include_str!("fixtures/node-native-chat-v1.json")).unwrap()
}
fn success() -> Value {
    json!({"id":"native-test","model":"node:literal","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"x","type":"function","function":{"name":"f","arguments":"{}"}}],"reasoning":"kept"},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5},"extra":[null,42]})
}
#[tokio::test]
async fn native_errors_are_forwarded_once_and_redirects_are_not_followed() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success()))
        .expect(0)
        .mount(&target)
        .await;
    for status in [400, 422, 429, 500, 302] {
        let server = MockServer::start().await;
        let body = json!({"error":{"message":"original","code":"specific","data":[null,1]}});
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(body_json(&request().body))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header(
                        "location",
                        format!("{}/v1/chat/completions", target.uri()).as_str(),
                    )
                    .insert_header("set-cookie", "secret")
                    .set_body_json(&body),
            )
            .expect(1)
            .mount(&server)
            .await;
        let result = OllamaClient::new(server.uri())
            .native_chat(&request(), chrono::Utc::now().timestamp_millis() + 2000)
            .await
            .unwrap();
        assert_eq!(result.status, status);
        assert_eq!(result.body, body);
        assert!(
            result
                .headers
                .iter()
                .all(|(n, _)| n != "set-cookie" && n != "location")
        );
        assert_eq!(result.validate("node:literal").is_ok(), status != 302);
    }
}
#[tokio::test]
async fn native_deadline_covers_stalled_response_body_after_headers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let _ = socket.read(&mut request).await.unwrap();
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1024\r\n\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
    });
    let started = tokio::time::Instant::now();
    let result = OllamaClient::new(format!("http://{address}"))
        .native_chat(&request(), chrono::Utc::now().timestamp_millis() + 150)
        .await;
    assert!(result.unwrap_err().to_string().contains("native_timeout"));
    assert!(started.elapsed() < Duration::from_secs(2));
    server.abort();
}
#[tokio::test]
async fn expired_or_oversized_native_requests_never_reach_upstream() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let client = OllamaClient::new(server.uri());
    assert!(
        client
            .native_chat(&request(), chrono::Utc::now().timestamp_millis() - 1)
            .await
            .is_err()
    );
    let mut body = request();
    body.body["huge"] = json!("x".repeat(MAX_NATIVE_BODY_BYTES));
    assert!(
        client
            .native_chat(&body, chrono::Utc::now().timestamp_millis() + 2000)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn completion_retry_reuploads_the_same_result_without_new_inference() {
    let ollama = MockServer::start().await;
    let api = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_json(&request().body))
        .respond_with(ResponseTemplate::new(200).set_body_json(success()))
        .expect(1)
        .mount(&ollama)
        .await;
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let task_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    Mock::given(method("POST")).and(path(format!("/node/v1/tasks/{task_id}/complete")))
        .respond_with(move|_req:&wiremock::Request| {
            if c.fetch_add(1,Ordering::SeqCst)==0 {ResponseTemplate::new(503)} else {
                ResponseTemplate::new(200).set_body_json(json!({"protocol_version":"node.v1","action":"succeeded","task_status":"succeeded","node_status":"online","server_failure_count":0,"failure_threshold":3}))
            }
        }).expect(2).mount(&api).await;
    let now = chrono::Utc::now().timestamp_millis();
    let task = NodeTaskEnvelope {
        task_id,
        lease_id: Uuid::new_v4(),
        model: "node:literal".into(),
        deadline_unix_ms: now + 15000,
        complete_grace_until_unix_ms: now + 20000,
        payload: NodeTaskPayload {
            request_id: Uuid::new_v4(),
            chat: None,
            image_generation: None,
            image_edit: None,
            native: Some(request()),
        },
    };
    let session = SessionData {
        node_id,
        session_id,
        session_token: "local-test-session".into(),
        capabilities: NodeCapabilities {
            runtime: "ollama".into(),
            models: vec![NodeModelCapability {
                model: "node:literal".into(),
            }],
            native_operations: vec![NodeNativeOperation::Chat],
            native_profiles: vec![
                NativeModelProfile::plain_chat("node:literal")
                    .with_features(vec![NativeFeature::Tools, NativeFeature::Vision]),
            ],
            runtime_version: Some("test".into()),
        },
        poll_timeout_secs: 1,
    };
    let executor = TaskExecutor::new(
        Arc::new(KeyComputeClient::new_with_token(
            api.uri(),
            "local-test-session".into(),
        )),
        Arc::new(OllamaClient::new(ollama.uri())),
        session,
        Arc::new(AtomicBool::new(false)),
    );
    executor.execute(task).await;
    let uploads = api.received_requests().await.unwrap();
    assert_eq!(uploads.len(), 2);
    assert_eq!(uploads[0].body, uploads[1].body);
    let uploaded: Value = serde_json::from_slice(&uploads[0].body).unwrap();
    assert_eq!(uploaded["result"]["response"]["body"], success());
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}
