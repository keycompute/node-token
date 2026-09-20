//! Phase-4 native stream integration contracts. Loopback upstreams only.
use node_token::{
    client::{api::KeyComputeClient, ollama::OllamaClient},
    protocol::{
        node_capability::{NativeFeature, NativeModelProfile},
        node_native::{NodeNativeOperation, NodeNativeRequest},
        types::{
            NodeCapabilities, NodeLeaseId, NodeModelCapability, NodeNativeStreamEvent,
            NodeSessionId, NodeTaskEnvelope, NodeTaskPayload, NodeTaskResult,
            NodeTaskStreamEventRequest, NodeTaskStreamEventResponse,
        },
    },
    runtime::executor::TaskExecutor,
    storage::SessionData,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const MODEL: &str = "node:literal";
const TOKEN: &str = "local-test-session";

fn native_request(operation: NodeNativeOperation) -> NodeNativeRequest {
    let fixture = match operation {
        NodeNativeOperation::Chat => include_str!("fixtures/node-native-chat-v1.json"),
        NodeNativeOperation::Messages => include_str!("fixtures/node-native-messages-v1.json"),
        NodeNativeOperation::Responses => include_str!("fixtures/node-native-responses-v1.json"),
    };
    let mut request: NodeNativeRequest = serde_json::from_str(fixture).unwrap();
    request.body["stream"] = Value::Bool(true);
    request
}

fn stream_body(operation: NodeNativeOperation) -> String {
    match operation {
        NodeNativeOperation::Chat => concat!(
            "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"model\":\"node:literal\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你\"},\"finish_reason\":null}]}\r\n\r\n",
            "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"model\":\"node:literal\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\n",
            "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"model\":\"node:literal\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\r\n\r\n",
            "data: [DONE]\r\n\r\n"
        )
        .to_string(),
        NodeNativeOperation::Messages => concat!(
            "event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"node:literal\",\"content\":[],\"usage\":{\"input_tokens\":2}}}\r\n\r\n",
            "event: content_block_delta\r\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"你\"}}\r\n\r\n",
            "event: message_delta\r\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\r\n\r\n",
            "event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\n\r\n"
        )
        .to_string(),
        NodeNativeOperation::Responses => concat!(
            "event: response.created\r\ndata: {\"type\":\"response.created\",\"response\":{\"object\":\"response\",\"id\":\"r\",\"status\":\"in_progress\",\"model\":\"node:literal\",\"output\":[]}}\r\n\r\n",
            "event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"你\"}\r\n\r\n",
            "event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"object\":\"response\",\"id\":\"r\",\"status\":\"completed\",\"model\":\"node:literal\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\r\n\r\n"
        )
        .to_string(),
    }
}

fn fragments(body: &str) -> Vec<Vec<u8>> {
    body.as_bytes().chunks(3).map(ToOwned::to_owned).collect()
}

async fn consume_request(socket: &mut TcpStream) {
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end;
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "upstream request ended before headers");
        request.extend_from_slice(&chunk[..read]);
        if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            header_end = end + 4;
            break;
        }
    }
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let content_length = header_text
        .lines()
        .find_map(|line| {
            line.strip_prefix("Content-Length:")
                .or_else(|| line.strip_prefix("content-length:"))
        })
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while request.len() < header_end + content_length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0, "upstream request ended before body");
        request.extend_from_slice(&chunk[..read]);
    }
}

async fn spawn_upstream(
    status: u16,
    content_type: &str,
    chunks: Vec<Vec<u8>>,
    hold_after_body: bool,
) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let seen = connections.clone();
    let content_type = content_type.to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        seen.fetch_add(1, Ordering::SeqCst);
        consume_request(&mut socket).await;
        let reason = if status == 200 {
            "OK"
        } else {
            "Too Many Requests"
        };
        let header = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nConnection: keep-alive\r\n\r\n"
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        for chunk in chunks {
            socket.write_all(&chunk).await.unwrap();
            socket.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        if hold_after_body {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
    (format!("http://{address}"), connections, server)
}

#[derive(Clone)]
struct AckResponder {
    calls: Arc<AtomicUsize>,
    cancel_on_start: bool,
    transient: bool,
}

impl Respond for AckResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let event: NodeTaskStreamEventRequest = serde_json::from_slice(&request.body).unwrap();
        if self.transient && call < 2 {
            return ResponseTemplate::new(if call == 0 { 503 } else { 429 });
        }
        let terminal =
            self.cancel_on_start && matches!(event.event, NodeNativeStreamEvent::Start { .. });
        ResponseTemplate::new(200).set_body_json(NodeTaskStreamEventResponse {
            accepted: true,
            next_seq: event.seq + 1,
            retry_after_ms: None,
            terminal,
        })
    }
}

async fn mount_acks(api: &MockServer, responder: AckResponder) {
    Mock::given(method("POST"))
        .and(path(
            "/node/v1/tasks/00000000-0000-0000-0000-000000000001/events",
        ))
        .respond_with(responder)
        .mount(api)
        .await;
}

fn executor(
    api: &MockServer,
    ollama_url: String,
    operation: NodeNativeOperation,
) -> (TaskExecutor, NodeTaskEnvelope, Uuid, Uuid) {
    let task_id = Uuid::from_u128(1);
    let now = chrono::Utc::now().timestamp_millis();
    let request = native_request(operation);
    let session_id = NodeSessionId::new_v4();
    let node_id = Uuid::new_v4();
    let features = vec![
        NativeFeature::Sse,
        NativeFeature::Tools,
        NativeFeature::Vision,
        NativeFeature::Thinking,
        NativeFeature::StructuredOutput,
    ];
    let profile = NativeModelProfile::for_operation(MODEL, operation).with_features(features);
    let session = SessionData {
        node_id,
        session_id,
        session_token: TOKEN.to_string(),
        capabilities: NodeCapabilities {
            runtime: "ollama".into(),
            native_operations: vec![operation],
            models: vec![NodeModelCapability {
                model: MODEL.into(),
            }],
            native_profiles: vec![profile],
            runtime_version: Some("0.14.0".into()),
        },
        poll_timeout_secs: 1,
    };
    let task = NodeTaskEnvelope {
        task_id,
        lease_id: NodeLeaseId::new_v4(),
        model: MODEL.into(),
        deadline_unix_ms: now + 8_000,
        complete_grace_until_unix_ms: now + 12_000,
        payload: NodeTaskPayload {
            request_id: Uuid::new_v4(),
            chat: None,
            native: Some(request),
            image_generation: None,
            image_edit: None,
        },
    };
    let executor = TaskExecutor::new(
        Arc::new(KeyComputeClient::new_with_token(api.uri(), TOKEN.into())),
        Arc::new(OllamaClient::new(ollama_url)),
        session,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    (executor, task, node_id, session_id)
}

async fn received_events(api: &MockServer) -> Vec<NodeTaskStreamEventRequest> {
    api.received_requests()
        .await
        .unwrap()
        .into_iter()
        .map(|request| serde_json::from_slice(&request.body).unwrap())
        .collect()
}

#[tokio::test]
async fn all_native_operations_preserve_frames_usage_and_finish_before_eof() {
    for operation in [
        NodeNativeOperation::Chat,
        NodeNativeOperation::Messages,
        NodeNativeOperation::Responses,
    ] {
        let api = MockServer::start().await;
        mount_acks(
            &api,
            AckResponder {
                calls: Arc::new(AtomicUsize::new(0)),
                cancel_on_start: false,
                transient: false,
            },
        )
        .await;
        let (ollama, _, upstream) = spawn_upstream(
            200,
            "text/event-stream; charset=utf-8",
            fragments(&stream_body(operation)),
            true,
        )
        .await;
        let (executor, task, node_id, session_id) = executor(&api, ollama, operation);
        let started = tokio::time::Instant::now();
        let result = executor
            .execute_task_with_events(
                &task,
                &KeyComputeClient::new_with_token(api.uri(), TOKEN.into()),
                node_id,
                session_id,
            )
            .await
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "held TCP delayed completion"
        );
        upstream.abort();
        let events = received_events(&api).await;
        assert!(
            matches!(result, NodeTaskResult::NativeStreamSucceeded { .. }),
            "{operation:?}: {result:?}"
        );
        assert!(matches!(
            events.first().unwrap().event,
            NodeNativeStreamEvent::Start { .. }
        ));
        assert!(matches!(
            events.last().unwrap().event,
            NodeNativeStreamEvent::Terminal { .. }
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.event, NodeNativeStreamEvent::Failed { .. }))
        );
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.seq, index as u64);
        }
        let start_headers = match &events[0].event {
            NodeNativeStreamEvent::Start { headers, .. } => headers,
            _ => unreachable!(),
        };
        assert!(
            start_headers
                .iter()
                .any(|(name, value)| name == "content-type"
                    && value.starts_with("text/event-stream"))
        );
        if operation == NodeNativeOperation::Chat {
            let terminal_data = events
                .iter()
                .filter_map(|event| match &event.event {
                    NodeNativeStreamEvent::Data { frame } => Some(frame),
                    _ => None,
                })
                .next_back()
                .unwrap();
            assert!(terminal_data.contains("[DONE]"));
        }
    }
}

#[tokio::test]
async fn non_2xx_is_forwarded_without_start_and_non_sse_200_is_rejected() {
    let api = MockServer::start().await;
    let error_body = json!({"error":{"code":"overloaded","message":"original"}});
    let (ollama, _, upstream) = spawn_upstream(
        429,
        "application/json",
        vec![serde_json::to_vec(&error_body).unwrap()],
        false,
    )
    .await;
    let (task_executor, task, node_id, session_id) =
        executor(&api, ollama, NodeNativeOperation::Chat);
    let client = KeyComputeClient::new_with_token(api.uri(), TOKEN.into());
    let result = task_executor
        .execute_task_with_events(&task, &client, node_id, session_id)
        .await
        .unwrap();
    upstream.abort();
    match result {
        NodeTaskResult::NativeSucceeded { response } => {
            assert_eq!(response.status, 429);
            assert_eq!(response.body, error_body);
            assert!(
                response
                    .headers
                    .iter()
                    .all(|(name, _)| name != "set-cookie")
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(api.received_requests().await.unwrap().is_empty());

    let api = MockServer::start().await;
    let (ollama, _, upstream) =
        spawn_upstream(200, "application/json", vec![b"{}".to_vec()], false).await;
    let (executor, task, node_id, session_id) = executor(&api, ollama, NodeNativeOperation::Chat);
    let error = executor
        .execute_task_with_events(
            &task,
            &KeyComputeClient::new_with_token(api.uri(), TOKEN.into()),
            node_id,
            session_id,
        )
        .await
        .unwrap_err();
    upstream.abort();
    assert!(
        error
            .to_string()
            .contains("native_stream_content_type_required")
    );
    assert!(api.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_and_truncated_streams_emit_failed_without_complete() {
    for body in [
        b"data: {broken}\r\n\r\n".to_vec(),
        b"data: {\"id\":\"truncated\"}".to_vec(),
    ] {
        let api = MockServer::start().await;
        mount_acks(
            &api,
            AckResponder {
                calls: Arc::new(AtomicUsize::new(0)),
                cancel_on_start: false,
                transient: false,
            },
        )
        .await;
        let (ollama, _, upstream) =
            spawn_upstream(200, "text/event-stream", vec![body], false).await;
        let (executor, task, node_id, session_id) =
            executor(&api, ollama, NodeNativeOperation::Chat);
        let result = executor
            .execute_task_with_events(
                &task,
                &KeyComputeClient::new_with_token(api.uri(), TOKEN.into()),
                node_id,
                session_id,
            )
            .await
            .unwrap();
        upstream.abort();
        assert!(matches!(result, NodeTaskResult::Failed { .. }));
        let events = received_events(&api).await;
        assert!(matches!(
            events[0].event,
            NodeNativeStreamEvent::Start { .. }
        ));
        assert!(matches!(
            events.last().unwrap().event,
            NodeNativeStreamEvent::Failed { .. }
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.event, NodeNativeStreamEvent::Terminal { .. }))
        );
    }
}

#[tokio::test]
async fn cancellation_ack_drops_upstream_without_a_second_inference_request() {
    let api = MockServer::start().await;
    mount_acks(
        &api,
        AckResponder {
            calls: Arc::new(AtomicUsize::new(0)),
            cancel_on_start: true,
            transient: false,
        },
    )
    .await;
    let (ollama, connections, upstream) = spawn_upstream(
        200,
        "text/event-stream",
        vec![b"data: {}\n\n".to_vec()],
        true,
    )
    .await;
    let (executor, task, node_id, session_id) = executor(&api, ollama, NodeNativeOperation::Chat);
    let error = executor
        .execute_task_with_events(
            &task,
            &KeyComputeClient::new_with_token(api.uri(), TOKEN.into()),
            node_id,
            session_id,
        )
        .await
        .unwrap_err();
    upstream.abort();
    assert!(error.to_string().contains("native_stream_canceled"));
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert_eq!(api.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn transient_event_delivery_retries_the_same_authenticated_sequence() {
    let api = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    mount_acks(
        &api,
        AckResponder {
            calls: calls.clone(),
            cancel_on_start: false,
            transient: true,
        },
    )
    .await;
    let task_id = Uuid::from_u128(1);
    let request = NodeTaskStreamEventRequest {
        protocol_version: "node.v1".into(),
        node_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        task_id,
        lease_id: Uuid::new_v4(),
        seq: 7,
        event: NodeNativeStreamEvent::Data {
            frame: "data: immutable\n\n".into(),
        },
    };
    let client = KeyComputeClient::new_with_token(api.uri(), TOKEN.into());
    let ack = client.stream_event(task_id, &request).await.unwrap();
    assert_eq!(ack.next_seq, 8);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let received = api.received_requests().await.unwrap();
    assert_eq!(received.len(), 3);
    assert!(received.windows(2).all(|pair| pair[0].body == pair[1].body));
    assert!(
        received
            .iter()
            .all(|request| request.headers.iter().any(|(name, values)| {
                name.as_str() == "authorization"
                    && values
                        .iter()
                        .any(|value| value.as_str() == "Bearer local-test-session")
            }))
    );
}

#[test]
fn native_payload_debug_is_redacted() {
    let request = native_request(NodeNativeOperation::Chat);
    assert!(!format!("{request:?}").contains("unknown_extension"));
    let result = node_token::protocol::node_native::NodeNativeHttpResult {
        status: 500,
        headers: vec![],
        body: json!({"secret":"do-not-log"}),
    };
    assert!(!format!("{result:?}").contains("do-not-log"));
}
