//! KeyCompute API 客户端
//!
//! 负责与 KeyCompute 服务端通信，包括注册、心跳、轮询和任务完成。

use crate::error::{NetworkResult, NodeTokenError};
use crate::protocol::types::{
    NodeHeartbeatRequest, NodeHeartbeatResponse, NodePollRequest, NodePollResponse,
    NodeRegisterRequest, NodeRegisterResponse, NodeTaskCompleteRequest, NodeTaskCompleteResponse,
};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

/// KeyCompute API 客户端
pub struct KeyComputeClient {
    /// 服务端基础 URL
    base_url: String,
    /// HTTP 客户端（连接池）
    http_client: Client,
    /// Session token（注册后设置，使用 RwLock 支持内部可变性）
    session_token: Arc<RwLock<Option<String>>>,
}

impl KeyComputeClient {
    /// 创建新的 KeyCompute 客户端
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .expect("Failed to create HTTP client");
        Self {
            base_url,
            http_client,
            session_token: Arc::new(RwLock::new(None)),
        }
    }

    pub fn new_with_token(base_url: impl Into<String>, token: String) -> Self {
        let base_url = base_url.into();
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .expect("Failed to create HTTP client");
        Self {
            base_url,
            http_client,
            session_token: Arc::new(RwLock::new(Some(token))),
        }
    }

    pub async fn set_session_token(&self, token: String) {
        let mut token_guard = self.session_token.write().await;
        *token_guard = Some(token);
    }

    pub async fn get_session_token(&self) -> Option<String> {
        let token_guard = self.session_token.read().await;
        token_guard.clone()
    }

    pub async fn register(
        &self,
        request: &NodeRegisterRequest,
    ) -> NetworkResult<NodeRegisterResponse> {
        let url = format!("{}/node/v1/register", self.base_url);
        info!("Registering node with server");
        let response = self
            .http_client
            .post(&url)
            .json(request)
            .send()
            .await
            .map_err(|e| {
                error!("Register request failed: {}", e);
                NodeTokenError::Network(e)
            })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(NodeTokenError::HttpError {
                status,
                message: format!("Register failed: {}", body),
            });
        }
        let response_body: NodeRegisterResponse =
            response.json().await.map_err(NodeTokenError::Network)?;
        info!(
            "Node registered successfully: node_id={}",
            response_body.node_id
        );
        Ok(response_body)
    }

    pub async fn heartbeat(
        &self,
        request: &NodeHeartbeatRequest,
    ) -> NetworkResult<NodeHeartbeatResponse> {
        let url = format!("{}/node/v1/heartbeat", self.base_url);
        let response = self
            .http_client
            .post(&url)
            .json(request)
            .header(
                "Authorization",
                format!("Bearer {}", self.require_session_token().await?),
            )
            .send()
            .await
            .map_err(|e| {
                error!("Heartbeat request failed: {}", e);
                NodeTokenError::Network(e)
            })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(NodeTokenError::HttpError {
                status,
                message: format!("Heartbeat failed: {}", body),
            });
        }
        let response_body: NodeHeartbeatResponse =
            response.json().await.map_err(NodeTokenError::Network)?;
        Ok(response_body)
    }

    pub async fn poll(&self, request: &NodePollRequest) -> NetworkResult<NodePollResponse> {
        let url = format!("{}/node/v1/tasks/poll", self.base_url);
        let response = self
            .http_client
            .post(&url)
            .json(request)
            .header(
                "Authorization",
                format!("Bearer {}", self.require_session_token().await?),
            )
            .send()
            .await
            .map_err(NodeTokenError::Network)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(NodeTokenError::HttpError {
                status,
                message: format!("Poll failed: {}", body),
            });
        }
        let response_body: NodePollResponse =
            response.json().await.map_err(NodeTokenError::Network)?;
        Ok(response_body)
    }

    pub async fn complete(
        &self,
        task_id: uuid::Uuid,
        request: &NodeTaskCompleteRequest,
    ) -> NetworkResult<NodeTaskCompleteResponse> {
        let url = format!("{}/node/v1/tasks/{}/complete", self.base_url, task_id);
        let response = self
            .http_client
            .post(&url)
            .json(request)
            .header(
                "Authorization",
                format!("Bearer {}", self.require_session_token().await?),
            )
            .send()
            .await
            .map_err(NodeTokenError::Network)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(NodeTokenError::HttpError {
                status,
                message: format!("Complete failed: {}", body),
            });
        }
        let response_body: NodeTaskCompleteResponse =
            response.json().await.map_err(NodeTokenError::Network)?;
        Ok(response_body)
    }

    async fn require_session_token(&self) -> Result<String, NodeTokenError> {
        let token_guard = self.session_token.read().await;
        token_guard
            .clone()
            .ok_or_else(|| NodeTokenError::InvalidSession)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_client_creation() {
        let client = KeyComputeClient::new("http://localhost:3000");
        assert_eq!(client.base_url, "http://localhost:3000");
        assert!(client.get_session_token().await.is_none());
    }

    #[tokio::test]
    async fn test_set_session_token() {
        let client = KeyComputeClient::new("http://localhost:3000");
        client.set_session_token("test-token".to_string()).await;
        assert_eq!(
            client.get_session_token().await,
            Some("test-token".to_string())
        );
    }

    #[tokio::test]
    async fn test_require_session_token() {
        let client = KeyComputeClient::new("http://localhost:3000");
        assert!(client.require_session_token().await.is_err());
        client.set_session_token("test-token".to_string()).await;
        let token = client.require_session_token().await.unwrap();
        assert_eq!(token, "test-token");
    }

    #[tokio::test]
    async fn test_register_success() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/node/v1/register"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "node.v1",
                "node_id": "00000000-0000-0000-0000-000000000001",
                "session_id": "00000000-0000-0000-0000-000000000002",
                "session_token": "test-session-token",
                "heartbeat_interval_secs": 30,
                "poll_timeout_secs": 10
            })))
            .mount(&mock_server)
            .await;
        let client = KeyComputeClient::new(mock_server.uri());
        let request = crate::protocol::types::NodeRegisterRequest {
            protocol_version: "node.v1".to_string(),
            client_instance_id: "test-instance".to_string(),
            display_name: "Test Node".to_string(),
            registration_token: "test-token".to_string(),
            capabilities: crate::protocol::types::NodeCapabilities {
                runtime: "ollama".to_string(),
                models: vec![crate::protocol::types::NodeModelCapability {
                    model: "deepseek-chat".to_string(),
                }],
            },
        };
        let response = client.register(&request).await.unwrap();
        assert_eq!(response.session_token, "test-session-token");
    }

    #[tokio::test]
    async fn test_register_http_error() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/node/v1/register"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&mock_server)
            .await;
        let client = KeyComputeClient::new(mock_server.uri());
        let request = crate::protocol::types::NodeRegisterRequest {
            protocol_version: "node.v1".to_string(),
            client_instance_id: "test-instance".to_string(),
            display_name: "Test Node".to_string(),
            registration_token: "test-token".to_string(),
            capabilities: crate::protocol::types::NodeCapabilities {
                runtime: "ollama".to_string(),
                models: vec![],
            },
        };
        assert!(client.register(&request).await.is_err());
    }

    #[tokio::test]
    async fn test_heartbeat_success() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/node/v1/heartbeat"))
            .and(header("Authorization", "Bearer test-session-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "node.v1", "accepted": true, "node_status": "online",
                "server_failure_count": 0, "failure_threshold": 3
            })))
            .mount(&mock_server)
            .await;
        let client =
            KeyComputeClient::new_with_token(mock_server.uri(), "test-session-token".to_string());
        let request = crate::protocol::types::NodeHeartbeatRequest {
            protocol_version: "node.v1".to_string(),
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            accepted_models: vec!["deepseek-chat".to_string()],
        };
        let response = client.heartbeat(&request).await.unwrap();
        assert!(response.accepted);
    }

    #[tokio::test]
    async fn test_heartbeat_missing_token() {
        let client = KeyComputeClient::new("http://localhost:3000");
        let request = crate::protocol::types::NodeHeartbeatRequest {
            protocol_version: "node.v1".to_string(),
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            accepted_models: vec![],
        };
        assert!(client.heartbeat(&request).await.is_err());
    }

    #[tokio::test]
    async fn test_poll_with_task() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/node/v1/tasks/poll"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "node.v1",
                "task": {
                    "task_id": "00000000-0000-0000-0000-000000000003",
                    "lease_id": "00000000-0000-0000-0000-000000000004",
                    "model": "deepseek-chat",
                    "deadline_unix_ms": 9999999999999_i64,
                    "complete_grace_until_unix_ms": 9999999999999_i64,
                    "payload": {
                        "request_id": "00000000-0000-0000-0000-000000000005",
                        "chat": {
                            "model": "deepseek-chat",
                            "messages": [{"role": "user", "content": "Hello"}],
                            "stream": false
                        }
                    }
                },
                "retry_after_ms": null
            })))
            .mount(&mock_server)
            .await;
        let client = KeyComputeClient::new_with_token(mock_server.uri(), "test-token".to_string());
        let request = crate::protocol::types::NodePollRequest {
            protocol_version: "node.v1".to_string(),
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
        };
        let response = client.poll(&request).await.unwrap();
        assert!(response.task.is_some());
    }

    #[tokio::test]
    async fn test_poll_no_task() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/node/v1/tasks/poll"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocol_version": "node.v1", "task": null, "retry_after_ms": 1000
            })))
            .mount(&mock_server)
            .await;
        let client = KeyComputeClient::new_with_token(mock_server.uri(), "test-token".to_string());
        let request = crate::protocol::types::NodePollRequest {
            protocol_version: "node.v1".to_string(),
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
        };
        let response = client.poll(&request).await.unwrap();
        assert!(response.task.is_none());
    }

    #[tokio::test]
    async fn test_complete_success() {
        let mock_server = MockServer::start().await;
        let task_id = uuid::Uuid::new_v4();
        Mock::given(method("POST"))
            .and(path(format!("/node/v1/tasks/{}/complete", task_id)))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "action": "succeeded", "task_status": "succeeded", "node_status": "online",
                "server_failure_count": 0, "failure_threshold": 3
            })))
            .mount(&mock_server)
            .await;
        let client = KeyComputeClient::new_with_token(mock_server.uri(), "test-token".to_string());
        let request = crate::protocol::types::NodeTaskCompleteRequest {
            protocol_version: "node.v1".to_string(),
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            task_id,
            lease_id: uuid::Uuid::new_v4(),
            result: crate::protocol::types::NodeTaskResult::Succeeded {
                response: crate::protocol::types::ChatCompletionResponse {
                    id: "resp-001".to_string(),
                    object: "chat.completion".to_string(),
                    created: 1234567890,
                    model: "deepseek-chat".to_string(),
                    choices: vec![crate::protocol::types::CompletionChoice {
                        index: 0,
                        message: crate::protocol::types::ChoiceMessage {
                            role: "assistant".to_string(),
                            content: "Hello!".to_string(),
                        },
                        finish_reason: Some("stop".to_string()),
                    }],
                    usage: crate::protocol::types::Usage {
                        prompt_tokens: 10,
                        completion_tokens: 20,
                        total_tokens: 30,
                    },
                },
            },
        };
        let response = client.complete(task_id, &request).await.unwrap();
        assert_eq!(
            response.action,
            crate::protocol::types::NodeTaskCompleteAction::Succeeded
        );
    }
}
