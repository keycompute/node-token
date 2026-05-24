//! Ollama HTTP 客户端
//!
//! 负责与本地 Ollama 实例通信，包括模型列表查询和 chat 调用。

use crate::error::{NodeTokenError, OllamaResult};
use crate::protocol::ollama::{
    OllamaChatRequest, OllamaChatResponse, OllamaMessage, OllamaModelListResponse,
};
use crate::protocol::types::{
    ChatCompletionRequest, ChatCompletionResponse, CompletionChoice, MessageContent, Usage,
};
use reqwest::Client;
use tracing::{debug, error, info};

/// Ollama HTTP 客户端
#[allow(dead_code)] // 在后续阶段使用
pub struct OllamaClient {
    /// Ollama 基础 URL
    base_url: String,
    /// HTTP 客户端（连接池）
    http_client: Client,
}

impl OllamaClient {
    /// 创建新的 Ollama 客户端
    #[allow(dead_code)] // 在后续阶段使用
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(600)) // 10 分钟超时（模型推理可能较慢）
            .build()
            .expect("Failed to create Ollama HTTP client");

        Self {
            base_url,
            http_client,
        }
    }

    /// 获取本地 Ollama 模型列表
    #[allow(dead_code)] // 在后续阶段使用
    pub async fn list_models(&self) -> OllamaResult<Vec<String>> {
        let url = format!("{}/api/tags", self.base_url);

        debug!("Fetching Ollama model list");

        let response = self.http_client.get(&url).send().await.map_err(|e| {
            error!("Failed to fetch Ollama models: {}", e);
            NodeTokenError::Ollama(format!("Failed to fetch models: {}", e))
        })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            error!("Failed to list models with status {}: {}", status, body);
            return Err(NodeTokenError::Ollama(format!(
                "Failed to list models: HTTP {}",
                status
            )));
        }

        let model_list: OllamaModelListResponse = response.json().await.map_err(|e| {
            error!("Failed to parse model list response: {}", e);
            NodeTokenError::Ollama(format!("Failed to parse model list: {}", e))
        })?;

        let models: Vec<String> = model_list.models.iter().map(|m| m.name.clone()).collect();

        info!("Found {} Ollama models: {:?}", models.len(), models);
        Ok(models)
    }

    /// 调用 Ollama chat API（非流式）
    #[allow(dead_code)] // 在后续阶段使用
    pub async fn chat(&self, request: &OllamaChatRequest) -> OllamaResult<OllamaChatResponse> {
        let url = format!("{}/api/chat", self.base_url);

        debug!("Calling Ollama chat API for model: {}", request.model);

        let response = self
            .http_client
            .post(&url)
            .json(request)
            .send()
            .await
            .map_err(|e| {
                error!("Ollama chat request failed: {}", e);
                NodeTokenError::Ollama(format!("Chat request failed: {}", e))
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            error!("Ollama chat failed with status {}: {}", status, body);
            // 用 HttpError 保留 status code, 让 executor 据此判断 is_client_error (B1 修复支撑)
            return Err(NodeTokenError::HttpError {
                status,
                message: body,
            });
        }

        let chat_response: OllamaChatResponse = response.json().await.map_err(|e| {
            error!("Failed to parse Ollama chat response: {}", e);
            NodeTokenError::Ollama(format!("Failed to parse chat response: {}", e))
        })?;

        debug!(
            "Ollama chat completed: model={}, tokens={}/{}",
            chat_response.model, chat_response.prompt_eval_count, chat_response.eval_count
        );
        Ok(chat_response)
    }

    /// 将 ChatCompletionRequest 转换为 OllamaChatRequest
    pub fn chat_request_to_ollama(request: &ChatCompletionRequest) -> OllamaChatRequest {
        OllamaChatRequest {
            model: request.model.clone(),
            messages: request
                .messages
                .iter()
                .map(|m| OllamaMessage {
                    role: m.role.as_str().to_string(),
                    content: m.content.clone(),
                })
                .collect(),
            stream: false, // MVP 只支持非流式
        }
    }

    /// 将 OllamaChatResponse 转换为 ChatCompletionResponse
    pub fn ollama_response_to_chat(
        response: &OllamaChatResponse,
        model: &str,
    ) -> ChatCompletionResponse {
        ChatCompletionResponse {
            id: format!("ollama-{}", uuid::Uuid::new_v4()),
            object: "chat.completion".to_string(),
            created: chrono::Utc::now().timestamp(),
            model: model.to_string(),
            choices: vec![CompletionChoice {
                index: 0,
                message: MessageContent {
                    role: response.message.role.clone(),
                    content: response.message.content.clone(),
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: Usage {
                prompt_tokens: response.prompt_eval_count,
                completion_tokens: response.eval_count,
                total_tokens: response.prompt_eval_count + response.eval_count,
            },
        }
    }

    /// 便捷方法：直接调用 chat 并转换为 ChatCompletionResponse
    #[allow(dead_code)] // 在后续阶段使用
    pub async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> OllamaResult<ChatCompletionResponse> {
        let ollama_request = Self::chat_request_to_ollama(request);
        let ollama_response = self.chat(&ollama_request).await?;
        Ok(Self::ollama_response_to_chat(
            &ollama_response,
            &request.model,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{Message, MessageRole};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_client_creation() {
        let client = OllamaClient::new("http://localhost:11434");
        assert_eq!(client.base_url, "http://localhost:11434");
    }

    #[test]
    fn test_chat_request_conversion() {
        let request = ChatCompletionRequest::new(
            "deepseek-chat",
            vec![
                Message::system("You are a helpful assistant"),
                Message::user("Hello"),
            ],
        );

        let ollama_request = OllamaClient::chat_request_to_ollama(&request);

        assert_eq!(ollama_request.model, "deepseek-chat");
        assert_eq!(ollama_request.messages.len(), 2);
        assert_eq!(ollama_request.messages[0].role, "system");
        assert_eq!(ollama_request.messages[1].role, "user");
        assert!(!ollama_request.stream);
    }

    #[test]
    fn test_ollama_response_conversion() {
        let ollama_response = OllamaChatResponse {
            model: "deepseek-chat".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            message: OllamaMessage {
                role: "assistant".to_string(),
                content: "Hello!".to_string(),
            },
            done: true,
            total_duration: 1000000000,
            load_duration: 500000000,
            prompt_eval_count: 10,
            eval_count: 20,
        };

        let chat_response =
            OllamaClient::ollama_response_to_chat(&ollama_response, "deepseek-chat");

        assert_eq!(chat_response.model, "deepseek-chat");
        assert_eq!(chat_response.choices.len(), 1);
        assert_eq!(chat_response.choices[0].message.content, "Hello!");
        assert_eq!(chat_response.usage.prompt_tokens, 10);
        assert_eq!(chat_response.usage.completion_tokens, 20);
        assert_eq!(chat_response.usage.total_tokens, 30);
    }

    #[test]
    fn test_message_role_conversion() {
        assert_eq!(MessageRole::System.as_str(), "system");
        assert_eq!(MessageRole::User.as_str(), "user");
        assert_eq!(MessageRole::Assistant.as_str(), "assistant");
        assert_eq!(MessageRole::Tool.as_str(), "tool");
    }

    #[tokio::test]
    /// 测试 Ollama 模型列表获取成功场景
    async fn test_list_models_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/tags"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [
                    {"name": "deepseek-chat:latest", "size": 4000000000_u64},
                    {"name": "llama3:latest", "size": 3800000000_u64}
                ]
            })))
            .mount(&mock_server)
            .await;

        let client = OllamaClient::new(mock_server.uri());
        let models = client.list_models().await.unwrap();

        assert_eq!(models.len(), 2);
        assert_eq!(models[0], "deepseek-chat:latest");
        assert_eq!(models[1], "llama3:latest");
    }

    #[tokio::test]
    /// 测试 Ollama Chat 请求成功场景
    async fn test_chat_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/chat"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "deepseek-chat",
                "created_at": "2024-01-01T00:00:00Z",
                "message": {
                    "role": "assistant",
                    "content": "Hello! How can I help you?"
                },
                "done": true,
                "total_duration": 1000000000,
                "prompt_eval_count": 10,
                "eval_count": 20
            })))
            .mount(&mock_server)
            .await;

        let client = OllamaClient::new(mock_server.uri());
        let request = OllamaChatRequest {
            model: "deepseek-chat".to_string(),
            messages: vec![OllamaMessage::new("user", "Hello")],
            stream: false,
        };

        let response = client.chat(&request).await.unwrap();

        assert_eq!(response.model, "deepseek-chat");
        assert_eq!(response.message.role, "assistant");
        assert_eq!(response.message.content, "Hello! How can I help you?");
        assert!(response.done);
        assert_eq!(response.prompt_eval_count, 10);
        assert_eq!(response.eval_count, 20);
    }

    #[tokio::test]
    /// 测试 Ollama Chat 请求失败场景（HTTP 500）
    async fn test_chat_http_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/chat"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&mock_server)
            .await;

        let client = OllamaClient::new(mock_server.uri());
        let request = OllamaChatRequest {
            model: "deepseek-chat".to_string(),
            messages: vec![OllamaMessage::new("user", "Hello")],
            stream: false,
        };

        let result = client.chat(&request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    /// 测试 Ollama 未启动场景（连接失败）
    async fn test_ollama_not_started() {
        // 使用一个不会被监听的端口，模拟 Ollama 未启动
        let client = OllamaClient::new("http://localhost:19999");

        let result = client.list_models().await;
        assert!(result.is_err());

        // 验证错误类型是 Ollama 错误
        match result.unwrap_err() {
            NodeTokenError::Ollama(msg) => {
                // 打印实际错误消息以便调试
                println!("Actual error message: {}", msg);
                // 只要是 Ollama 错误就认为测试通过
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected Ollama error"),
        }
    }
}
