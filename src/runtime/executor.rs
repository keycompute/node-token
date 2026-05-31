//! 任务执行器
//!
//! 负责执行从服务端领取的任务，调用本机 Ollama 完成推理，
//! 并将结果提交回服务端。

use std::sync::Arc;

use chrono::Utc;
use tokio_retry::Retry;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{debug, error, info, warn};

use crate::client::{KeyComputeClient, OllamaClient};
use crate::error::NodeTokenError;
use crate::protocol::types::{
    ChatCompletionResponse, ImageData, ImageGenerationResponse, NodeTaskCompleteRequest,
    NodeTaskCompleteResponse, NodeTaskEnvelope, NodeTaskResult,
};
use crate::storage::SessionData;

/// 结果类型别名
type Result<T> = std::result::Result<T, NodeTokenError>;

/// 图片生成/编辑操作类型
#[derive(Debug, Clone, Copy)]
enum GenerateOp {
    ImageGeneration,
    ImageEdit,
}

impl std::fmt::Display for GenerateOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenerateOp::ImageGeneration => write!(f, "image generation"),
            GenerateOp::ImageEdit => write!(f, "image edit"),
        }
    }
}

/// 任务执行器
///
/// 执行领取的任务，调用 Ollama 推理，提交结果。
pub struct TaskExecutor {
    /// KeyCompute HTTP 客户端
    client: Arc<KeyComputeClient>,
    /// Ollama HTTP 客户端
    ollama_client: Arc<OllamaClient>,
    /// 当前 session 信息
    session: SessionData,
    /// 镜像的服务端节点状态
    node_status: Arc<tokio::sync::Mutex<String>>,
    /// 镜像的服务端失败计数
    server_failure_count: Arc<tokio::sync::Mutex<u32>>,
    /// 镜像的服务端失败阈值
    failure_threshold: Arc<tokio::sync::Mutex<u32>>,
}

impl TaskExecutor {
    /// 创建新的任务执行器
    pub fn new(
        client: Arc<KeyComputeClient>,
        ollama_client: Arc<OllamaClient>,
        session: SessionData,
    ) -> Self {
        Self {
            client,
            ollama_client,
            session,
            node_status: Arc::new(tokio::sync::Mutex::new("unknown".to_string())),
            server_failure_count: Arc::new(tokio::sync::Mutex::new(0)),
            failure_threshold: Arc::new(tokio::sync::Mutex::new(3)),
        }
    }

    /// 执行单个任务
    ///
    /// # 流程
    /// 1. 从 envelope 中提取任务信息
    /// 2. 根据任务类型路由到具体执行方法
    /// 3. 将结果转换为 NodeTaskResult
    /// 4. 提交结果到服务端（带重试）
    pub async fn execute(&self, envelope: NodeTaskEnvelope) {
        let task_id = envelope.task_id;
        let lease_id = envelope.lease_id;
        let deadline_ms = envelope.deadline_unix_ms;
        let grace_until_ms = envelope.complete_grace_until_unix_ms;

        info!(
            "Executing task: task_id={}, model={}, deadline_ms={}, grace_until_ms={}",
            task_id, envelope.model, deadline_ms, grace_until_ms
        );

        // 1. 根据任务类型路由
        let result = if envelope.payload.is_chat() {
            match self.execute_chat(&envelope).await {
                Ok(response) => {
                    info!("Chat task {} executed successfully", task_id);
                    NodeTaskResult::Succeeded { response }
                }
                Err(e) => {
                    error!("Chat task {} execution failed: {}", task_id, e);
                    classify_ollama_error(&e)
                }
            }
        } else if envelope.payload.is_image_generation() {
            match self.execute_image_generation(&envelope).await {
                Ok(image_response) => {
                    info!("Image generation task {} executed successfully", task_id);
                    NodeTaskResult::ImageSucceeded { image_response }
                }
                Err(e) => {
                    error!("Image generation task {} execution failed: {}", task_id, e);
                    classify_ollama_error(&e)
                }
            }
        } else if envelope.payload.is_image_edit() {
            match self.execute_image_edit(&envelope).await {
                Ok(image_response) => {
                    info!("Image edit task {} executed successfully", task_id);
                    NodeTaskResult::ImageSucceeded { image_response }
                }
                Err(e) => {
                    error!("Image edit task {} execution failed: {}", task_id, e);
                    classify_ollama_error(&e)
                }
            }
        } else {
            error!("Task {} has no recognized payload type", task_id);
            NodeTaskResult::Failed {
                code: "unknown_task_type".to_string(),
                message: "No recognized task payload type".to_string(),
                is_client_error: true,
            }
        };

        // 2. 提交结果（带重试）
        self.complete_with_retry(task_id, lease_id, result, deadline_ms, grace_until_ms)
            .await;
    }

    /// 执行 Chat 任务
    async fn execute_chat(&self, envelope: &NodeTaskEnvelope) -> Result<ChatCompletionResponse> {
        let chat_req =
            envelope.payload.chat.as_ref().ok_or_else(|| {
                NodeTokenError::TaskExecution("Chat request is missing".to_string())
            })?;

        // 解析图片 URL：将 HTTP URL 下载并转为 base64 data URI
        let mut resolved_req = chat_req.clone();
        self.ollama_client.resolve_images(&mut resolved_req).await?;

        let ollama_req = OllamaClient::chat_request_to_ollama(&resolved_req, &envelope.model);
        let ollama_resp = self.ollama_client.chat(&ollama_req).await?;
        Ok(self.ollama_response_to_chat(&ollama_resp, &envelope.model))
    }

    /// Ollama generate API 公共调用逻辑
    ///
    /// 委托给 `OllamaClient::generate()` 封装 HTTP 请求、错误处理和 JSON 解析，
    /// 本方法仅负责从响应 JSON 中提取图片数据并构造 `ImageGenerationResponse`。
    async fn execute_ollama_generate(
        &self,
        body: serde_json::Value,
        op: GenerateOp,
    ) -> Result<ImageGenerationResponse> {
        let parsed = self.ollama_client.generate(body, &op.to_string()).await?;

        let images = if let Some(base64_data) = parsed["response"].as_str() {
            vec![ImageData {
                url: None,
                b64_json: Some(base64_data.to_string()),
                revised_prompt: None,
            }]
        } else if let Some(img_arr) = parsed.get("images").and_then(|v| v.as_array()) {
            img_arr
                .iter()
                .map(|img| ImageData {
                    url: None,
                    b64_json: img.as_str().map(|s| s.to_string()),
                    revised_prompt: None,
                })
                .collect()
        } else {
            return Err(NodeTokenError::Ollama(format!(
                "Ollama {} did not return image data",
                op
            )));
        };

        Ok(ImageGenerationResponse {
            created: Utc::now().timestamp(),
            data: images,
        })
    }

    /// 执行图片生成任务
    async fn execute_image_generation(
        &self,
        envelope: &NodeTaskEnvelope,
    ) -> Result<ImageGenerationResponse> {
        let img_req = envelope.payload.image_generation.as_ref().ok_or_else(|| {
            NodeTokenError::TaskExecution("Image generation request is missing".to_string())
        })?;

        info!(
            "Calling Ollama generate for image: model={}",
            envelope.model
        );

        // Ollama /api/generate 不支持 n/size 参数，显式警告调用方
        if img_req.n.is_some() && img_req.n != Some(1) {
            warn!(
                "Image generation n={:?} ignored: Ollama generate API does not support 'n' parameter",
                img_req.n
            );
        }
        if img_req.size.is_some() {
            warn!(
                "Image generation size={:?} ignored: Ollama generate API does not support 'size' parameter",
                img_req.size
            );
        }

        let body = serde_json::json!({
            "model": envelope.model,
            "prompt": img_req.prompt,
            "stream": false,
        });

        self.execute_ollama_generate(body, GenerateOp::ImageGeneration)
            .await
    }

    /// 执行图片编辑任务
    async fn execute_image_edit(
        &self,
        envelope: &NodeTaskEnvelope,
    ) -> Result<ImageGenerationResponse> {
        let edit_req = envelope.payload.image_edit.as_ref().ok_or_else(|| {
            NodeTokenError::TaskExecution("Image edit request is missing".to_string())
        })?;

        info!(
            "Calling Ollama generate for image edit: model={}",
            envelope.model
        );

        // Ollama /api/generate 不支持 n/size 参数，显式警告调用方
        if edit_req.n.is_some() && edit_req.n != Some(1) {
            warn!(
                "Image edit n={:?} ignored: Ollama generate API does not support 'n' parameter",
                edit_req.n
            );
        }
        if edit_req.size.is_some() {
            warn!(
                "Image edit size={:?} ignored: Ollama generate API does not support 'size' parameter",
                edit_req.size
            );
        }

        // 提取非空的 base64 image，传给 Ollama 的 images 数组
        let image_base64 = non_empty_base64(&edit_req.image).ok_or_else(|| {
            NodeTokenError::TaskExecution("Image edit request has empty image field".to_string())
        })?;
        let mut body = serde_json::json!({
            "model": envelope.model,
            "prompt": edit_req.prompt,
            "stream": false,
        });

        // 构建 images 数组：原始图片 + 可选遮罩
        let mask_b64 = edit_req.mask.as_ref().and_then(|m| non_empty_base64(m));
        if mask_b64.is_some() {
            debug!("Including mask image in edit request");
        }
        let mut images = vec![image_base64];
        if let Some(mask) = mask_b64 {
            images.push(mask);
        }
        body["images"] = serde_json::json!(images);

        self.execute_ollama_generate(body, GenerateOp::ImageEdit)
            .await
    }

    /// 将 Ollama 响应转换为 ChatCompletionResponse
    /// 委托给 `OllamaClient::ollama_response_to_chat`，消除重复代码
    fn ollama_response_to_chat(
        &self,
        ollama_resp: &crate::protocol::ollama::OllamaChatResponse,
        model: &str,
    ) -> crate::protocol::types::ChatCompletionResponse {
        crate::client::OllamaClient::ollama_response_to_chat(ollama_resp, model)
    }

    /// 提交结果到服务端（带重试）
    async fn complete_with_retry(
        &self,
        task_id: uuid::Uuid,
        lease_id: uuid::Uuid,
        result: NodeTaskResult,
        deadline_ms: i64,
        grace_until_ms: i64,
    ) {
        let req = NodeTaskCompleteRequest {
            protocol_version: "node.v1".to_string(),
            node_id: self.session.node_id,
            session_id: self.session.session_id,
            task_id,
            lease_id,
            result,
        };

        let now_ms = Utc::now().timestamp_millis();

        let retry_deadline = if now_ms < deadline_ms {
            deadline_ms
        } else {
            grace_until_ms
        };

        let max_retry_duration = std::cmp::max(0, retry_deadline - now_ms);

        if max_retry_duration <= 0 {
            warn!(
                "Task {} past grace period, attempting one-shot complete",
                task_id
            );
            match self.client.complete(task_id, &req).await {
                Ok(resp) => {
                    info!(
                        "Task {} completed (one-shot): action={:?}",
                        task_id, resp.action
                    );
                    *self.node_status.lock().await = resp.node_status.clone();
                    *self.server_failure_count.lock().await = resp.server_failure_count;
                    *self.failure_threshold.lock().await = resp.failure_threshold;
                    self.log_complete_response(task_id, &resp);
                }
                Err(e) => {
                    error!("Task {} one-shot complete failed: {}", task_id, e);
                }
            }
            return;
        }

        let max_retries =
            std::cmp::max(1, (max_retry_duration as f64 / 1000.0).ceil() as usize / 5);

        let retry_strategy = ExponentialBackoff::from_millis(100)
            .max_delay(std::time::Duration::from_secs(5))
            .take(max_retries);

        info!(
            "Starting complete retry for task {}: max_duration={}ms, max_retries={}",
            task_id, max_retry_duration, max_retries
        );

        match Retry::spawn(retry_strategy, || async {
            match self.client.complete(task_id, &req).await {
                Ok(resp) => {
                    info!("Task {} completed: action={:?}", task_id, resp.action);
                    *self.node_status.lock().await = resp.node_status.clone();
                    *self.server_failure_count.lock().await = resp.server_failure_count;
                    *self.failure_threshold.lock().await = resp.failure_threshold;
                    self.log_complete_response(task_id, &resp);
                    Ok(resp)
                }
                Err(e) => {
                    warn!("Complete failed for task {}: {}", task_id, e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(_) => {
                info!("Task {} complete succeeded", task_id);
            }
            Err(e) => {
                error!("Task {} complete failed after retries: {}", task_id, e);
            }
        }
    }

    fn log_complete_response(&self, task_id: uuid::Uuid, resp: &NodeTaskCompleteResponse) {
        info!(
            "Complete response for task {}: action={:?}, task_status={}, node_status={}, failure_count={}/{}",
            task_id,
            resp.action,
            resp.task_status,
            resp.node_status,
            resp.server_failure_count,
            resp.failure_threshold
        );

        if resp.node_status == "excluded" {
            warn!(
                "Node EXCLUDED after task {} complete, will stop poll but continue heartbeat",
                task_id
            );
        }
    }
}

/// 检查 base64 字符串是否非空，空字符串返回 None
///
/// 用于 `ImageEditRequest` 的 `image`/`mask` 字段，这些字段已是 base64 编码字符串。
/// 注意：此函数不做格式标准化（如 padding 补齐、空白剥离），仅做空值守卫。
fn non_empty_base64(data: &str) -> Option<String> {
    if data.is_empty() {
        return None;
    }
    Some(data.to_string())
}

/// 把 Ollama 调用错误分类成 NodeTaskResult
fn classify_ollama_error(err: &NodeTokenError) -> NodeTaskResult {
    let (code, message, is_client_error) = match err {
        NodeTokenError::HttpError { status, message } if (400..500).contains(status) => {
            (format!("ollama_http_{}", status), message.clone(), true)
        }
        NodeTokenError::HttpError { status, message } => {
            (format!("ollama_http_{}", status), message.clone(), false)
        }
        NodeTokenError::Network(e) => ("ollama_network".to_string(), e.to_string(), false),
        other => ("ollama_error".to_string(), other.to_string(), false),
    };
    NodeTaskResult::Failed {
        code,
        message,
        is_client_error,
    }
}

// 实现 Clone 以便在 tokio::spawn 中使用
impl Clone for TaskExecutor {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            ollama_client: self.ollama_client.clone(),
            session: self.session.clone(),
            node_status: self.node_status.clone(),
            server_failure_count: self.server_failure_count.clone(),
            failure_threshold: self.failure_threshold.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{ChatCompletionRequest, Message};

    fn create_test_executor() -> TaskExecutor {
        use crate::client::{KeyComputeClient, OllamaClient};
        use crate::protocol::types::NodeCapabilities;
        use crate::storage::SessionData;
        use std::sync::Arc;

        let client = Arc::new(KeyComputeClient::new("http://localhost:3000"));
        let ollama_client = Arc::new(OllamaClient::new("http://localhost:11434"));
        let session = SessionData {
            node_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            session_token: "test-token".to_string(),
            capabilities: NodeCapabilities {
                runtime: "ollama".to_string(),
                models: vec![],
            },
            poll_timeout_secs: 30,
        };

        TaskExecutor::new(client, ollama_client, session)
    }

    #[test]
    fn test_chat_request_to_ollama_basic() {
        let chat_req = ChatCompletionRequest::new(
            "deepseek-chat",
            vec![
                Message::system("You are a helpful assistant"),
                Message::user("Hello!"),
            ],
        );

        let ollama_req = OllamaClient::chat_request_to_ollama(&chat_req, "deepseek-chat");

        assert_eq!(ollama_req.model, "deepseek-chat");
        assert!(!ollama_req.stream);
        assert_eq!(ollama_req.messages.len(), 2);
        assert_eq!(ollama_req.messages[0].role, "system");
        assert_eq!(
            ollama_req.messages[0].content,
            "You are a helpful assistant"
        );
        assert_eq!(ollama_req.messages[1].role, "user");
        assert_eq!(ollama_req.messages[1].content, "Hello!");
    }

    #[test]
    fn test_chat_request_to_ollama_multiple_messages() {
        let chat_req = ChatCompletionRequest {
            model: "llama3".to_string(),
            messages: vec![
                Message::system("System prompt"),
                Message::user("Question 1"),
                Message::assistant("Answer 1"),
                Message::user("Question 2"),
            ],
            stream: Some(false),
            max_tokens: None,
            temperature: None,
            top_p: None,
            n: None,
            stop: None,
        };

        let ollama_req = OllamaClient::chat_request_to_ollama(&chat_req, "llama3");

        assert_eq!(ollama_req.model, "llama3");
        assert_eq!(ollama_req.messages.len(), 4);
        assert_eq!(ollama_req.messages[0].role, "system");
        assert_eq!(ollama_req.messages[1].role, "user");
        assert_eq!(ollama_req.messages[2].role, "assistant");
        assert_eq!(ollama_req.messages[3].role, "user");
    }

    #[test]
    fn test_ollama_response_to_chat_basic() {
        use crate::protocol::ollama::OllamaChatResponse;
        use crate::protocol::ollama::OllamaMessage as OllamaMsg;

        let executor = create_test_executor();
        let ollama_resp = OllamaChatResponse {
            model: "deepseek-chat".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            message: OllamaMsg {
                role: "assistant".to_string(),
                content: "Hello! How can I help you?".to_string(),
                images: None,
            },
            done: true,
            total_duration: 1000000000,
            load_duration: 500000000,
            prompt_eval_count: 10,
            eval_count: 20,
        };

        let chat_resp = executor.ollama_response_to_chat(&ollama_resp, "deepseek-chat");

        assert_eq!(chat_resp.object, "chat.completion");
        assert_eq!(chat_resp.model, "deepseek-chat");
        assert_eq!(chat_resp.choices.len(), 1);
        assert_eq!(chat_resp.choices[0].index, 0);
        assert_eq!(chat_resp.choices[0].message.role, "assistant");
        assert_eq!(
            chat_resp.choices[0].message.content,
            "Hello! How can I help you?"
        );
        assert_eq!(chat_resp.choices[0].finish_reason, Some("stop".to_string()));
        assert_eq!(chat_resp.usage.prompt_tokens, 10);
        assert_eq!(chat_resp.usage.completion_tokens, 20);
        assert_eq!(chat_resp.usage.total_tokens, 30);
    }

    #[test]
    fn test_ollama_response_to_chat_empty_counts() {
        use crate::protocol::ollama::OllamaChatResponse;
        use crate::protocol::ollama::OllamaMessage as OllamaMsg;

        let executor = create_test_executor();
        let ollama_resp = OllamaChatResponse {
            model: "llama3".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            message: OllamaMsg {
                role: "assistant".to_string(),
                content: "Response".to_string(),
                images: None,
            },
            done: true,
            total_duration: 0,
            load_duration: 0,
            prompt_eval_count: 0,
            eval_count: 0,
        };

        let chat_resp = executor.ollama_response_to_chat(&ollama_resp, "llama3");

        assert_eq!(chat_resp.usage.prompt_tokens, 0);
        assert_eq!(chat_resp.usage.completion_tokens, 0);
        assert_eq!(chat_resp.usage.total_tokens, 0);
    }

    #[test]
    fn test_chat_request_to_ollama_preserves_content() {
        let complex_content = "fn main() { println!(\"Hello\"); }";

        let chat_req =
            ChatCompletionRequest::new("deepseek-coder", vec![Message::user(complex_content)]);

        let ollama_req = OllamaClient::chat_request_to_ollama(&chat_req, "deepseek-coder");

        assert_eq!(ollama_req.messages.len(), 1);
        assert_eq!(ollama_req.messages[0].content, complex_content);
    }

    #[test]
    fn test_ollama_response_to_chat_generates_unique_id() {
        use crate::protocol::ollama::OllamaChatResponse;
        use crate::protocol::ollama::OllamaMessage as OllamaMsg;

        let executor = create_test_executor();
        let ollama_resp = OllamaChatResponse {
            model: "test".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            message: OllamaMsg {
                role: "assistant".to_string(),
                content: "test".to_string(),
                images: None,
            },
            done: true,
            total_duration: 0,
            load_duration: 0,
            prompt_eval_count: 0,
            eval_count: 0,
        };

        let chat_resp1 = executor.ollama_response_to_chat(&ollama_resp, "test");
        let chat_resp2 = executor.ollama_response_to_chat(&ollama_resp, "test");

        assert_ne!(chat_resp1.id, chat_resp2.id);
        assert!(chat_resp1.id.starts_with("chatcmpl-"));
        assert!(chat_resp2.id.starts_with("chatcmpl-"));
    }

    #[test]
    fn test_execute_result_construction_success() {
        use crate::protocol::ollama::OllamaChatResponse;
        use crate::protocol::ollama::OllamaMessage as OllamaMsg;

        let ollama_resp = OllamaChatResponse {
            model: "deepseek-chat".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            message: OllamaMsg {
                role: "assistant".to_string(),
                content: "Hello!".to_string(),
                images: None,
            },
            done: true,
            total_duration: 1000000000,
            load_duration: 500000000,
            prompt_eval_count: 10,
            eval_count: 20,
        };

        let executor = create_test_executor();
        let chat_resp = executor.ollama_response_to_chat(&ollama_resp, "deepseek-chat");
        let result = NodeTaskResult::Succeeded {
            response: chat_resp,
        };

        match result {
            NodeTaskResult::Succeeded { response } => {
                assert_eq!(response.model, "deepseek-chat");
                assert_eq!(response.choices[0].message.content, "Hello!");
                assert_eq!(response.usage.total_tokens, 30);
            }
            _ => panic!("Expected Succeeded variant"),
        }
    }

    #[test]
    fn test_execute_result_construction_failure() {
        let error_msg = "Ollama API error: model not found";
        let result = NodeTaskResult::Failed {
            code: "ollama_error".to_string(),
            message: error_msg.to_string(),
            is_client_error: false,
        };

        match result {
            NodeTaskResult::Failed {
                code,
                message,
                is_client_error,
            } => {
                assert_eq!(code, "ollama_error");
                assert_eq!(message, error_msg);
                assert!(!is_client_error);
            }
            _ => panic!("Expected Failed variant"),
        }
    }

    #[test]
    fn test_classify_ollama_4xx_is_client_error() {
        let err = NodeTokenError::HttpError {
            status: 400,
            message: "model does not support chat".to_string(),
        };
        match classify_ollama_error(&err) {
            NodeTaskResult::Failed {
                is_client_error,
                code,
                ..
            } => {
                assert!(is_client_error, "4xx must mark is_client_error=true");
                assert_eq!(code, "ollama_http_400");
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_classify_ollama_5xx_is_not_client_error() {
        let err = NodeTokenError::HttpError {
            status: 503,
            message: "service unavailable".to_string(),
        };
        match classify_ollama_error(&err) {
            NodeTaskResult::Failed {
                is_client_error, ..
            } => assert!(!is_client_error),
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_base64_encode() {
        let data = b"hello";
        let encoded = crate::client::ollama::base64_encode(data);
        assert_eq!(encoded, "aGVsbG8=");
    }

    #[test]
    fn test_task_deadline_and_grace_period() {
        use chrono::{Duration, Utc};

        let now = Utc::now();
        let deadline = now + Duration::seconds(60);
        let grace_until = deadline + Duration::seconds(30);

        assert!(deadline > now);
        assert!(grace_until > deadline);
        assert_eq!((grace_until - deadline).num_seconds(), 30);
        assert!(now < deadline);

        let after_deadline = deadline + Duration::seconds(10);
        assert!(after_deadline > deadline);
        assert!(after_deadline < grace_until);

        let after_grace = grace_until + Duration::seconds(1);
        assert!(after_grace > grace_until);
    }
}
