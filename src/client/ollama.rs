//! Ollama HTTP 客户端
//!
//! 负责与本地 Ollama 实例通信，包括模型列表查询和 chat 调用。

use crate::error::{NodeTokenError, OllamaResult};
use crate::protocol::ollama::{
    OllamaChatRequest, OllamaChatResponse, OllamaMessage, OllamaModelListResponse,
};
use crate::protocol::types::{
    ChatCompletionRequest, ChatCompletionResponse, ChoiceMessage, CompletionChoice, ContentPart,
    ImageUrl, MessageContent, Usage,
};
use reqwest::Client;
use std::net::IpAddr;
use tracing::{debug, error, info, warn};

/// 图片下载的最大字节数（20MB）
const MAX_IMAGE_BYTES: usize = 20 * 1024 * 1024;

/// 从 MessageContent 中提取 base64 图片数据（剥离 data URI 前缀）
///
/// 畸形的 data URI（无逗号或逗号后无数据）会被跳过并警告，
/// 避免将垃圾数据传给下游 Ollama。
pub(crate) fn extract_data_uri_images(content: &MessageContent) -> Vec<String> {
    let raw_images = content.extract_images();
    raw_images
        .iter()
        .filter_map(|img| {
            // 仅处理 data URI（data:image/...;base64,...），非 data URI 原样保留
            if img.starts_with("data:") {
                if let Some(comma_pos) = img.find(',') {
                    let after_comma = &img[comma_pos + 1..];
                    if !after_comma.is_empty() {
                        return Some(after_comma.to_string());
                    }
                }
                // 畸形的 data URI：无逗号或逗号后无实际 base64 数据
                // 跳过而非传给 Ollama，防止静默失败
                warn!(
                    "Skipping malformed data URI (missing base64 data): {}...",
                    &img[..img.len().min(80)]
                );
                None
            } else {
                // 非 data URI（如已下载的纯 base64），直接透传
                Some(img.clone())
            }
        })
        .collect()
}

/// 判断 URL 是否为 HTTP/HTTPS 链接（需要下载）
fn is_http_url(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

/// 判断 IP 地址是否为私有/内网地址
///
/// 覆盖所有 RFC 1918 / RFC 6598 / RFC 3927 / RFC 4291 私有地址范围：
/// - IPv4: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
/// - IPv4: 127.0.0.0/8 (loopback), 169.254.0.0/16 (link-local, 含云元数据端点)
/// - IPv4: 0.0.0.0 (unspecified)
/// - IPv6: ::1 (loopback), fc00::/7 (ULA), fe80::/10 (link-local)
fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            v4.is_loopback()
                || v4.is_unspecified()
                || octets[0] == 10
                || (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31)
                || (octets[0] == 192 && octets[1] == 168)
                || (octets[0] == 169 && octets[1] == 254) // AWS IMDS
        }
        IpAddr::V6(v6) => {
            let octets = v6.octets();
            v6.is_loopback()
                || (octets[0] & 0xfe) == 0xfc
                || octets[0] == 0xfe && octets[1] & 0xc0 == 0x80
        }
    }
}

/// 从 URL 中提取 host（不含端口），支持 IPv6 方括号
pub(crate) fn extract_host_from_url(url: &str) -> OllamaResult<&str> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| {
            NodeTokenError::Image(format!("Invalid image URL (unsupported protocol): {}", url))
        })?;

    let host_port = rest.split('/').next().unwrap_or("");
    if host_port.is_empty() {
        return Err(NodeTokenError::Image(format!(
            "Invalid image URL (missing host): {}",
            url
        )));
    }

    // IPv6 方括号格式: [::1] 或 [::1]:8080
    if let Some(inner) = host_port.strip_prefix('[') {
        return inner.split_once(']').map(|(h, _)| h).ok_or_else(|| {
            NodeTokenError::Image(format!("Invalid IPv6 URL (missing ']'): {}", url))
        });
    }

    // 普通 host:port 或纯 host
    if let Some((h, _)) = host_port.split_once(':') {
        Ok(h)
    } else {
        Ok(host_port)
    }
}

/// 解析 DNS 并验证实际 IP 非私有地址（防 DNS 重绑定攻击）
///
/// 多层防御：
/// 1. 如果 host 已经是 IP 字面量，直接用 is_private_ip 检查
/// 2. 否则解析 DNS，检查所有解析出的 IP 均为公网地址
///
/// # 重要：调用方必须包裹超时
///
/// 本函数内部使用 `tokio::net::lookup_host` 进行 DNS 解析，
/// 该调用**没有内置超时**——网络不通时可能永久挂起。
/// **所有调用方必须用 `tokio::time::timeout` 包裹本函数**。
/// 当前唯一调用方 `download_image_to_base64` 使用 10s DNS 超时。
///
/// 注意：DNS 预检与 reqwest 实际 DNS 解析之间存在 TOCTOU 窗口，
/// 但配合禁止重定向 + Content-Type 校验 + 大小限制，实际风险极低。
async fn validate_dns_no_private(url: &str) -> OllamaResult<()> {
    let host = extract_host_from_url(url)?;

    // 拦截 localhost（知名主机名，必然指向回环地址）
    if host.eq_ignore_ascii_case("localhost") {
        return Err(NodeTokenError::Image(format!(
            "Internal hostname blocked for image download: {}",
            host
        )));
    }

    // 如果 host 已经是 IP 字面量，直接检查是否为私有/内网地址
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_private_ip(&ip) {
            return Err(NodeTokenError::Image(format!(
                "Private IP address blocked for image download: {}",
                host
            )));
        }
        return Ok(());
    }

    // 解析 DNS（使用系统默认解析器）
    let addr_str = format!("{}:443", host);
    let addrs = tokio::net::lookup_host(&addr_str).await.map_err(|e| {
        NodeTokenError::Image(format!("Failed to resolve image host '{}': {}", host, e))
    })?;

    for addr in addrs {
        if is_private_ip(&addr.ip()) {
            return Err(NodeTokenError::Image(format!(
                "Image host '{}' resolved to private IP {} — blocked for security",
                host,
                addr.ip()
            )));
        }
    }

    Ok(())
}

/// Ollama HTTP 客户端
pub struct OllamaClient {
    /// Ollama 基础 URL
    pub(crate) base_url: String,
    /// HTTP 客户端（连接池，用于 Ollama API 调用）
    pub(crate) http_client: Client,
    /// 图片下载专用 HTTP 客户端（禁止重定向、独立 User-Agent，复用连接池）
    pub(crate) download_client: Client,
}

impl OllamaClient {
    /// 创建新的 Ollama 客户端
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(600)) // 10 分钟超时（模型推理可能较慢）
            .build()
            .expect("Failed to create Ollama HTTP client");

        // 图片下载专用客户端：禁止重定向（防 SSRF）、独立 User-Agent、复用连接池
        let download_client = Client::builder()
            .user_agent("KeyCompute-Node/1.0")
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create image download HTTP client");

        Self {
            base_url,
            http_client,
            download_client,
        }
    }

    /// 获取本地 Ollama 模型列表
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

    /// 下载远程图片并编码为 base64 data URI
    ///
    /// 安全措施：
    /// - DNS 预检解析（10s 独立超时，防止网络不通时挂起）
    /// - 最大 20MB 限制
    /// - 禁止重定向（防 SSRF）
    /// - 校验 Content-Type 为 image/*
    pub async fn download_image_to_base64(&self, url: &str) -> OllamaResult<String> {
        if !is_http_url(url) {
            return Err(NodeTokenError::Image(format!(
                "Unsupported image URL scheme: {}",
                url
            )));
        }

        // 防 SSRF: DNS 预检，验证实际 IP 非私有地址（防 DNS 重绑定攻击）
        // 使用 10s 超时包裹，防止网络不通时 tokio::net::lookup_host 永久挂起
        const DNS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        tokio::time::timeout(DNS_TIMEOUT, validate_dns_no_private(url))
            .await
            .map_err(|_| {
                NodeTokenError::Image(format!(
                    "DNS resolution timed out ({}s) for image host: {}",
                    DNS_TIMEOUT.as_secs(),
                    url
                ))
            })??;

        debug!("Downloading image from: {}", url);

        let response = self.download_client.get(url).send().await.map_err(|e| {
            warn!("Failed to download image from {}: {}", url, e);
            NodeTokenError::Image(format!("Failed to download image: {}", e))
        })?;

        let status = response.status();
        if !status.is_success() {
            return Err(NodeTokenError::Image(format!(
                "Image download failed with status {} from {}",
                status.as_u16(),
                url
            )));
        }

        // 检查 Content-Type
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_lowercase();

        if !content_type.starts_with("image/") {
            return Err(NodeTokenError::Image(format!(
                "Unexpected Content-Type '{}' for image URL: {}",
                content_type, url
            )));
        }

        // 读取字节（带大小限制，使用流式读取避免内存压力）
        let content_length = response.content_length().unwrap_or(0) as usize;
        if content_length > MAX_IMAGE_BYTES {
            return Err(NodeTokenError::Image(format!(
                "Image too large: {} bytes (max {}) from {}",
                content_length, MAX_IMAGE_BYTES, url
            )));
        }

        // 流式读取，在读取过程中累积检查大小，避免将超大响应完全缓冲到内存
        use futures_util::StreamExt;
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| {
                NodeTokenError::Image(format!("Failed to read image chunk from {}: {}", url, e))
            })?;
            if bytes.len() + chunk.len() > MAX_IMAGE_BYTES {
                return Err(NodeTokenError::Image(format!(
                    "Image too large: exceeds {} bytes (max {}) from {}",
                    bytes.len() + chunk.len(),
                    MAX_IMAGE_BYTES,
                    url
                )));
            }
            bytes.extend_from_slice(&chunk);
        }

        // 编码为 base64 data URI
        let base64_data = base64_encode(&bytes);
        let data_uri = format!("data:{};base64,{}", content_type, base64_data);

        debug!(
            "Downloaded image from {}: {} bytes, content_type={}",
            url,
            bytes.len(),
            content_type
        );
        Ok(data_uri)
    }

    /// 解析请求中的图片 URL：将 HTTP URL 下载并转为 base64 data URI
    ///
    /// 此方法原地修改 `request` 中的 `MessageContent::Parts`，
    /// 将 `ImageUrl` 中的 HTTP/HTTPS URL 替换为 data URI。
    /// 已经是 data URI 的图片保持不变。
    pub async fn resolve_images(&self, request: &mut ChatCompletionRequest) -> OllamaResult<()> {
        for msg in &mut request.messages {
            if let MessageContent::Parts(ref mut parts) = msg.content {
                for part in parts.iter_mut() {
                    if let ContentPart::ImageUrl {
                        image_url: ImageUrl { url, .. },
                    } = part
                        && is_http_url(url)
                    {
                        let data_uri = self.download_image_to_base64(url).await?;
                        *url = data_uri;
                    }
                }
            }
        }
        Ok(())
    }

    /// 调用 Ollama generate API（用于图片生成/编辑）
    ///
    /// 封装 `/api/generate` 的 HTTP 请求和错误处理，返回原始 JSON Value。
    /// 调用方负责从响应中提取图片数据。
    pub async fn generate(
        &self,
        body: serde_json::Value,
        operation: &str,
    ) -> OllamaResult<serde_json::Value> {
        let url = format!("{}/api/generate", self.base_url);

        let response = self
            .http_client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NodeTokenError::Ollama(format!("{} request failed: {}", operation, e)))?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            // 使用 .text() 而非 .json() 是为了在解析失败时能记录响应体片段
            let body = response.text().await.unwrap_or_default();
            let max_len = 500usize;
            let truncated = if body.len() <= max_len {
                body.clone()
            } else {
                format!(
                    "{}... (truncated, {} bytes total)",
                    &body[..max_len],
                    body.len()
                )
            };
            error!(
                "Ollama {} failed with status {}: {}",
                operation, status, truncated
            );
            return Err(NodeTokenError::HttpError {
                status,
                message: truncated,
            });
        }

        // 使用 .text() + from_str 而非 .json()，以便在 JSON 解析失败时
        // 记录响应体片段用于诊断（Ollama 偶尔返回非 JSON 错误文本）
        let body = response.text().await.map_err(|e| {
            NodeTokenError::Ollama(format!("Failed to read {} response: {}", operation, e))
        })?;

        serde_json::from_str(&body).map_err(|e| {
            let max_len = 200usize;
            let snippet = if body.len() <= max_len {
                body.clone()
            } else {
                format!(
                    "{}... (truncated, {} bytes total)",
                    &body[..max_len],
                    body.len()
                )
            };
            error!(
                "Failed to parse {} response: {} — body: {}",
                operation, e, snippet
            );
            NodeTokenError::Ollama(format!(
                "Failed to parse {} response: {} — body: {}",
                operation, e, snippet
            ))
        })
    }

    /// 将 ChatCompletionRequest 转换为 OllamaChatRequest
    ///
    /// `model` 参数允许调用方覆盖请求中的模型名（如 node 任务使用剥离前缀后的模型名）。
    pub fn chat_request_to_ollama(
        request: &ChatCompletionRequest,
        model: &str,
    ) -> OllamaChatRequest {
        let messages: Vec<OllamaMessage> = request
            .messages
            .iter()
            .map(|m| {
                let text = m.content.extract_text();
                let images = extract_data_uri_images(&m.content);
                OllamaMessage {
                    role: m.role.as_str().to_string(),
                    content: text,
                    images: if images.is_empty() {
                        None
                    } else {
                        Some(images)
                    },
                }
            })
            .collect();

        OllamaChatRequest {
            model: model.to_string(),
            messages,
            stream: false,
        }
    }

    /// 将 OllamaChatResponse 转换为 ChatCompletionResponse
    pub fn ollama_response_to_chat(
        response: &OllamaChatResponse,
        model: &str,
    ) -> ChatCompletionResponse {
        ChatCompletionResponse {
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            object: "chat.completion".to_string(),
            created: chrono::Utc::now().timestamp(),
            model: model.to_string(),
            choices: vec![CompletionChoice {
                index: 0,
                message: ChoiceMessage {
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
    pub async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> OllamaResult<ChatCompletionResponse> {
        let ollama_request = Self::chat_request_to_ollama(request, &request.model);
        let ollama_response = self.chat(&ollama_request).await?;
        Ok(Self::ollama_response_to_chat(
            &ollama_response,
            &request.model,
        ))
    }
}

/// Base64 编码（使用标准引擎）
pub(crate) fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
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
    fn test_is_http_url() {
        assert!(is_http_url("http://example.com/img.png"));
        assert!(is_http_url("https://example.com/img.png"));
        assert!(!is_http_url("data:image/png;base64,abc"));
        assert!(!is_http_url("ftp://example.com/img.png"));
    }

    #[test]
    fn test_extract_data_uri_images() {
        use crate::protocol::types::{ContentPart, ImageUrl};

        // 测试 data URI 前缀剥离
        let content = MessageContent::Parts(vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA".to_string(),
                detail: None,
            },
        }]);
        let images = extract_data_uri_images(&content);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0], "iVBORw0KGgoAAAANSUhEUgAAAA");

        // 纯文本消息无图片
        let content = MessageContent::Text("Hello".to_string());
        let images = extract_data_uri_images(&content);
        assert!(images.is_empty());
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

        let ollama_request = OllamaClient::chat_request_to_ollama(&request, "deepseek-chat");

        assert_eq!(ollama_request.model, "deepseek-chat");
        assert_eq!(ollama_request.messages.len(), 2);
        assert_eq!(ollama_request.messages[0].role, "system");
        assert_eq!(
            ollama_request.messages[0].content,
            "You are a helpful assistant"
        );
        assert_eq!(ollama_request.messages[1].role, "user");
        assert_eq!(ollama_request.messages[1].content, "Hello");
        assert!(ollama_request.messages[0].images.is_none());
        assert!(!ollama_request.stream);
    }

    #[test]
    fn test_chat_request_conversion_with_images() {
        use crate::protocol::types::{ContentPart, ImageUrl};

        let request = ChatCompletionRequest {
            model: "llava".to_string(),
            messages: vec![Message {
                role: MessageRole::User,
                content: MessageContent::Parts(vec![
                    ContentPart::Text {
                        text: "Describe this image".to_string(),
                    },
                    ContentPart::ImageUrl {
                        image_url: ImageUrl {
                            url: "data:image/png;base64,iVBORw0KGgo".to_string(),
                            detail: None,
                        },
                    },
                ]),
            }],
            stream: None,
            max_tokens: None,
            temperature: None,
            top_p: None,
            n: None,
            stop: None,
        };

        let ollama_request = OllamaClient::chat_request_to_ollama(&request, "llava");
        assert_eq!(ollama_request.messages.len(), 1);
        assert_eq!(ollama_request.messages[0].content, "Describe this image");
        let images = ollama_request.messages[0].images.as_ref().unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0], "iVBORw0KGgo");
    }

    #[test]
    fn test_ollama_response_conversion() {
        let ollama_response = OllamaChatResponse {
            model: "deepseek-chat".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            message: OllamaMessage {
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
    async fn test_resolve_images_with_data_uri() {
        let client = OllamaClient::new("http://localhost:11434");

        // 使用 data URI（不需要 HTTP 下载），验证 resolve_images 对已有 base64 的透传
        let mut request = ChatCompletionRequest {
            model: "llava".to_string(),
            messages: vec![Message {
                role: MessageRole::User,
                content: MessageContent::Parts(vec![
                    ContentPart::Text {
                        text: "What is this?".to_string(),
                    },
                    ContentPart::ImageUrl {
                        image_url: ImageUrl {
                            url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA".to_string(),
                            detail: None,
                        },
                    },
                ]),
            }],
            stream: None,
            max_tokens: None,
            temperature: None,
            top_p: None,
            n: None,
            stop: None,
        };

        client.resolve_images(&mut request).await.unwrap();

        // 验证 data URI 纹丝未动（已是 base64，无需下载）
        if let MessageContent::Parts(ref parts) = request.messages[0].content {
            if let ContentPart::ImageUrl {
                image_url: ImageUrl { url, .. },
            } = &parts[1]
            {
                assert_eq!(url, "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA");
            } else {
                panic!("Expected ImageUrl");
            }
        }

        // 验证 chat_request_to_ollama 能正确转换
        let ollama_req = OllamaClient::chat_request_to_ollama(&request, "llava");
        let images = ollama_req.messages[0].images.as_ref().unwrap();
        assert_eq!(images.len(), 1);
        assert!(
            !images[0].contains("data:"),
            "Ollama images must be raw base64"
        );
    }

    #[tokio::test]
    async fn test_ssrf_private_ip_blocked() {
        let client = OllamaClient::new("http://localhost:11434");

        // 127.0.0.1 应被拦截
        let result = client
            .download_image_to_base64("http://127.0.0.1:8080/test.png")
            .await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Private IP") || err_msg.contains("private IP"),
            "Expected SSRF rejection for 127.0.0.1, got: {}",
            err_msg
        );

        // localhost 应被拦截
        let result = client
            .download_image_to_base64("http://localhost/test.png")
            .await;
        assert!(result.is_err());

        // 192.168.x.x 应被拦截
        let result = client
            .download_image_to_base64("http://192.168.1.1/test.png")
            .await;
        assert!(result.is_err());

        // 10.x.x.x 应被拦截
        let result = client
            .download_image_to_base64("http://10.0.0.1/test.png")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
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
    async fn test_generate_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/generate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "stable-diffusion",
                "created_at": "2024-01-01T00:00:00Z",
                "response": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk",
                "done": true
            })))
            .mount(&mock_server)
            .await;

        let client = OllamaClient::new(mock_server.uri());
        let body = serde_json::json!({
            "model": "stable-diffusion",
            "prompt": "a cat",
            "stream": false
        });

        let result = client.generate(body, "image generation").await.unwrap();
        assert_eq!(
            result["response"].as_str().unwrap(),
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
        );
    }

    #[tokio::test]
    async fn test_generate_http_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/generate"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&mock_server)
            .await;

        let client = OllamaClient::new(mock_server.uri());
        let body = serde_json::json!({"model": "test", "prompt": "test", "stream": false});

        let result = client.generate(body, "image generation").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ollama_not_started() {
        let client = OllamaClient::new("http://localhost:19999");

        let result = client.list_models().await;
        assert!(result.is_err());

        match result.unwrap_err() {
            NodeTokenError::Ollama(msg) => {
                println!("Actual error message: {}", msg);
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected Ollama error"),
        }
    }
}
