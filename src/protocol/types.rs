//! 节点协议类型定义
//!
//! 从 keycompute-types 复制的协议类型，node-token 作为独立项目不依赖 workspace。
//! 本协议版本固定为 `node.v1`，所有公开 JSON 字段使用 `snake_case`。

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// 类型别名
// ============================================================================

/// 节点 ID
pub type NodeId = Uuid;

/// 节点会话 ID
pub type NodeSessionId = Uuid;

/// 节点任务 ID
pub type NodeTaskId = Uuid;

/// 节点租约 ID
pub type NodeLeaseId = Uuid;

// ============================================================================
// 节点能力
// ============================================================================

/// 节点模型能力
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeModelCapability {
    /// 模型名称
    pub model: String,
}

/// 节点能力声明
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// 运行时类型（MVP 固定为 "ollama"）
    pub runtime: String,
    /// 支持的模型列表
    pub models: Vec<NodeModelCapability>,
}

// ============================================================================
// 注册协议
// ============================================================================

/// 节点注册请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    /// 协议版本（固定为 "node.v1"）
    pub protocol_version: String,
    /// 客户端实例 ID
    pub client_instance_id: String,
    /// 节点显示名称
    pub display_name: String,
    /// 注册 token
    pub registration_token: String,
    /// 节点能力声明
    pub capabilities: NodeCapabilities,
}

/// 节点注册响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterResponse {
    /// 协议版本
    pub protocol_version: String,
    /// 节点 ID
    pub node_id: NodeId,
    /// 会话 ID
    pub session_id: NodeSessionId,
    /// 会话 token（只返回一次，服务端只保存 hash）
    pub session_token: String,
    /// 心跳间隔（秒）
    pub heartbeat_interval_secs: u64,
    /// 轮询超时（秒）
    pub poll_timeout_secs: u64,
}

// ============================================================================
// 心跳协议
// ============================================================================

/// 节点心跳请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeatRequest {
    /// 协议版本
    pub protocol_version: String,
    /// 节点 ID
    pub node_id: NodeId,
    /// 会话 ID
    pub session_id: NodeSessionId,
    /// 当前可接受模型列表
    pub accepted_models: Vec<String>,
}

/// 节点心跳响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeatResponse {
    /// 协议版本
    pub protocol_version: String,
    /// 是否接受（session 与请求体身份校验通过）
    pub accepted: bool,
    /// 节点状态（online/offline/excluded）
    pub node_status: String,
    /// 服务端失败计数
    pub server_failure_count: u32,
    /// 失败阈值
    pub failure_threshold: u32,
}

// ============================================================================
// 轮询协议
// ============================================================================

/// 节点轮询请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePollRequest {
    /// 协议版本
    pub protocol_version: String,
    /// 节点 ID
    pub node_id: NodeId,
    /// 会话 ID
    pub session_id: NodeSessionId,
}

/// 节点轮询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePollResponse {
    /// 协议版本
    pub protocol_version: String,
    /// 任务信封（如果有任务）
    pub task: Option<NodeTaskEnvelope>,
    /// 重试间隔（毫秒）
    pub retry_after_ms: Option<u64>,
}

// ============================================================================
// 任务协议
// ============================================================================

/// 节点任务信封
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTaskEnvelope {
    /// 任务 ID
    pub task_id: NodeTaskId,
    /// 租约 ID
    pub lease_id: NodeLeaseId,
    /// 模型名称（去掉 node: 前缀后的实际模型名）
    pub model: String,
    /// 任务截止时间（Unix 毫秒时间戳）
    pub deadline_unix_ms: i64,
    /// 完成宽限期（Unix 毫秒时间戳）
    pub complete_grace_until_unix_ms: i64,
    /// 任务载荷
    pub payload: NodeTaskPayload,
}

/// 节点任务载荷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTaskPayload {
    /// 请求 ID
    pub request_id: Uuid,
    /// Chat 完成请求（可选，与图片生成/编辑互斥）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat: Option<ChatCompletionRequest>,
    /// 图片生成请求（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_generation: Option<ImageGenerationRequest>,
    /// 图片编辑请求（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_edit: Option<ImageEditRequest>,
}

impl NodeTaskPayload {
    /// 是否为 Chat 任务
    pub fn is_chat(&self) -> bool {
        self.chat.is_some()
    }

    /// 是否为图片生成任务
    pub fn is_image_generation(&self) -> bool {
        self.image_generation.is_some()
    }

    /// 是否为图片编辑任务
    pub fn is_image_edit(&self) -> bool {
        self.image_edit.is_some()
    }
}

// ============================================================================
// 提交结果协议
// ============================================================================

/// 节点任务完成请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTaskCompleteRequest {
    /// 协议版本
    pub protocol_version: String,
    /// 节点 ID
    pub node_id: NodeId,
    /// 会话 ID
    pub session_id: NodeSessionId,
    /// 任务 ID
    pub task_id: NodeTaskId,
    /// 租约 ID
    pub lease_id: NodeLeaseId,
    /// 任务结果
    pub result: NodeTaskResult,
}

/// 节点任务结果
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum NodeTaskResult {
    /// 任务成功(非流式完整响应)
    Succeeded {
        /// Chat 完成响应
        response: ChatCompletionResponse,
    },
    /// 图片生成/编辑任务成功
    ImageSucceeded {
        /// 图片生成响应
        image_response: ImageGenerationResponse,
    },
    /// 任务失败
    Failed {
        /// 错误码
        code: String,
        /// 错误消息
        message: String,
        /// 该失败是否由请求本身的问题引起(如模型不支持、参数非法)。
        /// 老 server 忽略该字段时仍按通常失败处理, 保留向后兼容。
        #[serde(default)]
        is_client_error: bool,
    },
}

/// 节点任务完成响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTaskCompleteResponse {
    /// 执行动作
    pub action: NodeTaskCompleteAction,
    /// 任务状态
    pub task_status: String,
    /// 节点状态
    pub node_status: String,
    /// 服务端失败计数
    pub server_failure_count: u32,
    /// 失败阈值
    pub failure_threshold: u32,
}

/// 节点任务完成动作
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NodeTaskCompleteAction {
    /// 任务成功完成
    Succeeded,
    /// 任务恢复为 queued（重新入队）
    Requeued,
    /// 任务失败
    Failed,
    /// 任务过期
    Expired,
}

// ============================================================================
// OpenAI 兼容的请求/响应类型（用于与 Ollama 交互）
// ============================================================================

/// 消息角色枚举
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    #[default]
    User,
    Assistant,
    Tool,
}

impl MessageRole {
    /// 获取角色字符串表示
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        }
    }
}

impl std::fmt::Display for MessageRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 为 `#[serde(untagged)]` 枚举生成自定义 `Deserialize` 实现
///
/// 该枚举有两个变体：纯文本字符串 (`Text(String)`) 和多模态内容块数组 (`Parts(Vec<Part>)`)。
/// 自定义反序列化在遇到空数组 `[]` 时返回错误，避免多模态数据静默丢弃。
///
/// # 参数
/// - `$enum_ty`: 枚举类型名（如 `MessageContent`）
/// - `$part_ty`: Part 类型（如 `ContentPart`）
/// - `$err_desc`: 空数组时的错误描述字符串
macro_rules! impl_untagged_content_deserialize {
    ($enum_ty:ty, $part_ty:ty, $err_desc:expr) => {
        impl<'de> serde::Deserialize<'de> for $enum_ty {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use serde::de;

                struct ContentVisitor;

                impl<'de> de::Visitor<'de> for ContentVisitor {
                    type Value = $enum_ty;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("a string or a non-empty array of content parts")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(<$enum_ty>::Text(value.to_string()))
                    }

                    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(<$enum_ty>::Text(value))
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where
                        A: de::SeqAccess<'de>,
                    {
                        let mut parts = Vec::new();
                        while let Some(part) = seq.next_element::<$part_ty>()? {
                            parts.push(part);
                        }
                        if parts.is_empty() {
                            return Err(de::Error::invalid_value(de::Unexpected::Seq, &$err_desc));
                        }
                        Ok(<$enum_ty>::Parts(parts))
                    }
                }

                deserializer.deserialize_any(ContentVisitor)
            }
        }
    };
}

/// 消息内容：支持纯文本和 Vision 多模态内容
///
/// 反序列化时拒绝空数组 `[]`，避免静默丢失数据。
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// 纯文本内容
    Text(String),
    /// Vision 内容块列表（图片理解等）
    Parts(Vec<ContentPart>),
}

// 使用自定义 Deserialize 实现，拒绝空数组 []（防止多模态数据静默丢弃）
impl_untagged_content_deserialize!(
    MessageContent,
    ContentPart,
    "non-empty array of content parts"
);

impl MessageContent {
    /// 从纯文本创建
    pub fn text(content: impl Into<String>) -> Self {
        Self::Text(content.into())
    }

    /// 提取纯文本内容（用于日志/转换等场景）
    ///
    /// 对于 `Parts` 变体，此方法用空格拼接所有 `ContentPart::Text` 块，
    /// 跳过图片块。注意：这是有损转换 — 多段文本的结构信息和图片内容
    /// 会在此过程中丢失。如需完整的多模态数据，请直接使用 `extract_images()`。
    pub fn extract_text(&self) -> String {
        match self {
            Self::Text(s) => s.clone(),
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|p| match p {
                    ContentPart::Text { text } => Some(text.as_str()),
                    ContentPart::ImageUrl { .. } => None,
                })
                .collect::<Vec<_>>()
                .join(" "),
        }
    }

    /// 提取所有图片 URL（原始形式，含 data URI 前缀或 HTTP URL）
    pub fn extract_images(&self) -> Vec<String> {
        match self {
            Self::Text(_) => vec![],
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|p| match p {
                    ContentPart::ImageUrl { image_url } => Some(image_url.url.clone()),
                    ContentPart::Text { .. } => None,
                })
                .collect(),
        }
    }

    /// 是否为纯文本
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }
}

impl From<String> for MessageContent {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for MessageContent {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl std::fmt::Display for MessageContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.extract_text())
    }
}

/// Vision 内容块
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentPart {
    /// 文本块
    #[serde(rename = "text")]
    Text { text: String },
    /// 图片 URL 块
    #[serde(rename = "image_url")]
    ImageUrl { image_url: ImageUrl },
}

/// 图片 URL 描述
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageUrl {
    /// 图片 URL（支持 http/https URL 或 base64 data URI）
    pub url: String,
    /// 细节级别：low / high / auto（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: MessageRole,
    pub content: MessageContent,
}

impl Message {
    #[allow(dead_code)] // 在后续阶段使用
    pub fn new(role: MessageRole, content: impl Into<MessageContent>) -> Self {
        Self {
            role,
            content: content.into(),
        }
    }

    #[allow(dead_code)] // 在后续阶段使用
    pub fn system(content: impl Into<MessageContent>) -> Self {
        Self::new(MessageRole::System, content)
    }

    #[allow(dead_code)] // 在后续阶段使用
    pub fn user(content: impl Into<MessageContent>) -> Self {
        Self::new(MessageRole::User, content)
    }

    #[allow(dead_code)] // 在后续阶段使用
    pub fn assistant(content: impl Into<MessageContent>) -> Self {
        Self::new(MessageRole::Assistant, content)
    }

    #[allow(dead_code)] // 在后续阶段使用
    pub fn tool(content: impl Into<MessageContent>) -> Self {
        Self::new(MessageRole::Tool, content)
    }
}

/// OpenAI 兼容的请求体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,
}

impl ChatCompletionRequest {
    #[allow(dead_code)] // 在后续阶段使用
    pub fn new(model: impl Into<String>, messages: Vec<Message>) -> Self {
        Self {
            model: model.into(),
            messages,
            stream: None,
            max_tokens: None,
            temperature: None,
            top_p: None,
            n: None,
            stop: None,
        }
    }
}

/// OpenAI 兼容的非流式响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<CompletionChoice>,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionChoice {
    pub index: u32,
    pub message: ChoiceMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

/// 响应消息（用于 chat completion choices）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChoiceMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

// ============================================================================
// 图片生成/编辑类型
// ============================================================================

/// 图片生成请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageGenerationRequest {
    /// 生成提示词
    pub prompt: String,
    /// 生成图片数量（可选，默认 1）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    /// 图片尺寸（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

/// 图片编辑请求
///
/// 注意：`image` 和 `mask` 字段使用 base64 编码字符串传输，
/// 而非 `Vec<u8>`（后者经 JSON 序列化后会膨胀约 6 倍体积）。
/// 执行时由 executor 解码为原始字节。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageEditRequest {
    /// 编辑提示词
    pub prompt: String,
    /// 原始图片（base64 编码，不含 data URI 前缀）
    pub image: String,
    /// 遮罩图片（可选，base64 编码）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mask: Option<String>,
    /// 图片数量（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    /// 图片尺寸（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

/// 图片数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageData {
    /// 图片 URL（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Base64 编码的图片数据（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b64_json: Option<String>,
    /// 修改后的提示词（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revised_prompt: Option<String>,
}

/// 图片生成响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageGenerationResponse {
    /// 创建时间戳
    pub created: i64,
    /// 图片数据列表
    pub data: Vec<ImageData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_role_serialize() {
        let role = MessageRole::Assistant;
        let json = serde_json::to_string(&role).unwrap();
        assert_eq!(json, "\"assistant\"");
    }

    #[test]
    fn test_message_role_deserialize() {
        let json = "\"system\"";
        let role: MessageRole = serde_json::from_str(json).unwrap();
        assert_eq!(role, MessageRole::System);
    }

    #[test]
    fn test_chat_completion_request_serialize() {
        let req = ChatCompletionRequest::new("deepseek-chat", vec![Message::user("Hello")]);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"model\":\"deepseek-chat\""));
        assert!(json.contains("\"role\":\"user\""));
    }

    // ========================================================================
    // 节点能力测试
    // ========================================================================

    #[test]
    fn test_node_model_capability_serialize() {
        let capability = NodeModelCapability {
            model: "deepseek-chat".to_string(),
        };
        let json = serde_json::to_string(&capability).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodeModelCapability = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.model, "deepseek-chat");
    }

    #[test]
    fn test_node_model_capability_deserialize() {
        let json = r#"{"model":"llama3"}"#;
        let capability: NodeModelCapability = serde_json::from_str(json).unwrap();
        assert_eq!(capability.model, "llama3");
    }

    #[test]
    fn test_node_capabilities_serialize() {
        let caps = NodeCapabilities {
            runtime: "ollama".to_string(),
            models: vec![
                NodeModelCapability {
                    model: "deepseek-chat".to_string(),
                },
                NodeModelCapability {
                    model: "llama3".to_string(),
                },
            ],
        };
        let json = serde_json::to_string(&caps).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodeCapabilities = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.runtime, "ollama");
        assert_eq!(parsed.models.len(), 2);
        assert_eq!(parsed.models[0].model, "deepseek-chat");
        assert_eq!(parsed.models[1].model, "llama3");
    }

    #[test]
    fn test_node_capabilities_deserialize() {
        let json = r#"{
            "runtime": "ollama",
            "models": [
                {"model": "deepseek-chat"},
                {"model": "llama3"}
            ]
        }"#;
        let caps: NodeCapabilities = serde_json::from_str(json).unwrap();
        assert_eq!(caps.runtime, "ollama");
        assert_eq!(caps.models.len(), 2);
        assert_eq!(caps.models[0].model, "deepseek-chat");
        assert_eq!(caps.models[1].model, "llama3");
    }

    // ========================================================================
    // 注册协议测试
    // ========================================================================

    #[test]
    fn test_node_register_request_serialize() {
        let req = NodeRegisterRequest {
            protocol_version: "node.v1".to_string(),
            client_instance_id: "test-instance-001".to_string(),
            display_name: "Test Node".to_string(),
            registration_token: "secret-token".to_string(),
            capabilities: NodeCapabilities {
                runtime: "ollama".to_string(),
                models: vec![NodeModelCapability {
                    model: "deepseek-chat".to_string(),
                }],
            },
        };
        let json = serde_json::to_string(&req).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodeRegisterRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol_version, "node.v1");
        assert_eq!(parsed.client_instance_id, "test-instance-001");
        assert_eq!(parsed.display_name, "Test Node");
        assert_eq!(parsed.capabilities.runtime, "ollama");
        assert_eq!(parsed.capabilities.models.len(), 1);
    }

    #[test]
    fn test_node_register_response_deserialize() {
        let node_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let json = serde_json::json!({
            "protocol_version": "node.v1",
            "node_id": node_id.to_string(),
            "session_id": session_id.to_string(),
            "session_token": "test-session-token",
            "heartbeat_interval_secs": 30,
            "poll_timeout_secs": 60
        });
        let resp: NodeRegisterResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.protocol_version, "node.v1");
        assert_eq!(resp.node_id, node_id);
        assert_eq!(resp.session_id, session_id);
        assert_eq!(resp.session_token, "test-session-token");
        assert_eq!(resp.heartbeat_interval_secs, 30);
        assert_eq!(resp.poll_timeout_secs, 60);
    }

    // ========================================================================
    // 心跳协议测试
    // ========================================================================

    #[test]
    fn test_node_heartbeat_request_serialize() {
        let node_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let req = NodeHeartbeatRequest {
            protocol_version: "node.v1".to_string(),
            node_id,
            session_id,
            accepted_models: vec!["deepseek-chat".to_string(), "llama3".to_string()],
        };
        let json = serde_json::to_string(&req).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodeHeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol_version, "node.v1");
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.session_id, session_id);
        assert_eq!(parsed.accepted_models.len(), 2);
        assert!(
            parsed
                .accepted_models
                .contains(&"deepseek-chat".to_string())
        );
        assert!(parsed.accepted_models.contains(&"llama3".to_string()));
    }

    #[test]
    fn test_node_heartbeat_response_deserialize() {
        let json = serde_json::json!({
            "protocol_version": "node.v1",
            "accepted": true,
            "node_status": "online",
            "server_failure_count": 0,
            "failure_threshold": 3
        });
        let resp: NodeHeartbeatResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.protocol_version, "node.v1");
        assert!(resp.accepted);
        assert_eq!(resp.node_status, "online");
        assert_eq!(resp.server_failure_count, 0);
        assert_eq!(resp.failure_threshold, 3);
    }

    #[test]
    fn test_node_heartbeat_response_excluded() {
        let json = serde_json::json!({
            "protocol_version": "node.v1",
            "accepted": true,
            "node_status": "excluded",
            "server_failure_count": 3,
            "failure_threshold": 3
        });
        let resp: NodeHeartbeatResponse = serde_json::from_value(json).unwrap();
        assert!(resp.accepted);
        assert_eq!(resp.node_status, "excluded");
        assert_eq!(resp.server_failure_count, 3);
    }

    // ========================================================================
    // 轮询协议测试
    // ========================================================================

    #[test]
    fn test_node_poll_request_serialize() {
        let node_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let req = NodePollRequest {
            protocol_version: "node.v1".to_string(),
            node_id,
            session_id,
        };
        let json = serde_json::to_string(&req).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodePollRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol_version, "node.v1");
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.session_id, session_id);
    }

    #[test]
    fn test_node_poll_response_with_task() {
        let task_id = Uuid::new_v4();
        let lease_id = Uuid::new_v4();
        let json = serde_json::json!({
            "protocol_version": "node.v1",
            "task": {
                "task_id": task_id.to_string(),
                "lease_id": lease_id.to_string(),
                "model": "deepseek-chat",
                "deadline_unix_ms": 1234567890123i64,
                "complete_grace_until_unix_ms": 1234567950123i64,
                "payload": {
                    "request_id": Uuid::new_v4().to_string(),
                    "chat": {
                        "model": "deepseek-chat",
                        "messages": [{"role": "user", "content": "Hello"}]
                    }
                }
            },
            "retry_after_ms": null
        });
        let resp: NodePollResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.protocol_version, "node.v1");
        assert!(resp.task.is_some());
        let task = resp.task.unwrap();
        assert_eq!(task.task_id, task_id);
        assert_eq!(task.lease_id, lease_id);
        assert_eq!(task.model, "deepseek-chat");
    }

    #[test]
    fn test_node_poll_response_no_task() {
        let json = serde_json::json!({
            "protocol_version": "node.v1",
            "task": null,
            "retry_after_ms": 1000
        });
        let resp: NodePollResponse = serde_json::from_value(json).unwrap();
        assert!(resp.task.is_none());
        assert_eq!(resp.retry_after_ms, Some(1000));
    }

    // ========================================================================
    // 提交结果协议测试
    // ========================================================================

    #[test]
    fn test_node_task_complete_request_succeeded() {
        let node_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let lease_id = Uuid::new_v4();
        let req = NodeTaskCompleteRequest {
            protocol_version: "node.v1".to_string(),
            node_id,
            session_id,
            task_id,
            lease_id,
            result: NodeTaskResult::Succeeded {
                response: ChatCompletionResponse {
                    id: "resp-001".to_string(),
                    object: "chat.completion".to_string(),
                    created: 1234567890,
                    model: "deepseek-chat".to_string(),
                    choices: vec![CompletionChoice {
                        index: 0,
                        message: ChoiceMessage {
                            role: "assistant".to_string(),
                            content: "Hello! How can I help you?".to_string(),
                        },
                        finish_reason: Some("stop".to_string()),
                    }],
                    usage: Usage {
                        prompt_tokens: 10,
                        completion_tokens: 20,
                        total_tokens: 30,
                    },
                },
            },
        };
        let json = serde_json::to_string(&req).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodeTaskCompleteRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol_version, "node.v1");
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.task_id, task_id);
        assert_eq!(parsed.lease_id, lease_id);
        match parsed.result {
            NodeTaskResult::Succeeded { response } => {
                assert_eq!(response.id, "resp-001");
                assert_eq!(
                    response.choices[0].message.content,
                    "Hello! How can I help you?"
                );
            }
            NodeTaskResult::ImageSucceeded { .. } => panic!("Expected Succeeded variant"),
            NodeTaskResult::Failed { .. } => panic!("Expected Succeeded variant"),
        }
    }

    #[test]
    fn test_node_task_complete_request_failed() {
        let node_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let lease_id = Uuid::new_v4();
        let req = NodeTaskCompleteRequest {
            protocol_version: "node.v1".to_string(),
            node_id,
            session_id,
            task_id,
            lease_id,
            result: NodeTaskResult::Failed {
                code: "ollama_error".to_string(),
                message: "Model not found".to_string(),
                is_client_error: false,
            },
        };
        let json = serde_json::to_string(&req).unwrap();

        // 反序列化验证（强断言）
        let parsed: NodeTaskCompleteRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol_version, "node.v1");
        assert_eq!(parsed.task_id, task_id);
        match parsed.result {
            NodeTaskResult::Failed {
                code,
                message,
                is_client_error,
            } => {
                assert_eq!(code, "ollama_error");
                assert_eq!(message, "Model not found");
                assert!(!is_client_error);
            }
            NodeTaskResult::Succeeded { .. } => panic!("Expected Failed variant"),
            NodeTaskResult::ImageSucceeded { .. } => panic!("Expected Failed variant"),
        }
    }

    #[test]
    fn test_node_task_complete_response_deserialize() {
        let json = serde_json::json!({
            "action": "succeeded",
            "task_status": "succeeded",
            "node_status": "online",
            "server_failure_count": 0,
            "failure_threshold": 3
        });
        let resp: NodeTaskCompleteResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.action, NodeTaskCompleteAction::Succeeded);
        assert_eq!(resp.task_status, "succeeded");
        assert_eq!(resp.node_status, "online");
    }

    #[test]
    fn test_node_task_complete_action_enum() {
        // 测试所有 action 变体的序列化
        let actions = vec![
            (NodeTaskCompleteAction::Succeeded, "succeeded"),
            (NodeTaskCompleteAction::Requeued, "requeued"),
            (NodeTaskCompleteAction::Failed, "failed"),
            (NodeTaskCompleteAction::Expired, "expired"),
        ];

        for (action, expected_str) in actions {
            let json = serde_json::to_string(&action).unwrap();
            assert_eq!(json, format!("\"{}\"", expected_str));

            // 反序列化验证
            let deserialized: NodeTaskCompleteAction = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, action);
        }
    }

    // ========================================================================
    // ChatCompletionResponse 测试
    // ========================================================================

    #[test]
    fn test_chat_completion_response_serialize() {
        let resp = ChatCompletionResponse {
            id: "resp-001".to_string(),
            object: "chat.completion".to_string(),
            created: 1234567890,
            model: "deepseek-chat".to_string(),
            choices: vec![CompletionChoice {
                index: 0,
                message: ChoiceMessage {
                    role: "assistant".to_string(),
                    content: "Test response".to_string(),
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: Usage {
                prompt_tokens: 5,
                completion_tokens: 10,
                total_tokens: 15,
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"id\":\"resp-001\""));
        assert!(json.contains("\"object\":\"chat.completion\""));
        assert!(json.contains("\"model\":\"deepseek-chat\""));
        assert!(json.contains("\"role\":\"assistant\""));
        assert!(json.contains("Test response"));
    }

    #[test]
    fn test_chat_completion_response_deserialize() {
        let json = r#"{
            "id": "resp-002",
            "object": "chat.completion",
            "created": 1234567891,
            "model": "llama3",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Response"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 8, "completion_tokens": 12, "total_tokens": 20}
        }"#;
        let resp: ChatCompletionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "resp-002");
        assert_eq!(resp.model, "llama3");
        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].message.content, "Response");
        assert_eq!(resp.usage.total_tokens, 20);
    }

    // ========================================================================
    // Message 测试
    // ========================================================================

    #[test]
    fn test_message_serialize() {
        let msg = Message::user("Hello, world!");
        let json = serde_json::to_string(&msg).unwrap();

        // 反序列化验证（强断言）
        let parsed: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.role, MessageRole::User);
        assert_eq!(parsed.content.extract_text(), "Hello, world!");
    }

    #[test]
    fn test_message_deserialize() {
        let json = r#"{"role":"system","content":"You are a helpful assistant"}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert_eq!(msg.role, MessageRole::System);
        assert_eq!(msg.content.extract_text(), "You are a helpful assistant");
    }

    #[test]
    fn test_message_helper_methods() {
        let system_msg = Message::system("System prompt");
        assert_eq!(system_msg.role, MessageRole::System);
        assert_eq!(system_msg.content.extract_text(), "System prompt");

        let user_msg = Message::user("User question");
        assert_eq!(user_msg.role, MessageRole::User);
        assert_eq!(user_msg.content.extract_text(), "User question");

        let assistant_msg = Message::assistant("Assistant response");
        assert_eq!(assistant_msg.role, MessageRole::Assistant);
        assert_eq!(assistant_msg.content.extract_text(), "Assistant response");

        let tool_msg = Message::tool("Tool output");
        assert_eq!(tool_msg.role, MessageRole::Tool);
        assert_eq!(tool_msg.content.extract_text(), "Tool output");
    }

    // ========================================================================
    // 边界条件测试
    // ========================================================================

    #[test]
    fn test_message_with_unicode() {
        let msg = Message::user("🚀 My Node 节点 Привет");
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content.extract_text(), "🚀 My Node 节点 Привет");
    }

    /// 验证 MessageContent 拒绝空数组 `[]`（防止多模态数据静默丢弃）
    #[test]
    fn test_message_content_rejects_empty_array() {
        let json = r#"{"role":"user","content":[]}"#;
        let result: Result<Message, _> = serde_json::from_str(json);
        assert!(result.is_err(), "MessageContent must reject empty array []");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("non-empty") || err_msg.contains("invalid"),
            "Error should mention non-empty array, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_message_with_special_characters() {
        let content = r#"Line1\nLine2\"Quote\\Backslash"#;
        let msg = Message::user(content);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content.extract_text(), content);
    }

    #[test]
    fn test_empty_accepted_models() {
        let node_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let req = NodeHeartbeatRequest {
            protocol_version: "node.v1".to_string(),
            node_id,
            session_id,
            accepted_models: vec![], // 空列表
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: NodeHeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.accepted_models.len(), 0);
    }

    #[test]
    fn test_chat_completion_request_with_optional_fields() {
        let req = ChatCompletionRequest {
            model: "deepseek-chat".to_string(),
            messages: vec![Message::user("Test")],
            stream: Some(false),
            max_tokens: Some(100),
            temperature: Some(0.7),
            top_p: Some(0.9),
            n: Some(1),
            stop: Some(vec!["\n".to_string()]),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ChatCompletionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.max_tokens, Some(100));
        assert_eq!(parsed.temperature, Some(0.7));
        assert_eq!(parsed.top_p, Some(0.9));
        assert_eq!(parsed.stop, Some(vec!["\n".to_string()]));
    }

    #[test]
    fn test_node_task_complete_action_all_variants() {
        // 测试所有 action 变体的 roundtrip
        let actions = vec![
            NodeTaskCompleteAction::Succeeded,
            NodeTaskCompleteAction::Requeued,
            NodeTaskCompleteAction::Failed,
            NodeTaskCompleteAction::Expired,
        ];

        for action in actions {
            let json = serde_json::to_string(&action).unwrap();
            let parsed: NodeTaskCompleteAction = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, action);
        }
    }

    /// 验证 IPv6 方括号 host 提取（复用 ollama 模块的 extract_host_from_url）
    #[test]
    fn test_ipv6_bracket_handling() {
        use crate::client::ollama::extract_host_from_url;
        // IPv6 with port
        assert_eq!(
            extract_host_from_url("http://[::1]:8080/img.png").unwrap(),
            "::1"
        );
        // IPv6 without port
        assert_eq!(
            extract_host_from_url("http://[::1]/img.png").unwrap(),
            "::1"
        );
        // Normal IPv4
        assert_eq!(
            extract_host_from_url("http://example.com:8080/img.png").unwrap(),
            "example.com"
        );
        // Normal host without port
        assert_eq!(
            extract_host_from_url("https://example.com/img.png").unwrap(),
            "example.com"
        );
    }
}
