//! Ollama API 类型定义
//!
//! 定义与 Ollama API 交互所需的请求和响应类型。

use serde::{Deserialize, Serialize};

/// Ollama chat 请求
#[derive(Debug, Clone, Serialize)]
pub struct OllamaChatRequest {
    /// 模型名称
    pub model: String,
    /// 消息列表
    pub messages: Vec<OllamaMessage>,
    /// 流式输出
    pub stream: bool,
}

/// Ollama 消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OllamaMessage {
    /// 角色：system, user, assistant
    pub role: String,
    /// 消息内容
    pub content: String,
    /// 图片列表（base64 编码，不含 data URI 前缀）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<String>>,
}

impl OllamaMessage {
    #[allow(dead_code)] // 在后续阶段使用
    pub fn new(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
            images: None,
        }
    }

    /// 创建带图片的消息
    #[allow(dead_code)]
    pub fn with_images(
        role: impl Into<String>,
        content: impl Into<String>,
        images: Vec<String>,
    ) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
            images: Some(images),
        }
    }
}

/// Ollama chat 响应
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // 部分字段在后续阶段使用
pub struct OllamaChatResponse {
    /// 模型名称
    pub model: String,
    /// 创建时间
    pub created_at: String,
    /// 响应消息
    pub message: OllamaMessage,
    /// 是否完成
    pub done: bool,
    /// 总耗时（纳秒）
    #[serde(default)]
    pub total_duration: u64,
    /// 加载模型耗时（纳秒）
    #[serde(default)]
    pub load_duration: u64,
    /// prompt token 数量
    #[serde(default)]
    pub prompt_eval_count: u32,
    /// 生成 token 数量
    #[serde(default)]
    pub eval_count: u32,
}

/// Ollama 模型列表响应
#[derive(Debug, Clone, Deserialize)]
pub struct OllamaModelListResponse {
    /// 模型列表
    pub models: Vec<OllamaModelInfo>,
}

/// Ollama 模型信息
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // 部分字段在后续阶段使用
pub struct OllamaModelInfo {
    /// 模型名称
    pub name: String,
    /// 模型大小
    #[serde(default)]
    pub size: u64,
    /// 修改时间
    #[serde(default)]
    pub modified_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// 验证 OllamaChatRequest 的 JSON 序列化符合 Ollama API 规范。
    /// 确保 model、messages、stream 字段正确序列化。
    fn test_ollama_chat_request_serialize() {
        let req = OllamaChatRequest {
            model: "deepseek-r1".to_string(),
            messages: vec![
                OllamaMessage::new("system", "You are a helpful assistant"),
                OllamaMessage::new("user", "Hello"),
            ],
            stream: false,
        };
        let json = serde_json::to_string(&req).unwrap();

        // 反序列化验证（注意：OllamaChatRequest 没有实现 Deserialize，
        // 所以我们验证 JSON 字符串包含预期的字段）
        assert!(json.contains("\"model\":\"deepseek-r1\""));
        assert!(json.contains("\"stream\":false"));
        assert!(json.contains("\"messages\""));
        assert!(json.contains("\"role\":\"system\""));
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("You are a helpful assistant"));
        assert!(json.contains("Hello"));

        // 验证消息数量
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["messages"].as_array().unwrap().len(), 2);
    }

    #[test]
    /// 验证 OllamaChatResponse 的 JSON 反序列化正确性。
    /// 覆盖完整字段和可选字段（带 default 属性）。
    fn test_ollama_chat_response_deserialize() {
        let json = r#"{
            "model": "deepseek-r1",
            "created_at": "2024-01-01T00:00:00Z",
            "message": {
                "role": "assistant",
                "content": "Hello!"
            },
            "done": true,
            "total_duration": 1000000000,
            "prompt_eval_count": 10,
            "eval_count": 20
        }"#;
        let resp: OllamaChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.model, "deepseek-r1");
        assert_eq!(resp.created_at, "2024-01-01T00:00:00Z");
        assert_eq!(resp.message.role, "assistant");
        assert_eq!(resp.message.content, "Hello!");
        assert!(resp.done);
        assert_eq!(resp.total_duration, 1000000000);
        assert_eq!(resp.prompt_eval_count, 10);
        assert_eq!(resp.eval_count, 20);
    }

    #[test]
    /// 验证 OllamaChatResponse 反序列化时可选字段的默认值处理。
    fn test_ollama_chat_response_default_fields() {
        let json = r#"{
            "model": "llama3",
            "created_at": "2024-01-01T00:00:00Z",
            "message": {
                "role": "assistant",
                "content": "Hi"
            },
            "done": true
        }"#;
        let resp: OllamaChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.model, "llama3");
        // 验证 default 字段使用默认值
        assert_eq!(resp.total_duration, 0);
        assert_eq!(resp.load_duration, 0);
        assert_eq!(resp.prompt_eval_count, 0);
        assert_eq!(resp.eval_count, 0);
    }

    #[test]
    /// 验证 OllamaModelListResponse 的 JSON 反序列化正确性。
    fn test_ollama_model_list_deserialize() {
        let json = r#"{
            "models": [
                {"name": "deepseek-r1:latest", "size": 4000000000, "modified_at": "2024-01-01T00:00:00Z"},
                {"name": "llama2:latest", "size": 3800000000, "modified_at": "2024-01-02T00:00:00Z"}
            ]
        }"#;
        let resp: OllamaModelListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.models.len(), 2);
        assert_eq!(resp.models[0].name, "deepseek-r1:latest");
        assert_eq!(resp.models[0].size, 4000000000);
        assert_eq!(resp.models[1].name, "llama2:latest");
        assert_eq!(resp.models[1].size, 3800000000);
    }

    #[test]
    /// 验证 OllamaModelInfo 反序列化时可选字段的默认值处理。
    fn test_ollama_model_info_default_fields() {
        let json = r#"{
            "models": [
                {"name": "deepseek-r1:latest"}
            ]
        }"#;
        let resp: OllamaModelListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.models.len(), 1);
        assert_eq!(resp.models[0].name, "deepseek-r1:latest");
        // 验证 default 字段使用默认值
        assert_eq!(resp.models[0].size, 0);
        assert_eq!(resp.models[0].modified_at, "");
    }

    #[test]
    /// 验证 OllamaMessage 的构造函数和序列化。
    fn test_ollama_message_helpers() {
        let msg = OllamaMessage::new("user", "Hello, world!");
        assert_eq!(msg.role, "user");
        assert_eq!(msg.content, "Hello, world!");

        let json = serde_json::to_string(&msg).unwrap();
        let parsed: OllamaMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.role, "user");
        assert_eq!(parsed.content, "Hello, world!");
    }

    #[test]
    /// 验证 OllamaMessage 支持多种角色。
    fn test_ollama_message_roles() {
        let roles = vec!["system", "user", "assistant", "tool"];

        for role in roles {
            let msg = OllamaMessage::new(role, "test content");
            assert_eq!(msg.role, role);
        }
    }

    #[test]
    /// 验证带图片的 OllamaMessage 序列化
    fn test_ollama_message_with_images() {
        let msg = OllamaMessage::with_images(
            "user",
            "Describe this image",
            vec!["iVBORw0KGgoAAAANSUhEUgAAAA".to_string()],
        );
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"images\""));
        assert!(json.contains("iVBORw0KGgoAAAANSUhEUgAAAA"));

        let parsed: OllamaMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.role, "user");
        assert_eq!(parsed.content, "Describe this image");
        assert_eq!(parsed.images.unwrap().len(), 1);
    }

    #[test]
    /// 验证不带图片的 OllamaMessage 不序列化 images 字段
    fn test_ollama_message_without_images() {
        let msg = OllamaMessage::new("user", "Hello");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("images"));
    }
}
