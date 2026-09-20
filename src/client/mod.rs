//! HTTP 客户端模块

pub mod api;
pub mod ollama;

// 重新导出主要客户端类型
pub use api::KeyComputeClient;
pub use ollama::{OllamaClient, OllamaNativeDiscovery};
