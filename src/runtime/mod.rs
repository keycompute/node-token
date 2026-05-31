//! 运行时模块
//!
//! 包含节点的核心运行逻辑：
//! - 注册（register）
//! - 心跳（heartbeat）
//! - 轮询（poll）
//! - 任务执行（executor）

pub mod executor;
pub mod heartbeat;
pub mod poll;
pub mod register;

// 重新导出主要类型和函数，方便外部使用
pub use executor::TaskExecutor;
pub use heartbeat::heartbeat_loop;
pub use poll::{PollLoopConfig, poll_loop};
pub use register::{register_node, try_load_session};
