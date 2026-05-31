//! 心跳循环逻辑
//!
//! 定期向服务端发送心跳，上报当前可接受模型快照，
//! 镜像服务端返回的节点状态和失败计数。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::{error, info, warn};

use crate::client::{KeyComputeClient, OllamaClient};
use crate::config::NodeTokenConfig;
use crate::protocol::types::NodeHeartbeatRequest;
use crate::storage::SessionData;

/// 心跳循环
///
/// # 参数
/// - `client`: KeyCompute HTTP 客户端
/// - `ollama_client`: Ollama HTTP 客户端
/// - `session`: 当前 session 信息
/// - `config`: 节点配置
/// - `is_excluded`: 节点排除标志（与 poll 循环共享）
/// - `stop_signal`: 退出信号
///
/// # 行为
/// - 定期发送心跳（间隔由 config.heartbeat_interval_secs 控制）
/// - 上报当前 Ollama 模型列表作为 accepted_models
/// - 镜像服务端返回的 node_status、server_failure_count、failure_threshold
/// - 如果节点被 excluded，使用低频心跳（间隔增大 3 倍）
/// - 网络错误不增加失败计数，继续重试
pub async fn heartbeat_loop(
    client: &KeyComputeClient,
    ollama_client: &OllamaClient,
    session: &SessionData,
    config: &NodeTokenConfig,
    is_excluded: Arc<AtomicBool>,
    stop_signal: Arc<AtomicBool>,
) {
    let base_interval = Duration::from_secs(config.heartbeat_interval_secs);
    let mut current_interval = base_interval;
    let mut interval = tokio::time::interval(current_interval);

    // 第一次立即触发，不等待完整间隔
    // 这样 main.rs 中的初始等待（2 秒）后，is_excluded 已被第一次心跳更新
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        "Starting heartbeat loop: interval={}s",
        config.heartbeat_interval_secs
    );

    // 连续失败计数，用于日志记录
    let mut consecutive_failures: u32 = 0;

    while !stop_signal.load(Ordering::Relaxed) {
        interval.tick().await;

        // 获取上报的模型列表
        // 设计意图：
        // 1. 正常情况下，使用注册时的 capabilities（保证是注册能力的子集）
        // 2. 如果注册时为空（启动时机问题），则重新扫描 Ollama（容错处理）
        let registered_models: Vec<String> = session
            .capabilities
            .models
            .iter()
            .map(|m| m.model.clone())
            .collect();

        let models = if registered_models.is_empty() {
            // 注册时未扫描到模型，尝试重新扫描（可能是启动时 Ollama 未就绪）
            match ollama_client.list_models().await {
                Ok(current_models) => {
                    let current_models: Vec<String> = current_models;
                    if !current_models.is_empty() {
                        info!(
                            "Registration had empty models, using current Ollama models: {:?}",
                            current_models
                        );
                    }
                    current_models
                }
                Err(e) => {
                    warn!("Failed to list Ollama models for heartbeat: {}", e);
                    // Ollama 不可用，跳过本次心跳，下次重试
                    continue;
                }
            }
        } else {
            // 正常情况：使用注册时的 capabilities
            // 但需要过滤掉当前 Ollama 中不存在的模型（模型被删除的情况）
            match ollama_client.list_models().await {
                Ok(current_models) => {
                    let current_models: Vec<String> = current_models;
                    // 取注册模型和当前模型的交集
                    let active_models: Vec<String> = registered_models
                        .into_iter()
                        .filter(|m| current_models.contains(m))
                        .collect();

                    // 检查是否有模型被删除
                    if active_models.len() != session.capabilities.models.len() {
                        if active_models.is_empty() {
                            // 严重情况：所有注册模型都被删除
                            error!(
                                "All registered models have been removed from Ollama. \
                                 Node will not receive any tasks. \
                                 Registered: {:?}, Please pull models back.",
                                session
                                    .capabilities
                                    .models
                                    .iter()
                                    .map(|m| &m.model)
                                    .collect::<Vec<_>>()
                            );
                        } else {
                            // 部分模型被删除
                            warn!(
                                "Some registered models no longer available in Ollama. \
                                 Registered: {:?}, Active: {:?}",
                                session
                                    .capabilities
                                    .models
                                    .iter()
                                    .map(|m| &m.model)
                                    .collect::<Vec<_>>(),
                                active_models
                            );
                        }
                    }

                    active_models
                }
                Err(e) => {
                    warn!("Failed to list Ollama models for heartbeat: {}", e);
                    // Ollama 不可用，使用注册时的模型（保守策略）
                    registered_models
                }
            }
        };

        let req = NodeHeartbeatRequest {
            protocol_version: "node.v1".to_string(),
            node_id: session.node_id,
            session_id: session.session_id,
            accepted_models: models,
        };

        match client.heartbeat(&req).await {
            Ok(resp) => {
                // 成功后重置失败计数
                consecutive_failures = 0;

                // 镜像服务端状态
                info!(
                    "Heartbeat: accepted={}, status={}, failure_count={}/{}",
                    resp.accepted,
                    resp.node_status,
                    resp.server_failure_count,
                    resp.failure_threshold
                );

                // 更新 excluded 标志（通知 poll 循环）
                let was_excluded = is_excluded.load(Ordering::Relaxed);
                let now_excluded = resp.node_status == "excluded";
                is_excluded.store(now_excluded, Ordering::Relaxed);

                if now_excluded && !was_excluded {
                    warn!("Node has been EXCLUDED - will stop poll but continue heartbeat");
                    // excluded 节点使用低频心跳（间隔增大 3 倍）
                    current_interval = base_interval * 3;
                    interval = tokio::time::interval(current_interval);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                } else if !now_excluded && was_excluded {
                    info!(
                        "Node status changed from excluded to {}, restoring normal heartbeat interval",
                        resp.node_status
                    );
                    // 恢复为正常心跳间隔
                    current_interval = base_interval;
                    interval = tokio::time::interval(current_interval);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                }

                // 如果 heartbeat 被拒绝，记录警告
                if !resp.accepted {
                    warn!(
                        "Heartbeat not accepted by server, node_status={}",
                        resp.node_status
                    );
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                error!(
                    "Heartbeat failed (consecutive={}): {}",
                    consecutive_failures, e
                );
                // 网络错误不增加失败计数，继续重试
                // interval 会继续按当前间隔触发，这是合理的退避策略
            }
        }
    }

    info!("Heartbeat loop stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    /// 验证 AtomicBool 的 excluded 状态更新逻辑。
    /// 模拟服务端返回 excluded 状态后的标志设置。
    fn test_is_excluded_flag_update() {
        let is_excluded = Arc::new(AtomicBool::new(false));

        // 初始状态：非 excluded
        assert!(!is_excluded.load(Ordering::Relaxed));

        // 模拟服务端返回 excluded
        is_excluded.store(true, Ordering::Relaxed);
        assert!(is_excluded.load(Ordering::Relaxed));

        // 模拟恢复
        is_excluded.store(false, Ordering::Relaxed);
        assert!(!is_excluded.load(Ordering::Relaxed));
    }

    #[test]
    /// 验证心跳间隔计算逻辑。
    /// excluded 节点的心跳间隔应该增大 3 倍。
    fn test_heartbeat_interval_calculation() {
        let base_interval = Duration::from_secs(30);
        let excluded_interval = base_interval * 3;

        assert_eq!(excluded_interval, Duration::from_secs(90));

        // 验证其他倍数
        let short_interval = Duration::from_secs(10);
        assert_eq!(short_interval * 3, Duration::from_secs(30));

        let long_interval = Duration::from_secs(60);
        assert_eq!(long_interval * 3, Duration::from_secs(180));
    }

    #[test]
    /// 验证心跳间隔边界条件。
    fn test_heartbeat_interval_edge_cases() {
        // 最小间隔
        let min_interval = Duration::from_secs(1);
        assert_eq!(min_interval * 3, Duration::from_secs(3));

        // 零间隔（理论上不应该出现，但要处理）
        let zero_interval = Duration::from_secs(0);
        assert_eq!(zero_interval * 3, Duration::from_secs(0));
    }

    #[test]
    /// 验证多个 AtomicBool 并发访问的安全性。
    fn test_atomic_bool_concurrent_access() {
        let is_excluded = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];

        // 创建多个线程同时读写
        for i in 0..10 {
            let flag = is_excluded.clone();
            let handle = std::thread::spawn(move || {
                if i % 2 == 0 {
                    flag.store(true, Ordering::Relaxed);
                } else {
                    let _ = flag.load(Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 验证没有 panic
        let _ = is_excluded.load(Ordering::Relaxed);
    }
}
