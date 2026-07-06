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
use crate::error::NodeTokenError;
use crate::protocol::types::NodeHeartbeatRequest;
use crate::storage::SessionData;

/// 模型扫描间隔：每 10 次心跳扫描一次（默认心跳 30s → 每 5 分钟扫描一次）
const MODEL_SCAN_HEARTBEAT_INTERVAL: u32 = 10;

/// 心跳上下文：携带控制信号和状态标志，减少函数参数数量
pub struct HeartbeatContext {
    /// 节点排除标志（与 poll 循环共享）
    pub is_excluded: Arc<AtomicBool>,
    /// 退出信号
    pub stop_signal: Arc<AtomicBool>,
    /// Session 失效信号（401 时置 true，通知 main 触发重注册）
    pub session_lost: Arc<AtomicBool>,
    /// 首次心跳完成后发送信号（通知 main 初始状态已就绪）
    pub first_hb_signal: Option<tokio::sync::oneshot::Sender<()>>,
}

/// 心跳循环
///
/// # 参数
/// - `client`: KeyCompute HTTP 客户端
/// - `ollama_client`: Ollama HTTP 客户端
/// - `session`: 当前 session 信息
/// - `config`: 节点配置
/// - `ctx`: 心跳上下文（包含控制信号和状态标志）
///
/// # 行为
/// - 定期发送心跳（间隔由 config.heartbeat_interval_secs 控制）
/// - 上报当前 Ollama 模型列表作为 accepted_models
/// - 镜像服务端返回的 node_status、server_failure_count、failure_threshold
/// - 如果节点被 excluded，使用低频心跳（间隔增大 3 倍）
/// - 网络错误不增加失败计数，继续重试
/// - 收到 401 Invalid session token 时置 session_lost 并立即退出循环
pub async fn heartbeat_loop(
    client: &KeyComputeClient,
    ollama_client: &OllamaClient,
    session: &SessionData,
    config: &NodeTokenConfig,
    mut ctx: HeartbeatContext,
) {
    let base_interval = Duration::from_secs(config.heartbeat_interval_secs);
    let mut current_interval = base_interval;
    let mut interval = tokio::time::interval(current_interval);

    // 第一次立即触发，不等待完整间隔
    // 第一次心跳完成后通过 oneshot channel 通知 main.rs，
    // 避免使用固定延迟等待（原 2s 硬编码已移除）
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        "Starting heartbeat loop: interval={}s",
        config.heartbeat_interval_secs
    );

    // 连续失败计数，用于日志记录
    let mut consecutive_failures: u32 = 0;

    // 心跳迭代计数，用于控制模型扫描频率
    let mut heartbeat_count: u32 = 0;

    // 缓存的模型列表（只在需要时刷新）
    let mut cached_models: Option<Vec<String>> = None;

    while !ctx.stop_signal.load(Ordering::Relaxed) {
        interval.tick().await;
        heartbeat_count += 1;

        // 获取上报的模型列表
        // 设计意图：
        // 1. 正常情况下，使用注册时的 capabilities（保证是注册能力的子集）
        // 2. 如果注册时为空（启动时机问题），则重新扫描 Ollama（容错处理）
        // 3. 非空注册模型每隔 MODEL_SCAN_HEARTBEAT_INTERVAL 次心跳扫描一次
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
                        cached_models = Some(current_models.clone());
                    }
                    current_models
                }
                Err(e) => {
                    warn!("Failed to list Ollama models for heartbeat: {}", e);
                    // Ollama 不可用，跳过本次心跳，下次重试
                    if ctx.first_hb_signal.is_some() {
                        let _ = ctx.first_hb_signal.take().unwrap().send(());
                    }
                    continue;
                }
            }
        } else {
            // 正常情况：使用注册时的 capabilities
            // 模型扫描按固定心跳间隔进行，避免每次心跳都调用 Ollama API
            let do_scan = heartbeat_count % MODEL_SCAN_HEARTBEAT_INTERVAL == 1;

            let current_models: Vec<String> = if do_scan || cached_models.is_none() {
                match ollama_client.list_models().await {
                    Ok(models) => {
                        cached_models = Some(models.clone());
                        models
                    }
                    Err(e) => {
                        warn!("Failed to list Ollama models for heartbeat: {}", e);
                        // 使用缓存或注册模型
                        cached_models
                            .clone()
                            .unwrap_or_else(|| registered_models.clone())
                    }
                }
            } else {
                // 使用缓存，不扫描 Ollama
                cached_models
                    .clone()
                    .unwrap_or_else(|| registered_models.clone())
            };

            // 取注册模型和当前模型的交集：
            // 服务端强制校验 accepted_models 必须是注册列表的子集
            let active_models: Vec<String> = registered_models
                .into_iter()
                .filter(|m| current_models.contains(m))
                .collect();

            if do_scan || heartbeat_count == 1 {
                info!("Accepted models (schedulable): {:?}", active_models);
            }

            // 新模型（不在注册列表中）不会被 accepted_models 包含，
            // 受限于服务端设计：capabilities_json 在注册时写入后不再更新，
            // 且心跳强制校验 accepted_models 必须是注册列表的子集。
            // 新模型需联系管理员更新数据库中 nodes.capabilities_json 后重启。

            // 检测模型删除：注册模型在当前 Ollama 中不再存在
            // 注意：不能通过 active_models 长度判断，因为新模型也会被加入
            let removed: Vec<&String> = session
                .capabilities
                .models
                .iter()
                .map(|m| &m.model)
                .filter(|reg_m| !current_models.contains(*reg_m))
                .collect();

            if !removed.is_empty() {
                if removed.len() == session.capabilities.models.len()
                    && (do_scan || heartbeat_count == 1)
                {
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
                } else if do_scan || heartbeat_count == 1 {
                    warn!(
                        "Some registered models no longer available in Ollama. \
                         Removed: {:?}, Remaining: {:?}",
                        removed, active_models
                    );
                }
            }

            active_models
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
                let was_excluded = ctx.is_excluded.load(Ordering::Acquire);
                let now_excluded = resp.node_status == "excluded";
                ctx.is_excluded.store(now_excluded, Ordering::Release);

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

                // 首次心跳完成后通知 main
                if let Some(tx) = ctx.first_hb_signal.take() {
                    let _ = tx.send(());
                }
            }
            Err(e) => {
                // 关键：检测服务端返回 401 → session 失效 → 触发自愈
                if NodeTokenError::is_session_invalid(&e) {
                    error!(
                        "Heartbeat: session invalid on server (401), triggering re-registration"
                    );
                    ctx.session_lost.store(true, Ordering::Release);
                    // 首次心跳 401 也需通知 main，避免无限等待
                    if let Some(tx) = ctx.first_hb_signal.take() {
                        let _ = tx.send(());
                    }
                    // 立即退出，让 main.rs 处理重注册流程
                    return;
                }

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
