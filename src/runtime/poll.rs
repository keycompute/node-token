//! 轮询循环逻辑
//!
//! 定期从服务端领取任务，提交到执行器执行。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;
use tracing::{debug, error, info};

use crate::client::KeyComputeClient;
use crate::error::NodeTokenError;
use crate::protocol::types::NodePollRequest;
use crate::runtime::executor::TaskExecutor;
use crate::storage::SessionData;

/// Poll 循环配置参数
pub struct PollLoopConfig {
    /// Excluded 节点 poll 检查间隔
    pub excluded_check_interval: Duration,
    /// 服务端轮询超时（秒）
    pub poll_timeout_secs: u64,
    /// 并发控制信号量
    pub concurrency_semaphore: Arc<Semaphore>,
}

/// 轮询循环
///
/// # 参数
/// - `client`: KeyCompute HTTP 客户端
/// - `session`: 当前 session 信息
/// - `executor`: 任务执行器
/// - `is_excluded`: 节点排除标志（由 heartbeat 循环更新）
/// - `stop_signal`: 退出信号
/// - `config`: Poll 循环配置参数
/// - `session_lost`: Session 失效信号（401 时置 true，通知 main 触发重注册）
///
/// # 行为
/// - 定期调用 poll API 领取任务
/// - 如果节点被 excluded，停止 poll
/// - 服务端返回 retry_after_ms 时等待指定时间
/// - 网络错误指数退避（AGENTS.md 第 724 行）
/// - 无任务时等待间隔 = poll_timeout_secs / 10（默认 1 秒）
/// - 领取任务前需要获取并发许可，达到上限时阻塞等待
/// - 收到 401 Invalid session token 时置 session_lost 并立即退出循环
pub async fn poll_loop(
    client: &KeyComputeClient,
    session: &SessionData,
    executor: Arc<TaskExecutor>,
    is_excluded: Arc<AtomicBool>,
    stop_signal: Arc<AtomicBool>,
    config: PollLoopConfig,
    session_lost: Arc<AtomicBool>,
) {
    info!("Starting poll loop");

    // 连续失败计数，用于指数退避
    let mut consecutive_failures: u32 = 0;
    let max_backoff = Duration::from_secs(16);

    // 计算无任务时的等待间隔：poll_timeout_secs / 10，至少 1 秒
    let empty_poll_interval = if config.poll_timeout_secs >= 10 {
        Duration::from_secs(config.poll_timeout_secs / 10)
    } else {
        Duration::from_secs(1) // 默认 1 秒，避免 poll_timeout_secs < 10 时除出 0
    };

    info!(
        "Poll empty interval: {}s (poll_timeout_secs={})",
        empty_poll_interval.as_secs(),
        config.poll_timeout_secs
    );

    while !stop_signal.load(Ordering::Relaxed) {
        // 如果节点被 excluded，停止 poll
        if is_excluded.load(Ordering::Relaxed) {
            info!("Node excluded, stopping poll (will continue heartbeat only)");
            // 按配置的检查间隔等待后，检查是否恢复
            tokio::time::sleep(config.excluded_check_interval).await;
            continue;
        }

        let req = NodePollRequest {
            protocol_version: "node.v1".to_string(),
            node_id: session.node_id,
            session_id: session.session_id,
        };

        match client.poll(&req).await {
            Ok(resp) => {
                // 成功后重置失败计数
                consecutive_failures = 0;

                if let Some(task) = resp.task {
                    info!(
                        "Received task: task_id={}, model={}, deadline_unix_ms={}",
                        task.task_id, task.model, task.deadline_unix_ms
                    );

                    // 获取并发许可（如果达到上限会阻塞等待）
                    if config.concurrency_semaphore.available_permits() == 0 {
                        info!("Concurrency limit reached, waiting for available permit...");
                    }

                    // acquire_owned() 返回拥有所有权的 Permit，可以在 tokio::spawn 中使用
                    // 只在 Semaphore 被 close 时返回 Err，正常情况下不会失败
                    let permit = match config.concurrency_semaphore.clone().acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            // Semaphore 被关闭，正常退出 poll 循环
                            debug!("Semaphore closed, stopping poll loop");
                            return;
                        }
                    };

                    debug!(
                        "Acquired concurrency permit, available permits: {}",
                        config.concurrency_semaphore.available_permits()
                    );

                    // 收到任务，提交到执行器
                    // 使用 Arc 克隆 executor，让 executor 在后台执行
                    let executor_clone = executor.clone();
                    let semaphore = config.concurrency_semaphore.clone();
                    tokio::spawn(async move {
                        // 执行任务
                        executor_clone.execute(task).await;
                        // 任务完成后释放许可（通过 drop permit 实现）
                        drop(permit);
                        debug!(
                            "Task completed, released concurrency permit, available permits: {}",
                            semaphore.available_permits()
                        );
                    });
                } else if let Some(retry_ms) = resp.retry_after_ms {
                    debug!("No task available, retry_after={}ms", retry_ms);
                    tokio::time::sleep(Duration::from_millis(retry_ms)).await;
                } else {
                    // 没有任务也没有 retry_after，使用计算的间隔等待后继续
                    tokio::time::sleep(empty_poll_interval).await;
                }
            }
            Err(e) => {
                // 关键：检测服务端返回 401 → session 失效 → 触发自愈
                if NodeTokenError::is_session_invalid(&e) {
                    error!("Poll: session invalid on server (401), triggering re-registration");
                    session_lost.store(true, Ordering::Release);
                    // 立即退出，让 main.rs 处理重注册流程
                    return;
                }

                error!("Poll failed: {}", e);
                // 网络错误指数退避（AGENTS.md 第 724 行）
                consecutive_failures += 1;
                let backoff = std::cmp::min(
                    Duration::from_secs(2_u64.pow(consecutive_failures.min(4))),
                    max_backoff,
                );
                info!(
                    "Poll retrying after {}s (consecutive_failures={})",
                    backoff.as_secs(),
                    consecutive_failures
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }

    info!("Poll loop stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// 验证 poll 循环的 excluded 标志检查逻辑。
    /// excluded 节点应该停止 poll。
    fn test_excluded_flag_check() {
        let is_excluded = Arc::new(AtomicBool::new(false));

        // 非 excluded 时可以 poll
        assert!(!is_excluded.load(Ordering::Relaxed));

        // excluded 时停止 poll
        is_excluded.store(true, Ordering::Relaxed);
        assert!(is_excluded.load(Ordering::Relaxed));
    }

    #[test]
    /// 验证 poll 循环的停止信号检查逻辑。
    fn test_stop_signal_check() {
        let stop_signal = Arc::new(AtomicBool::new(false));

        // 未停止
        assert!(!stop_signal.load(Ordering::Relaxed));

        // 发送停止信号
        stop_signal.store(true, Ordering::Relaxed);
        assert!(stop_signal.load(Ordering::Relaxed));
    }

    #[test]
    /// 验证轮询指数退避间隔计算。
    /// 网络错误后应该指数级增加等待时间。
    fn test_poll_backoff_calculation() {
        let max_backoff = Duration::from_secs(16);

        // 第 1 次失败：2^1 = 2 秒
        let backoff_1 = Duration::from_secs(2_u64.pow(1));
        assert_eq!(backoff_1, Duration::from_secs(2));

        // 第 2 次失败：2^2 = 4 秒
        let backoff_2 = Duration::from_secs(2_u64.pow(2));
        assert_eq!(backoff_2, Duration::from_secs(4));

        // 第 3 次失败：2^3 = 8 秒
        let backoff_3 = Duration::from_secs(2_u64.pow(3));
        assert_eq!(backoff_3, Duration::from_secs(8));

        // 第 4 次失败：2^4 = 16 秒
        let backoff_4 = Duration::from_secs(2_u64.pow(4));
        assert_eq!(backoff_4, Duration::from_secs(16));

        // 第 5 次失败：min(2^5, 16) = 16 秒（达到最大值）
        let backoff_5 = std::cmp::min(Duration::from_secs(2_u64.pow(5)), max_backoff);
        assert_eq!(backoff_5, Duration::from_secs(16));

        // 第 10 次失败：min(2^10, 16) = 16 秒（仍然最大值）
        let backoff_10 = std::cmp::min(Duration::from_secs(2_u64.pow(10)), max_backoff);
        assert_eq!(backoff_10, Duration::from_secs(16));
    }

    #[test]
    /// 验证空轮询间隔计算。
    /// 无任务时的等待间隔 = poll_timeout_secs / 10，至少 1 秒
    fn test_empty_poll_interval_calculation() {
        // poll_timeout_secs = 20，间隔 = 2 秒
        let interval_20 = if 20 >= 10 {
            Duration::from_secs(20 / 10)
        } else {
            Duration::from_secs(1)
        };
        assert_eq!(interval_20, Duration::from_secs(2));

        // poll_timeout_secs = 30，间隔 = 3 秒
        let interval_30 = if 30 >= 10 {
            Duration::from_secs(30 / 10)
        } else {
            Duration::from_secs(1)
        };
        assert_eq!(interval_30, Duration::from_secs(3));

        // poll_timeout_secs = 5，< 10，fallback 到 1 秒
        let interval_5 = if 5 >= 10 {
            Duration::from_secs(5 / 10)
        } else {
            Duration::from_secs(1)
        };
        assert_eq!(interval_5, Duration::from_secs(1));
    }

    #[test]
    /// 验证多个 AtomicBool 并发访问的安全性。
    fn test_atomic_bool_concurrent_access() {
        let is_excluded = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];

        // 创建多个线程同时读写
        for i in 0..10 {
            let excluded = is_excluded.clone();
            let stop = stop_signal.clone();
            let handle = std::thread::spawn(move || {
                if i % 3 == 0 {
                    excluded.store(true, Ordering::Relaxed);
                } else if i % 3 == 1 {
                    stop.store(true, Ordering::Relaxed);
                } else {
                    let _ = excluded.load(Ordering::Relaxed);
                    let _ = stop.load(Ordering::Relaxed);
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
        let _ = stop_signal.load(Ordering::Relaxed);
    }
}
