//! node-token - KeyCompute 个人 PC 节点客户端
//!
//! 运行在个人 PC 上，负责连接本机 Ollama、主动轮询任务并提交结果。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use tracing::{debug, info, warn};

use node_token::NodeTokenConfig;
use node_token::client::{KeyComputeClient, OllamaClient};
use node_token::load_config;
use node_token::runtime::{
    HeartbeatContext, PollLoopConfig, TaskExecutor, heartbeat_loop, poll_loop, register_node,
    try_load_session,
};
use node_token::storage::LocalStorage;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 初始化日志（不得输出 token 明文，AGENTS.md 第 729 行）
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("node_token=info".parse()?),
        )
        .init();

    info!("node-token starting");

    // 2. 加载配置
    let config = load_config()?;
    let client_instance_id = node_token::config::generate_client_instance_id();
    info!("Configuration loaded successfully");
    info!("Server URL: {}", config.server_url);
    info!(
        "Client instance ID: {} (auto-generated from hostname)",
        client_instance_id
    );
    info!("Display name: {}", config.display_name);
    info!("Ollama URL: {}", config.ollama_url);

    // 3. 初始化客户端
    let client = Arc::new(KeyComputeClient::new(&config.server_url));
    let ollama_client = Arc::new(OllamaClient::new(&config.ollama_url));
    let storage = LocalStorage::new(config.data_dir.as_deref())?;

    // 4. 主状态机循环 — 支持运行时 401 自愈（AGENTS.md 第 36、714 行）
    //
    // 每次循环迭代：
    //   1. 加载本地 session 或重新注册
    //   2. 启动 heartbeat + poll
    //   3. 等待：Ctrl+C 退出 / 401 session 失效
    //   4. 401 时：清除 session.json → 回到步骤 1 重新注册
    //   5. 超过最大重注册次数 → 退出
    let max_reregisters = config.max_reregisters;
    let mut reregister_count: u32 = 0;

    loop {
        // ---- 阶段 1: 加载或注册 session ----
        let session = match try_load_session(&storage)? {
            Some(s) => {
                info!("Loaded existing session, skipping registration");
                s
            }
            None => {
                info!("Registering new node");
                // 注册带重试：网络短暂不可用时自动重试
                register_with_retry(&client, &ollama_client, &config, &storage).await?;

                match try_load_session(&storage)? {
                    Some(s) => s,
                    None => {
                        return Err(anyhow::anyhow!(
                            "Failed to load session after successful registration"
                        ));
                    }
                }
            }
        };

        client
            .set_session_token(session.session_token.clone())
            .await;

        // ---- 阶段 2: 初始化共享状态 ----
        let is_excluded = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::new(AtomicBool::new(false));
        let session_lost = Arc::new(AtomicBool::new(false));

        // ---- 阶段 3: 启动心跳循环 ----
        // 使用 oneshot channel 等待首次心跳完成，避免固定延迟等待
        let (first_hb_tx, first_hb_rx) = tokio::sync::oneshot::channel::<()>();

        let hb_client = client.clone();
        let hb_ollama = ollama_client.clone();
        let hb_session = session.clone();
        let hb_config = config.clone();
        let hb_excluded = is_excluded.clone();
        let hb_stop = stop_signal.clone();
        let hb_lost = session_lost.clone();
        let hb_ctx = HeartbeatContext {
            is_excluded: hb_excluded,
            stop_signal: hb_stop,
            session_lost: hb_lost,
            first_hb_signal: Some(first_hb_tx),
        };
        let heartbeat_handle = tokio::spawn(async move {
            heartbeat_loop(&hb_client, &hb_ollama, &hb_session, &hb_config, hb_ctx).await;
        });

        // 等待初始心跳完成（获取 is_excluded 状态和可能的 401）
        // 超时时间：心跳间隔 + 5 秒缓冲，确保即使第一次心跳缓慢也不会被遗漏
        let hb_timeout = Duration::from_secs(std::cmp::max(config.heartbeat_interval_secs, 5) + 5);
        match tokio::time::timeout(hb_timeout, first_hb_rx).await {
            Ok(Ok(())) => {
                debug!("First heartbeat completed");
            }
            _ => {
                warn!(
                    "First heartbeat did not complete within {:?}, proceeding anyway",
                    hb_timeout
                );
            }
        }

        // ---- 阶段 4: 检测初始心跳是否 401 ----
        if session_lost.load(Ordering::Acquire) {
            warn!("Heartbeat returned 401 during initial handshake, entering recovery");
            stop_signal.store(true, Ordering::Relaxed);
            let _ = heartbeat_handle.await;
            // 走 recovery 流程，不清除 reregister_count
            // 但下面的 recovery 代码会处理
        } else {
            // ---- 阶段 5: 启动 poll 循环 ----
            let max_concurrent = config.max_concurrent_tasks;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
            info!("Concurrency limit set to {} tasks", max_concurrent);

            let executor = Arc::new(TaskExecutor::new(
                client.clone(),
                ollama_client.clone(),
                session.clone(),
                session_lost.clone(),
            ));

            let poll_client = client.clone();
            let poll_session = session.clone();
            let poll_executor = executor;
            let poll_excluded = is_excluded.clone();
            let poll_stop = stop_signal.clone();
            let poll_lost = session_lost.clone();
            let poll_config = PollLoopConfig {
                excluded_check_interval: Duration::from_secs(
                    config.excluded_poll_check_interval_secs,
                ),
                poll_timeout_secs: session.poll_timeout_secs,
                concurrency_semaphore: semaphore,
            };
            let poll_handle = tokio::spawn(async move {
                poll_loop(
                    &poll_client,
                    &poll_session,
                    poll_executor,
                    poll_excluded,
                    poll_stop,
                    poll_config,
                    poll_lost,
                )
                .await;
            });

            // ---- 阶段 6: 等待事件（退出信号 或 session 失效）----
            let event = monitor_events(session_lost.clone()).await;

            // ---- 阶段 7: 停止当前循环 ----
            stop_signal.store(true, Ordering::Relaxed);
            let _ = tokio::join!(heartbeat_handle, poll_handle);

            // ---- 阶段 8: 处理事件 ----
            match event {
                Event::Shutdown => {
                    info!("Received shutdown signal, exiting");
                    break;
                }
                Event::SessionLost => {
                    // 继续到下面的 recovery 流程
                }
            }
        }

        // ---- 阶段 9: Session 失效恢复流程（两个路径共用）----
        reregister_count += 1;
        if reregister_count >= max_reregisters {
            return Err(anyhow::anyhow!(
                "Session re-registration failed {} consecutive times. \
                 Please check server connectivity and registration token validity.",
                max_reregisters
            ));
        }
        warn!(
            "Session invalidated (attempt {}/{}{}), clearing local session and re-registering...",
            reregister_count,
            max_reregisters,
            if reregister_count >= max_reregisters {
                ", max reached"
            } else {
                ""
            }
        );
        storage.clear_session()?;
        info!("Session cleared, re-registering...");
        continue;
    }

    info!("Node token stopped");
    Ok(())
}

/// 注册节点（带网络重试）
///
/// 注册 API 调用失败时自动重试最多 3 次，间隔指数退避（5s/10s/20s）。
/// 这与 `register_node` 内部的 `wait_for_models_ready`（等待 Ollama 模型就绪）
/// 互补，后者处理的是 Ollama 未就绪的场景，此处处理服务端短暂不可用。
async fn register_with_retry(
    client: &KeyComputeClient,
    ollama_client: &OllamaClient,
    config: &NodeTokenConfig,
    storage: &LocalStorage,
) -> anyhow::Result<()> {
    let max_attempts = 3;
    for attempt in 1..=max_attempts {
        match register_node(client, ollama_client, config, storage).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt == max_attempts => {
                return Err(anyhow::anyhow!(
                    "Node registration failed after {} attempts: {}",
                    max_attempts,
                    e
                ));
            }
            Err(e) => {
                let backoff = Duration::from_secs(5u64 * attempt as u64);
                warn!(
                    "Registration failed (attempt {}/{}): {}. Retrying in {}s...",
                    attempt,
                    max_attempts,
                    e,
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
    // 不可到达
    Ok(())
}

/// 等待退出信号（SIGTERM/SIGINT 或 Ctrl+C）
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
        }
    }

    #[cfg(windows)]
    {
        use tokio::signal::windows;

        let mut ctrl_c = windows::ctrl_c().expect("failed to install CTRL+C handler");
        ctrl_c.recv().await;
        info!("Received CTRL+C");
    }

    #[cfg(not(any(unix, windows)))]
    {
        warn!("No signal handling on this platform, waiting indefinitely");
        // 在不支持的平台上，简单等待
        tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
    }
}

/// 监控事件：等待退出信号 或 session 失效
///
/// 通过 tokio::select! 同时等待两个事件源。session_lost 以 100ms 间隔轮询
/// AtomicBool，因为该标志由 heartbeat/poll 循环在检测到 401 时设置。
async fn monitor_events(session_lost: Arc<AtomicBool>) -> Event {
    tokio::select! {
        _ = wait_for_signal() => Event::Shutdown,
        _ = async {
            loop {
                if session_lost.load(Ordering::Acquire) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        } => Event::SessionLost,
    }
}

/// 主循环事件
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    /// 用户按 Ctrl+C 或收到 SIGTERM/SIGINT
    Shutdown,
    /// heartbeat/poll 收到 401，session 失效
    SessionLost,
}
