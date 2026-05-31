//! node-token - KeyCompute 个人 PC 节点客户端
//!
//! 运行在个人 PC 上，负责连接本机 Ollama、主动轮询任务并提交结果。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use tracing::info;
#[allow(unused_imports)] // 仅在不支持的平台上使用
use tracing::warn;

use node_token::client::{KeyComputeClient, OllamaClient};
use node_token::load_config;
use node_token::runtime::{
    PollLoopConfig, TaskExecutor, heartbeat_loop, poll_loop, register_node, try_load_session,
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

    // 4. 恢复本地持久化的 session（AGENTS.md 第 36、714 行）
    let session = match try_load_session(&storage)? {
        Some(s) => {
            info!("Loaded existing session, skipping registration");
            s
        }
        None => {
            // 无本地 session，执行新注册
            info!("Registering new node");
            // register_node 内部已经保存了 session 到本地存储
            register_node(&client, &ollama_client, &config, &storage).await?;

            // 从存储中加载刚保存的 session
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

    // 设置客户端 session token
    client
        .set_session_token(session.session_token.clone())
        .await;

    // 5. 初始化共享状态
    let is_excluded = Arc::new(AtomicBool::new(false));
    let stop_signal = Arc::new(AtomicBool::new(false));

    // 6. 启动心跳循环（heartbeat 会更新 is_excluded）
    let heartbeat_client = client.clone();
    let heartbeat_ollama = ollama_client.clone();
    let heartbeat_session = session.clone();
    let heartbeat_config = config.clone();
    let heartbeat_excluded = is_excluded.clone();
    let heartbeat_stop = stop_signal.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(
            &heartbeat_client,
            &heartbeat_ollama,
            &heartbeat_session,
            &heartbeat_config,
            heartbeat_excluded,
            heartbeat_stop,
        )
        .await;
    });

    // 7. 等待心跳完成一次，获取初始节点状态
    // 因为心跳循环第一次立即触发，2 秒足够获取初始状态
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 8. 创建并发控制信号量
    let max_concurrent_tasks = config.max_concurrent_tasks;
    let concurrency_semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_tasks));
    info!("Concurrency limit set to {} tasks", max_concurrent_tasks);

    // 9. 启动轮询循环（如果节点未 excluded）
    let executor = Arc::new(TaskExecutor::new(
        client.clone(),
        ollama_client.clone(),
        session.clone(),
    ));
    let poll_client = client.clone();
    let poll_session = session.clone();
    let poll_executor = executor;
    let poll_excluded = is_excluded.clone();
    let poll_stop = stop_signal.clone();
    let poll_config = PollLoopConfig {
        excluded_check_interval: Duration::from_secs(config.excluded_poll_check_interval_secs),
        poll_timeout_secs: session.poll_timeout_secs,
        concurrency_semaphore,
    };
    let poll_handle = tokio::spawn(async move {
        poll_loop(
            &poll_client,
            &poll_session,
            poll_executor,
            poll_excluded,
            poll_stop,
            poll_config,
        )
        .await;
    });

    // 10. 等待退出信号
    wait_for_signal().await;
    info!("Received shutdown signal, stopping...");
    stop_signal.store(true, Ordering::Relaxed);

    // 10. 等待循环结束（已领取任务尽力提交，AGENTS.md 第 730 行）
    let _ = tokio::join!(heartbeat_handle, poll_handle);

    info!("Node token stopped");
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
