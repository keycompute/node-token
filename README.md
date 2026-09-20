<div align="center">

# node-token

<p align="center">
  <a href="./README.md">English</a> |
  <a href="./README.zh-CN.md">简体中文</a> |
  <a href="./README.zh-TW.md">繁體中文</a> |
  <a href="./README.es.md">Español</a> |
  <a href="./README.ar.md">العربية</a>
</p>

**KeyCompute personal PC node client — bring your own compute**

<p align="center">
  <a href="https://github.com/keycompute/node-token/stargazers"><img src="https://img.shields.io/github/stars/keycompute/node-token?style=social" alt="GitHub Stars" /></a>
  <a href="https://github.com/keycompute/node-token/issues"><img src="https://img.shields.io/github/issues/keycompute/node-token" alt="GitHub Issues" /></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/License-GPLv3-blue.svg" alt="GPLv3 License" /></a>
  <a href="./CONTRIBUTING.md"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen" alt="PRs Welcome" /></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.92%2B-orange?logo=rust" alt="Rust Version" /></a>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#usage">Usage</a>
</p>

</div>

---

## Overview

`node-token` is a lightweight Rust client that runs on personal PCs and connects them to the [KeyCompute](https://github.com/keycompute/keycompute) platform as compute nodes. It polls the server for tasks, executes them on a local Ollama instance, and submits results back — all without requiring a public IP address.

---

## Features

- **Pull-based polling**: works behind NAT and home networks with no public IP required
- **Local Ollama execution**: runs models hosted on Ollama directly on your hardware
- **Automatic recovery**: persists session state locally and resumes after restarts
- **Heartbeat keepalive**: periodic heartbeats maintain node availability
- **Graceful shutdown**: stops accepting new tasks on exit while completing in-flight work
- **Excluded node handling**: mirrors server-side exclusion status and continues low-frequency heartbeat for admin visibility

---

## Prerequisites

| Component | Version |
|:---|:---|
| Rust | ≥ 1.92 |
| Ollama | Latest |

> You need a running Ollama instance with at least one model pulled. The client scans local models on startup and reports them during registration.

---

## Quick Start

### Install Ollama

```bash
# Linux
curl -fsSL https://ollama.com/install.sh | sh

# Pull a model
ollama pull gemma3:270m
```

### Build and Run node-token

```bash
# Clone and build
git clone https://github.com/keycompute/node-token.git
cd node-token
cp config.example.toml config.toml
# Edit config.toml with your KeyCompute server URL and registration token

# Build
cargo build --release

# Run
./target/release/node-token
```

### Docker

Using `docker-compose.yml` (recommended, includes Ollama and model pre-warming):

```bash
# Create .env from template (edit NODE_TOKEN__REGISTRATION_TOKEN)
cp .env.example .env

# Start Ollama + node-token
docker compose up -d

# Follow logs
docker compose logs -f
```

Run node-token standalone (requires an existing Ollama instance):

```bash
# Build image
docker build -t node-token .

# Create data volume
docker volume create node_token_data

# Run (use --network host to reach host Ollama)
docker run -d \
  --name node-token \
  --network host \
  -v node_token_data:/data \
  -e NODE_TOKEN__SERVER_URL="http://keycompute-server:3000" \
  -e NODE_TOKEN__REGISTRATION_TOKEN="your-registration-token" \
  -e NODE_TOKEN__DISPLAY_NAME="My PC Node" \
  -e NODE_TOKEN__OLLAMA_URL="http://localhost:11434" \
  node-token
```

---

## Configuration

Configuration is loaded from `config.toml` (or a path set via the `NODE_TOKEN_CONFIG` environment variable). Environment variables with the `NODE_TOKEN__` prefix override file values.

| Variable | Description | Default | Required |
|:---|:---|:---|:---:|
| `server_url` | KeyCompute server URL | `http://localhost:3000` | ✅ |
| `registration_token` | Registration token from KeyCompute admin | — | ✅ |
| `display_name` | Human-readable node name | — | ✅ |
| `ollama_url` | Local Ollama API endpoint | `http://localhost:11434` | ⚪ |
| `heartbeat_interval_secs` | Heartbeat interval in seconds | `30` | ⚪ |
| `excluded_poll_check_interval_secs` | Poll check interval when excluded | `30` | ⚪ |
| `data_dir` | Local data directory for session persistence | `~/.local/share/node-token` | ⚪ |

**Environment variable mapping**: `NODE_TOKEN__SERVER_URL`, `NODE_TOKEN__REGISTRATION_TOKEN`, etc.

> The `registration_token` and `session_token` are never logged in plaintext.

---

## Usage

Once `node-token` is registered and running, users can send requests via the KeyCompute API using the `node:` model prefix:

```bash
curl -s http://your-keycompute-server:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-xxx" \
  -d '{
    "model": "node:gemma3:270m",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": false
  }'
```

- `node:<model>` routes the request to the node pool (non-streaming only)
- `<model>` (without prefix) routes to the normal provider account path

---

## How It Works

```text
┌─────────────┐     poll tasks      ┌──────────────────┐
│  node-token │ ◄────────────────── │  KeyCompute       │
│  (your PC)  │ ──────────────────► │  Server           │
│             │   heartbeat/complete│                   │
│     │       │                     │        │          │
│     │ call  │                     │        │ enqueue  │
│     ▼       │                     │        ▼          │
│  ┌───────┐  │                     │  ┌──────────┐    │
│  │Ollama │  │                     │  │ User API │    │
│  └───────┘  │                     │  │ Requests │    │
└─────────────┘                     └──┴──────────┴────┘
```

1. `node-token` registers with the KeyCompute server, reporting available Ollama models
2. It sends periodic heartbeats to maintain session liveness
3. It long-polls for tasks matching its accepted models
4. On receiving a task, it calls the local Ollama instance and submits the result
5. If excluded by the server (e.g., too many failures), it stops polling but continues low-frequency heartbeat

---

## Development

```bash
# Build
cargo build --release

# Run tests
cargo test --lib
cargo test --tests

# Code checks
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

---

## Project Structure

```text
node-token/
├── src/
│   ├── main.rs              # Program entry point, signal handling
│   ├── config.rs            # Configuration management
│   ├── error.rs             # Error types
│   ├── lib.rs               # Library root
│   ├── client/              # HTTP clients
│   │   ├── api.rs           # KeyCompute API client
│   │   └── ollama.rs        # Ollama HTTP client
│   ├── protocol/            # Protocol types (copied from keycompute-types)
│   │   ├── types.rs         # Node protocol DTOs
│   │   └── ollama.rs        # Ollama API types
│   ├── runtime/             # Core runtime logic
│   │   ├── register.rs      # Registration logic
│   │   ├── heartbeat.rs     # Heartbeat loop
│   │   ├── poll.rs          # Polling loop
│   │   └── executor.rs      # Task executor
│   └── storage/             # Local persistence
│       └── mod.rs           # Session storage
├── tests/                   # Integration tests
├── benches/                 # Benchmarks
├── config.example.toml
├── .env.example
└── Cargo.toml
```

---

## License

This project is open sourced under the [GNU GPLv3](LICENSE) License.

---

<div align="center">

### 💖 Thanks for using node-token

If this project helps you, feel free to give it a ⭐️ star.

**[Quick Start](#quick-start)** • **[Report Issues](https://github.com/keycompute/node-token/issues)** • **[Latest Releases](https://github.com/keycompute/node-token/releases)**

</div>
# Native Chat protocol (phase 1)

The node advertises `node.native.v1` Chat support for the configured Ollama origin at `/v1/chat/completions`. Phase 1 is non-streaming only (`stream: true` is rejected), accepts no caller headers, preserves the complete JSON request and response, and bounds each body to 1 MiB. Only an explicit safe response-header allowlist is returned. Messages, Responses, streaming events, and stateful features require a later server/client upgrade and are not advertised.
