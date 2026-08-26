# MPE MCP 插件

![CI](https://github.com/multi-protocol-flow/mpe-plugin-mcp/actions/workflows/ci.yml/badge.svg)

> **定位**：本插件是 MPE 的**官方协议插件**之一——不依赖宿主仓库的任何代码，
> 只依赖公开的 `mpe-plugin-sdk`（Sidecar 进程 + JSON-RPC over stdio，git tag `v0.1.0`）。
> 经 rmcp 为 MPE 提供 Model Context Protocol（MCP）操作，支持
> streamable-http 与 stdio 两种传输。
>
> 本仓库独立于宿主（宿主仓库 `.gitignore` 忽略 `/plugins/`、宿主从不构建它），
> 由本仓库自己的 CI（GitHub Actions → Release 产物）构建与发布。

---

## 0. 一分钟理解插件如何工作

```
宿主 (mpe / mpe-cli)                    插件进程 (本 crate)
   │  扫描 plugins/ 目录                       │
   │  ── describe ───────────────────────────► │  返回节点描述（类型、端口、config schema）
   │  ◄─────────── 节点清单 ──────────────────  │
   │  ── execute(config, execution_id) ──────► │  执行 MCP 操作
   │  ◄─────────── 结果 / 变量更新 ───────────  │
   │  ── flowEnded(execution_id) ────────────► │  释放 per-execution 连接池
```

- **传输**：stdin/stdout，一行一个 JSON（JSON-RPC 2.0，LF 分隔）
- **驻留**：`capabilities.streaming: true` → 进程常驻，连接池跨执行复用
- **单节点验证**：`mcp:connect` 声明 `capabilities.single_node: true` →
  宿主 `mpe run-node` / GUI 测试按钮无需改宿主代码即可连通性验证
- **无共享内存**：插件是独立进程，宿主只能通过 JSON 传值

## 1. 项目结构

```
mpe-plugin-mcp/
├── Cargo.toml            # 独立 package，不依赖宿主 workspace
├── plugin.json           # 宿主扫描用清单（启动描述、驻留模式）
├── .github/workflows/ci.yml  # 三平台构建 + 离线测试 + Release 打包
├── src/
│   ├── main.rs           # 先安装 rustls ring CryptoProvider，再调 __plugin_main::<McpPlugin>
│   ├── lib.rs            # McpPlugin：describe/execute/validate + 内嵌 inline 配置前端
│   ├── client.rs         # rmcp 客户端（streamable-http + stdio 双传输）
│   ├── i18n.rs           # MPE_LOCALE 驱动的中英文案
│   ├── pool.rs           # per-execution 连接池（execution_id, connection_id）
│   ├── frontend/
│   │   └── panel.html    # 内嵌配置面板（include_str! 嵌入），MCP 服务发现 UI
│   ├── nodes/
│   │   ├── mod.rs        # 各节点 execute 分派
│   │   ├── connect.rs    # mcp:connect
│   │   ├── call_tool.rs  # mcp:call_tool
│   │   ├── read_resource.rs  # mcp:read_resource
│   │   ├── get_prompt.rs # mcp:get_prompt
│   │   └── close.rs      # mcp:close
│   └── bin/
│       └── test_server.rs    # stdio MCP 测试服务器（echo 工具 / config 资源 / greet 提示词）
└── tests/
    ├── roundtrip.rs      # 离线 stdio JSON-RPC 往返测试
    └── integration.rs    # 全链路：spawn 测试服务器 + 插件，走真实 stdio（无需外部 MCP 服务器）
```

## 2. 节点类型

| type_id | 端口 | 说明 |
|---------|------|------|
| `mcp:connect` | in/true/false | 建立 MCP 连接。`transport` 枚举 `streamable_http\|stdio`；streamable_http：`url`、`protocol_version`（默认 `2025-11-25`）、`headers`；stdio：`command`、`args`、`env`。发现缓存字段 `discovered_tools`/`discovered_resources`/`discovered_prompts`。声明 `single_node: true` |
| `mcp:call_tool` | in/true/false | 调用工具。必填 `connection_id`（宿主 `x-node-selector` 渲染为对 `mcp:connect` 节点的选择器）、`tool_name`；`arguments` 按工具的 input_schema |
| `mcp:read_resource` | in/true/false | 读取资源。必填 `connection_id`、`resource_uri` |
| `mcp:get_prompt` | in/true/false | 获取提示词。必填 `connection_id`、`prompt_name`；可选 `arguments` |
| `mcp:close` | in/out | 释放连接（可选 `connection_id` 选中单个，未选释放整个 execution） |

五个节点都携带内嵌配置面板（HTML 嵌入二进制）；面板经宿主 `plugin_ui_call`
中继驱动运行时发现（`uiCall("mcp.discover")`）。

`mcp:call_tool`/`mcp:read_resource`/`mcp:get_prompt`/`mcp:close` 的
`connection_id` 字段由宿主 `x-node-selector` 渲染为流程内 `mcp:connect` 节点
的选择器；选中后宿主把 connect 节点的 instance id 注入 config，插件据此从
连接池取连接。

## 3. Cargo.toml（独立构建）

```toml
[package]
name = "mpe-plugin-mcp"
version = "0.1.0"
edition = "2021"

[dependencies]
mpe-plugin-sdk = { git = "https://github.com/multi-protocol-flow/mpe-plugin-sdk.git", tag = "v0.1.0" }
rmcp = { version = "1.5", default-features = false, features = ["client", "transport-streamable-http-client-reqwest", "transport-child-process", "server", "transport-io"] }
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
rustls = { version = "0.23", features = ["ring"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }

[[bin]]
name = "mpe_plugin_mcp"
path = "src/main.rs"

[[bin]]
name = "mcp_test_server"
path = "src/bin/test_server.rs"
```

注意：`main.rs` 不能用 `mpe_plugin_main!` 宏（宏会生成自己的 `fn main`，
嵌套在我们的 main 里永不执行）；它先安装 rustls ring CryptoProvider，
再直接调用 `__plugin_main::<McpPlugin>`。

## 4. 构建与测试

> **plugin.json 的 `entry.command`**：值为市场契约形式的相对路径 `./mpe_plugin_mcp`。
> 当前宿主以自身 CWD 解析该命令（不 chdir 到插件目录）。仓库内开发调试时，若需宿主
> 直接拉起本插件，可把 command 改为本机绝对路径，或在插件目录下启动宿主。

```bash
# 构建 release 产物（宿主通过 plugin.json entry.command 拉起）
# 仓库根（mpe-plugin-mcp）下执行
cargo build --release

# 离线测试（无需外部 MCP 服务器）：
# roundtrip = stdio JSON-RPC 往返；integration 经 CARGO_BIN_EXE 拉起内置
# mcp_test_server，走真实 stdio 传输
cargo test
```

## 5. 安装与验证（宿主侧）

```bash
# 开发期：构建产物放仓库根（宿主按 MPE_PLUGIN_DIR 或数据目录 plugins 扫描）
# 仓库根（mpe-plugin-mcp）下执行
cargo build --release

# 单节点连接性验证（capability 驱动，无需宿主白名单改动）
mpe run-node '{"type":"mcp:connect","transport":"stdio","command":"/path/to/your/mcp/server"}'
```

全链路流程由本仓库测试覆盖（spawn 测试服务器走真实 stdio），也可在 MPE
编辑器中组装。

## 6. 设计要点

- **rmcp 客户端**：两种传输均由 rmcp SDK 提供——streamable-http 走
  rustls 后端 reqwest，stdio 走子进程（command/args/env）。
- **连接池隔离**：连接按 `(execution_id, connection_id)` 复合键隔离，
  并发执行互不串扰。
- **发现缓存**：`mcp:connect` 把发现的 tools/resources/prompts 缓存回
  config，面板在 `mcp.discover` 后无需重查服务器即可渲染。
- **显式成功路由**：操作节点返回 `next_ports: ["true"]`（`ok_result`），
  宿主 `resolve_next_ids` 据此串联下游；空列表 = 分支终止。失败由宿主
  `on_error` 策略（默认 RouteToFalse）处理——插件从不自报 false。
- **内嵌配置面板**：面板 HTML 经 `include_str!` 嵌入二进制并随 `describe`
  下发，无需外部资源。
- **全离线测试**：`integration.rs` spawn 内置 stdio 测试服务器（echo 工具 /
  config 资源 / greet 提示词）与插件，走真实 SDK 运行时 + 连接池 + rmcp，
  无需外部 MCP 服务器。

## 7. 与宿主的关系

- 依赖：仅 `mpe-plugin-sdk`（`mpe-plugin-sdk::protocol` wire 契约），**绝不**依赖
  `flow-engine-core` 等宿主类型。
- 交互：describe/execute/validate/shutdown RPC + log 通知 + flowEnded 通知。
- 宿主适配点：`capabilities.single_node` 由宿主 `execute_single_node` /
  `mpe run-node` 的能力门读取（`NodePlugin::supports_single_node()` +
  `SidecarPlugin` 透传），插件侧无需感知宿主实现细节。
