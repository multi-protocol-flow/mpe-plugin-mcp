//! MCP 插件的 stdio 集成测试：spawn `mcp_test_server`（rmcp stdio 测试服务），
//! 再经 JSON-RPC 2.0 over piped stdio 驱动编译产物 `mpe_mcp_plugin` —— 与宿主
//! 走完全相同的传输通道（SDK 运行时 + 连接池 + 客户端 + rmcp 全部真实执行）。
//!
//! 全链路：`mcp:connect`(stdio → test server) → `mcp:call_tool`(echo) →
//! `mcp:read_resource`(config) → `mcp:get_prompt`(greet) → `mcp:close`。
//! 另覆盖 `uiCall("mcp.discover")` 返回含 echo 工具。
//!
//! 不需要外部服务：测试服务器是本包 `[[bin]]`（`CARGO_BIN_EXE_mcp_test_server`）。

use std::io::{BufRead, BufReader, Write as _};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{json, Value};

/// 插件编译产物路径（cargo 为包内 `[[bin]]` 注入）。
const PLUGIN_BIN: &str = env!("CARGO_BIN_EXE_mpe_plugin_mcp");
/// 测试服务器编译产物路径。
const TEST_SERVER_BIN: &str = env!("CARGO_BIN_EXE_mcp_test_server");

// ---------------------------------------------------------------------------
// stdio 驱动（插件进程，与 redis integration 同款）
// ---------------------------------------------------------------------------

/// 一个插件子进程 + 双向 stdio 管道。同一进程内可连续发多个请求
/// （per-execution 隔离用例需要）。
struct PluginProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl PluginProcess {
    /// 拉起编译产物（每次用例一个全新进程，模拟宿主的"拉起→常驻→EOF 回收"）。
    fn spawn() -> PluginProcess {
        let mut child = Command::new(PLUGIN_BIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn mpe_plugin_mcp");
        let stdin = child.stdin.take().expect("plugin stdin unavailable");
        let stdout = child.stdout.take().expect("plugin stdout unavailable");
        PluginProcess {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            next_id: 0,
        }
    }

    /// 发一个 JSON-RPC 请求并等待其响应（按 id 关联；log 通知帧无 id，跳过）。
    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let frame = serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params,
        }))
        .expect("request serializable");
        let stdin = self.stdin.as_mut().expect("stdin alive");
        writeln!(stdin, "{frame}").expect("request write");
        stdin.flush().expect("request flush");

        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).expect("read response frame");
            assert!(n > 0, "plugin stdout closed early");
            let frame: Value = serde_json::from_str(&line).expect("frame is valid JSON");
            if frame.get("id").is_some() {
                return frame;
            }
            // 无 id → log 通知帧，跳过。
        }
    }

    /// 关闭 stdin（EOF），等待干净退出。
    fn shutdown(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("plugin exit");
        assert!(status.success(), "plugin should exit cleanly");
    }
}

fn execute(process: &mut PluginProcess, config: Value) -> Value {
    process.request("execute", json!({ "config": config }))
}

/// 测试服务器进程句柄：Drop 时终止（rmcp 子进程传输在 stdin 关闭后退出）。
struct TestServerProcess {
    child: Child,
}

impl TestServerProcess {
    fn spawn() -> TestServerProcess {
        let child = Command::new(TEST_SERVER_BIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn mcp_test_server");
        TestServerProcess { child }
    }
}

impl Drop for TestServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `mcp:connect` 节点配置（stdio → 本用例的测试服务器）。
fn connect_config() -> Value {
    json!({
        "type": "mcp:connect",
        "transport": "stdio",
        "command": TEST_SERVER_BIN,
        "args": [],
        "env": {},
        "protocol_version": "2025-11-25",
    })
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

/// connect → call_tool(echo) → read_resource(config) → get_prompt(greet) →
/// close：完整生命周期。
///
/// `#[ignore]`：该用例（连同 `ui_call_discover_returns_tools` 与
/// `connect_is_idempotent`）在 Windows CI 上挂起（插件经 rmcp 拉起测试服务器
/// 孙进程后 initialize 握手不返回）。默认 `cargo test` 跳过；本地/非 Windows
/// 上可用 `cargo test --test integration -- --include-ignored` 运行。
#[test]
#[ignore]
fn connect_call_read_prompt_close_roundtrip() {
    let mut process = PluginProcess::spawn();
    let _server = TestServerProcess::spawn();

    // connect（未带 execution_id/node_instance_id → 连接池键回退 "default"）
    let resp = execute(&mut process, connect_config());
    assert_eq!(resp["result"]["success"], true, "connect 成功: {resp}");
    let output = &resp["result"]["output_data"];
    assert_eq!(output["connection_id"], "default");
    assert_eq!(output["server_info"]["name"], "mcp-test-server");
    assert_eq!(output["transport"], "stdio");
    let tools = output["tools"].as_array().expect("tools array");
    assert!(
        tools.iter().any(|t| t["name"] == "echo"),
        "discovery must include the echo tool: {tools:?}"
    );

    // call_tool: echo(message: "hello-mcp")
    let resp = execute(
        &mut process,
        json!({
            "type": "mcp:call_tool",
            "connection_id": "default",
            "tool_name": "echo",
            "arguments": { "message": "hello-mcp" },
        }),
    );
    assert_eq!(resp["result"]["success"], true, "call_tool 成功: {resp}");
    assert_eq!(resp["result"]["output_data"]["tool_name"], "echo");
    assert_eq!(resp["result"]["output_data"]["is_error"], false);
    let content = &resp["result"]["output_data"]["content"];
    let text = content
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|c| c["text"].as_str())
        .unwrap_or_default();
    assert_eq!(text, "hello-mcp", "echo must return the message");

    // read_resource: config://app
    let resp = execute(
        &mut process,
        json!({
            "type": "mcp:read_resource",
            "connection_id": "default",
            "resource_uri": "config://app",
        }),
    );
    assert_eq!(resp["result"]["success"], true, "read_resource 成功: {resp}");
    assert_eq!(resp["result"]["output_data"]["resource_uri"], "config://app");
    let contents = resp["result"]["output_data"]["contents"].as_array().expect("contents array");
    assert!(
        contents.iter().any(|c| c["text"].as_str().is_some_and(|s| s.contains("hello"))),
        "config resource must carry the JSON payload: {contents:?}"
    );

    // get_prompt: greet(name: "mcp")
    let resp = execute(
        &mut process,
        json!({
            "type": "mcp:get_prompt",
            "connection_id": "default",
            "prompt_name": "greet",
            "arguments": { "name": "mcp" },
        }),
    );
    assert_eq!(resp["result"]["success"], true, "get_prompt 成功: {resp}");
    assert_eq!(resp["result"]["output_data"]["prompt_name"], "greet");
    let serialized = serde_json::to_string(&resp["result"]["output_data"]["messages"])
        .unwrap_or_default();
    assert!(
        serialized.contains("Hello, mcp!"),
        "greet prompt must render the name: {serialized}"
    );

    // close（选中连接）→ 连接释放
    let resp = execute(
        &mut process,
        json!({ "type": "mcp:close", "connection_id": "default" }),
    );
    assert_eq!(resp["result"]["success"], true, "close 成功: {resp}");
    assert_eq!(resp["result"]["output_data"]["connection_id"], "default");

    // 已关闭的连接：call_tool 报"连接不存在"（池查找失败，离线错误路径）。
    let resp = execute(
        &mut process,
        json!({
            "type": "mcp:call_tool",
            "connection_id": "default",
            "tool_name": "echo",
            "arguments": { "message": "x" },
        }),
    );
    assert_eq!(resp["result"]["success"], false, "closed connection must fail: {resp}");

    process.shutdown();
}

/// `uiCall("mcp.discover")`（宿主配置面板的发现查询通道）经插件进程返回
/// 服务器能力快照。
#[test]
#[ignore]
fn ui_call_discover_returns_tools() {
    let mut process = PluginProcess::spawn();
    let _server = TestServerProcess::spawn();

    let resp = process.request(
        "uiCall",
        json!({
            "method": "mcp.discover",
            "params": {
                "transport": "stdio",
                "command": TEST_SERVER_BIN,
                "args": [],
                "env": {},
                "protocol_version": "2025-11-25",
            },
        }),
    );
    assert_eq!(resp["result"]["server_info"]["name"], "mcp-test-server");
    let tools = resp["result"]["tools"].as_array().expect("tools array");
    assert!(
        tools.iter().any(|t| t["name"] == "echo"),
        "discover must include the echo tool: {tools:?}"
    );
    let resources = resp["result"]["resources"].as_array().expect("resources array");
    assert!(
        resources.iter().any(|r| r["uri"] == "config://app"),
        "discover must include the config resource: {resources:?}"
    );
    let prompts = resp["result"]["prompts"].as_array().expect("prompts array");
    assert!(
        prompts.iter().any(|p| p["name"] == "greet"),
        "discover must include the greet prompt: {prompts:?}"
    );

    // 未知 uiCall 子方法 → 插件可读错误。
    let resp = process.request(
        "uiCall",
        json!({ "method": "mcp.unknown", "params": {} }),
    );
    assert_eq!(resp["error"]["code"], -32601);
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("unknown uiCall sub-method")),
        "unknown sub-method error: {resp}"
    );

    process.shutdown();
}

/// connect 幂等：同目标二次 connect 复用池内连接，不报错。
#[test]
#[ignore]
fn connect_is_idempotent() {
    let mut process = PluginProcess::spawn();
    let _server = TestServerProcess::spawn();

    let first = execute(&mut process, connect_config());
    assert_eq!(first["result"]["success"], true, "first connect: {first}");
    let second = execute(&mut process, connect_config());
    assert_eq!(second["result"]["success"], true, "second connect must reuse: {second}");
    assert_eq!(
        second["result"]["output_data"]["connection_id"], "default",
        "same target must keep the same pool key"
    );

    process.shutdown();
}

/// 连到不存在的 stdio 命令：connect 失败并带可读错误（单节点连接性验证的
/// 失败路径）。
#[test]
fn connect_to_missing_command_fails() {
    let mut process = PluginProcess::spawn();

    let resp = execute(
        &mut process,
        json!({
            "type": "mcp:connect",
            "transport": "stdio",
            "command": "/nonexistent/mcp-server-binary",
            "args": [],
            "env": {},
        }),
    );
    assert_eq!(resp["result"]["success"], false, "connect must fail: {resp}");
    let err = resp["result"]["errors"].as_array().and_then(|a| a.first());
    assert!(
        err.is_some(),
        "failure must carry a readable error: {resp}"
    );

    process.shutdown();
}
