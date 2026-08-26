//! Offline stdio JSON-RPC roundtrip tests against the compiled
//! `mpe_mcp_plugin` binary — the exact transport shape the host uses:
//! JSON-RPC 2.0 requests on stdin, LF-framed responses on stdout.
//!
//! Fully offline: no MCP server, no network. Every `execute` case
//! short-circuits before any socket/process I/O — call_tool-without-connect
//! fails at the pool lookup, unknown/missing type fails at the dispatch
//! match, and close on an empty pool is an idempotent no-op.
//!
//! Wire notes mirrored from `mpe-plugin-sdk/tests/roundtrip.rs`:
//! - The runtime spawns one task per `execute` and correlates responses by
//!   request `id`; responses may interleave with other traffic.
//! - Failing node executions emit an id-less `log` notification frame on
//!   stdout *before* their response. Readers must skip id-less frames and
//!   collect only frames carrying an `id`.
//! - Notifications (no `id`) never produce a response.
//! - A clean EOF on stdin (dropped write end) exits the loop with code 0.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::Value;

/// Path to the compiled plugin binary (set by cargo for `[[bin]]` targets).
const PLUGIN_BIN: &str = env!("CARGO_BIN_EXE_mpe_plugin_mcp");

/// The 5 node type_ids the plugin must describe (byte-sorted — the test
/// compares against a sorted copy).
const EXPECTED_TYPE_IDS: [&str; 5] = [
    "mcp:call_tool",
    "mcp:close",
    "mcp:connect",
    "mcp:get_prompt",
    "mcp:read_resource",
];

/// A spawned plugin process with piped stdio.
struct PluginProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl PluginProcess {
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

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let frame = serde_json::to_string(&serde_json::json!({
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

    fn shutdown(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("plugin exit");
        assert!(status.success(), "plugin should exit cleanly");
    }
}

/// `describe` returns exactly the 5 expected node types with inline
/// frontends and the single_node capability on `mcp:connect`.
#[test]
fn describe_lists_all_nodes() {
    let mut process = PluginProcess::spawn();
    let resp = process.request("describe", serde_json::json!({}));
    // SDK describe answers a bare array of NodeDescriptions (array shape).
    let nodes = resp["result"].as_array().expect("describe nodes array");

    let mut type_ids: Vec<&str> = nodes
        .iter()
        .filter_map(|n| n["type_id"].as_str())
        .collect();
    type_ids.sort_unstable();
    assert_eq!(type_ids, EXPECTED_TYPE_IDS, "all 5 mcp nodes described");

    for node in nodes {
        let frontend = &node["frontend"];
        assert_eq!(frontend["type"], "inline", "every node carries an inline panel");
        assert!(
            frontend["content"].as_str().is_some_and(|c| c.len() >= 100),
            "inline panel content must be non-trivial"
        );
        assert_eq!(node["category"], "mcp");
        assert_eq!(node["color"], "#F59E0B");
        assert!(
            node["default_config"].get("type").is_none(),
            "default_config must not carry a `type` discriminator"
        );
    }

    let connect = nodes
        .iter()
        .find(|n| n["type_id"] == "mcp:connect")
        .expect("mcp:connect described");
    assert_eq!(connect["capabilities"]["single_node"], true);

    let call_tool = nodes
        .iter()
        .find(|n| n["type_id"] == "mcp:call_tool")
        .expect("mcp:call_tool described");
    assert_eq!(
        call_tool["config_schema"]["properties"]["connection_id"]["x-node-selector"]["node_type"],
        "mcp:connect"
    );

    process.shutdown();
}

/// `mcp:call_tool` without a preceding connect fails at the pool lookup —
/// no server needed, proves the pool gate runs before any socket I/O.
#[test]
fn call_tool_without_connect_fails_offline() {
    let mut process = PluginProcess::spawn();
    let resp = process.request(
        "execute",
        serde_json::json!({
            "config": {
                "type": "mcp:call_tool",
                "connection_id": "never-connected",
                "tool_name": "echo",
                "arguments": { "message": "x" },
            }
        }),
    );
    assert_eq!(resp["result"]["success"], false);
    let serialized = serde_json::to_string(&resp["result"]).unwrap_or_default();
    assert!(
        serialized.contains("MCP connection not found"),
        "pool gate must produce a readable error: {serialized}"
    );
    process.shutdown();
}

/// Unknown node type fails at the dispatch match with a readable error.
#[test]
fn unknown_node_type_fails() {
    let mut process = PluginProcess::spawn();
    let resp = process.request(
        "execute",
        serde_json::json!({ "config": { "type": "mcp:totally_unknown" } }),
    );
    assert_eq!(resp["result"]["success"], false);
    let serialized = serde_json::to_string(&resp["result"]).unwrap_or_default();
    assert!(
        serialized.contains("Unknown node type"),
        "dispatch must name the unknown type: {serialized}"
    );
    process.shutdown();
}

/// `mcp:close` on an empty pool is an idempotent success — release is a
/// no-op on a missing entry (offline-safe).
#[test]
fn close_without_connect_is_idempotent() {
    let mut process = PluginProcess::spawn();
    let resp = process.request(
        "execute",
        serde_json::json!({ "config": { "type": "mcp:close" } }),
    );
    assert_eq!(resp["result"]["success"], true);
    assert_eq!(
        resp["result"]["output_data"]["connection_id"],
        serde_json::Value::Null,
        "close-all on an empty pool reports null connection_id"
    );
    process.shutdown();
}

/// validate: connect without a target (empty stdio command) is rejected;
/// a complete connect config passes.
#[test]
fn validate_connect_rules() {
    let mut process = PluginProcess::spawn();
    let bad = process.request(
        "validate",
        serde_json::json!({
            "config": { "type": "mcp:connect", "transport": "stdio", "command": "" }
        }),
    );
    assert_eq!(bad["result"]["valid"], false);
    assert!(
        bad["result"]["message"].as_str().is_some_and(|m| m.contains("command")),
        "validation message must mention command: {bad}"
    );

    let good = process.request(
        "validate",
        serde_json::json!({
            "config": { "type": "mcp:connect", "transport": "stdio", "command": "npx" }
        }),
    );
    assert_eq!(good["result"]["valid"], true);

    process.shutdown();
}

/// Clean shutdown: EOF on stdin exits with code 0.
#[test]
fn clean_shutdown_on_eof() {
    let process = PluginProcess::spawn();
    process.shutdown();
}
