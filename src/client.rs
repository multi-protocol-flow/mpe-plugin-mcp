//! MCP client wrapper: rmcp-based connect + discovery.
//!
//! Ported from the former host crate `flow-engine-mcp/src/mcp_client.rs`,
//! freed of host types: config is read as plain JSON (the host already
//! resolved `{{variables}}` before sending), and all rmcp types stay inside
//! this module — the plugin surface (pool values, node outputs, `uiCall`
//! answers) is plain serde data.
//!
//! Two transports:
//! - `streamable_http` — reqwest client (custom headers via
//!   `default_headers`), URL from config;
//! - `stdio` — child process (`TokioChildProcess`), command/args/env from
//!   config.
//!
//! [`connect`] establishes one connection and lists the server's
//! tools/resources/prompts (resource/prompt listing degrades gracefully to
//! empty lists when the server does not support them). [`discover`] is the
//! `uiCall("mcp.discover")` probe: connect → list → serialize → drop.

use std::collections::HashMap;
use std::time::Instant;

use rmcp::service::{RoleClient, RunningService, ServiceExt};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::transport::TokioChildProcess;
use serde::Serialize;

/// Streamable HTTP transport marker (`config["transport"]` value).
pub const TRANSPORT_STREAMABLE_HTTP: &str = "streamable_http";
/// Stdio transport marker (`config["transport"]` value).
pub const TRANSPORT_STDIO: &str = "stdio";

/// A discovered MCP tool (wire/UI shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ToolInfo {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
}

/// A discovered MCP resource (wire/UI shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ResourceInfo {
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
}

/// A discovered MCP prompt argument (wire/UI shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PromptArgumentInfo {
    pub name: String,
    pub description: Option<String>,
    pub required: Option<bool>,
}

/// A discovered MCP prompt (wire/UI shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PromptInfo {
    pub name: String,
    pub description: Option<String>,
    pub arguments: Vec<PromptArgumentInfo>,
}

/// Server identification (wire/UI shape).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ServerInfo {
    pub name: String,
    pub version: Option<String>,
}

/// A live MCP client bound to one flow execution: the running service plus
/// the discovery snapshot taken at connect time.
pub struct McpConnection {
    /// rmcp running service (keeps the connection alive; drop closes it).
    pub service: RunningService<RoleClient, ()>,
    /// Server identification.
    pub server_info: ServerInfo,
    /// Tools discovered at connect time.
    pub tools: Vec<ToolInfo>,
    /// Resources discovered at connect time.
    pub resources: Vec<ResourceInfo>,
    /// Prompts discovered at connect time.
    pub prompts: Vec<PromptInfo>,
    /// Transport label (`streamable_http` | `stdio`) — idempotency key.
    pub transport: String,
    /// Display target (URL for http, `command args…` for stdio) — idempotency key.
    pub url: String,
}

/// Whether two connections target the SAME server (pool idempotency): same
/// transport AND same display target. Used by the connect node's
/// `key_match`; headers/env changes do not trigger a reconnect.
pub fn same_target(a: &McpConnection, b: &McpConnection) -> bool {
    a.transport == b.transport && a.url == b.url
}

/// Parses the transport label from config; missing/invalid → error with the
/// supported values.
fn transport_from_config(config: &serde_json::Value) -> Result<String, String> {
    match config.get("transport").and_then(serde_json::Value::as_str) {
        Some(TRANSPORT_STREAMABLE_HTTP) => Ok(TRANSPORT_STREAMABLE_HTTP.to_string()),
        Some(TRANSPORT_STDIO) => Ok(TRANSPORT_STDIO.to_string()),
        Some(other) => Err(format!(
            "unsupported MCP transport `{other}` (expected `streamable_http` or `stdio`)"
        )),
        None => Err("MCP connect config is missing `transport`".to_string()),
    }
}

/// Establishes an MCP connection from a plain JSON config and snapshots the
/// server's tools/resources/prompts.
///
/// Config shape (matching the connect node's `default_config`):
/// `{ "transport", "url", "headers", "protocol_version", "command", "args",
///   "env" }` — the host already resolved `{{variables}}`.
pub async fn connect(config: &serde_json::Value) -> Result<McpConnection, String> {
    let start = Instant::now();
    let transport = transport_from_config(config)?;

    let service = match transport.as_str() {
        TRANSPORT_STREAMABLE_HTTP => {
            let url = config
                .get("url")
                .and_then(serde_json::Value::as_str)
                .filter(|url| !url.is_empty())
                .ok_or_else(|| {
                    "streamable_http transport requires a non-empty `url`".to_string()
                })?;
            let headers = config
                .get("headers")
                .and_then(serde_json::Value::as_object)
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| {
                            v.as_str()
                                .map(|s| (k.clone(), s.to_string()))
                                .or_else(|| {
                                    // tolerate non-string header values (coerce to string)
                                    v.as_i64()
                                        .map(|n| (k.clone(), n.to_string()))
                                        .or_else(|| v.as_bool().map(|b| (k.clone(), b.to_string())))
                                })
                        })
                        .collect::<HashMap<String, String>>()
                })
                .unwrap_or_default();
            create_streamable_http_transport(url, headers).await?
        }
        TRANSPORT_STDIO => {
            let command_raw = config
                .get("command")
                .and_then(serde_json::Value::as_str)
                .filter(|cmd| !cmd.trim().is_empty())
                .ok_or_else(|| "stdio transport requires a non-empty `command`".to_string())?;
            let args_raw = config
                .get("args")
                .and_then(serde_json::Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();
            let (command, args) = parse_command_and_args(command_raw, &args_raw);
            let env = config
                .get("env")
                .and_then(serde_json::Value::as_object)
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect::<HashMap<String, String>>()
                })
                .unwrap_or_default();
            create_stdio_transport(&command, args, env).await?
        }
        other => return Err(format!("unsupported MCP transport `{other}`")),
    };

    let connect_ms = start.elapsed().as_millis() as u64;
    let url = display_target(config, &transport);
    let server_name = service
        .peer_info()
        .map(|info| info.server_info.name.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let server_version = service.peer_info().map(|info| info.server_info.version.clone());
    let server_info = ServerInfo {
        name: server_name,
        version: server_version,
    };

    let tools: Vec<ToolInfo> = match service.list_all_tools().await {
        Ok(result) => result
            .iter()
            .map(|tool| ToolInfo {
                name: tool.name.to_string(),
                description: tool.description.as_ref().map(|d| d.to_string()),
                input_schema: serde_json::Value::Object((*tool.input_schema).clone()),
            })
            .collect(),
        Err(e) => return Err(format!("failed to list MCP tools: {e}")),
    };

    // 优雅降级：服务器不支持资源/提示词时返回空列表。
    let resources: Vec<ResourceInfo> = match service.list_all_resources().await {
        Ok(result) => result
            .iter()
            .map(|res| ResourceInfo {
                uri: res.uri.to_string(),
                name: res.name.to_string(),
                description: res.description.clone(),
                mime_type: res.mime_type.clone(),
            })
            .collect(),
        Err(e) => {
            eprintln!("[mcp-plugin] list_all_resources failed (degraded to empty): {e}");
            Vec::new()
        }
    };

    let prompts: Vec<PromptInfo> = match service.list_all_prompts().await {
        Ok(result) => result
            .iter()
            .map(|prompt| PromptInfo {
                name: prompt.name.clone(),
                description: prompt.description.clone(),
                arguments: match &prompt.arguments {
                    Some(args) => args
                        .iter()
                        .map(|arg| PromptArgumentInfo {
                            name: arg.name.clone(),
                            description: arg.description.clone(),
                            required: arg.required,
                        })
                        .collect(),
                    None => Vec::new(),
                },
            })
            .collect(),
        Err(e) => {
            eprintln!("[mcp-plugin] list_all_prompts failed (degraded to empty): {e}");
            Vec::new()
        }
    };

    eprintln!(
        "[mcp-plugin] connected to {} ({}ms): {} tools, {} resources, {} prompts",
        url,
        connect_ms,
        tools.len(),
        resources.len(),
        prompts.len()
    );

    Ok(McpConnection {
        service,
        server_info,
        tools,
        resources,
        prompts,
        transport,
        url,
    })
}

/// `uiCall("mcp.discover")` probe: connect with the panel-provided params,
/// serialize the discovery snapshot, drop the connection (probe only — the
/// result is never pooled). Unknown sub-methods are rejected by the caller.
pub async fn discover(params: &serde_json::Value) -> Result<serde_json::Value, String> {
    let conn = connect(params).await?;
    let snapshot = serde_json::json!({
        "transport": conn.transport,
        "url": conn.url,
        "server_info": conn.server_info,
        "tools": conn.tools,
        "resources": conn.resources,
        "prompts": conn.prompts,
    });
    // Dropping the running service closes the connection.
    drop(conn.service);
    Ok(snapshot)
}

/// Creates a Streamable HTTP transport (with custom headers when present).
async fn create_streamable_http_transport(
    url: &str,
    headers: HashMap<String, String>,
) -> Result<RunningService<RoleClient, ()>, String> {
    if headers.is_empty() {
        let transport = StreamableHttpClientTransport::from_uri(url.to_string());
        ().serve(transport)
            .await
            .map_err(|e| format!("MCP connection failed: {e}"))
    } else {
        let mut header_map = reqwest::header::HeaderMap::new();
        for (key, value) in &headers {
            let header_name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| format!("invalid request header name '{key}': {e}"))?;
            let header_value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|e| format!("invalid request header value '{value}': {e}"))?;
            header_map.insert(header_name, header_value);
        }
        let client = reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;
        let config = StreamableHttpClientTransportConfig::with_uri(url.to_string());
        let transport = StreamableHttpClientTransport::with_client(client, config);
        ().serve(transport)
            .await
            .map_err(|e| format!("MCP connection failed: {e}"))
    }
}

/// Normalizes stdio command and args with shell-style argument splitting.
///
/// If `command` contains spaces (e.g. `"uvx --python 3.11 mcp-for-blender"`),
/// the first token becomes the executable binary and subsequent tokens are
/// prepended to `args`.
///
/// Any entry in `args` that contains whitespace (e.g. `"--python 3.11"`) is
/// split into separate arguments according to standard shell quoting rules,
/// allowing users to either configure args per-row or paste space-separated flags.
pub fn parse_command_and_args(command_raw: &str, args_raw: &[String]) -> (String, Vec<String>) {
    let trimmed_cmd = command_raw.trim();
    let (cmd, leading_args) = if trimmed_cmd.contains(' ') {
        if let Some(tokens) = shlex::split(trimmed_cmd) {
            if let Some((first, rest)) = tokens.split_first() {
                (first.clone(), rest.to_vec())
            } else {
                (trimmed_cmd.to_string(), Vec::new())
            }
        } else {
            (trimmed_cmd.to_string(), Vec::new())
        }
    } else {
        (trimmed_cmd.to_string(), Vec::new())
    };

    let mut final_args = leading_args;
    for arg in args_raw {
        let trimmed_arg = arg.trim();
        if trimmed_arg.is_empty() {
            continue;
        }
        // Only split args that start with a flag prefix ('-') and contain whitespace
        // (e.g. `"--python 3.11"` or `"--python 3.11 mcp-for-blender"`).
        // Non-flag arguments (such as script strings or file paths with spaces)
        // must stay intact.
        if trimmed_arg.starts_with('-') && trimmed_arg.contains(' ') {
            if let Some(tokens) = shlex::split(trimmed_arg) {
                final_args.extend(tokens);
            } else {
                final_args.push(trimmed_arg.to_string());
            }
        } else {
            final_args.push(trimmed_arg.to_string());
        }
    }

    (cmd, final_args)
}

/// Creates a Stdio child-process transport with stderr capture for actionable errors.
async fn create_stdio_transport(
    command: &str,
    args: Vec<String>,
    env: HashMap<String, String>,
) -> Result<RunningService<RoleClient, ()>, String> {
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncReadExt;

    let mut cmd = tokio::process::Command::new(command);
    cmd.args(&args);
    for (k, v) in &env {
        cmd.env(k, v);
    }

    let (transport, stderr_opt) = TokioChildProcess::builder(cmd)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to start MCP process '{command}': {e}"))?;

    let stderr_buf = Arc::new(Mutex::new(String::new()));
    let stderr_clone = Arc::clone(&stderr_buf);
    if let Some(mut stderr) = stderr_opt {
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok(n) = stderr.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]);
                let mut lock = stderr_clone.lock().unwrap_or_else(|e| e.into_inner());
                if lock.len() < 8192 {
                    lock.push_str(&chunk);
                }
            }
        });
    }

    match ().serve(transport).await {
        Ok(service) => Ok(service),
        Err(e) => {
            // Small yield so the stderr reader can capture any EOF diagnostic
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let stderr_text = {
                let lock = stderr_buf.lock().unwrap_or_else(|e| e.into_inner());
                lock.trim().to_string()
            };
            if stderr_text.is_empty() {
                Err(format!("MCP stdio connection failed: {e}"))
            } else {
                Err(format!("MCP stdio connection failed: {e} (stderr: {stderr_text})"))
            }
        }
    }
}

/// Display target for logs and pool idempotency: URL for http, `command
/// args…` for stdio.
fn display_target(config: &serde_json::Value, transport: &str) -> String {
    match transport {
        TRANSPORT_STDIO => {
            let command = config
                .get("command")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let args = config
                .get("args")
                .and_then(serde_json::Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(serde_json::Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            if args.is_empty() {
                command.to_string()
            } else {
                format!("{command} {args}")
            }
        }
        _ => config
            .get("url")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_parsing() {
        assert_eq!(
            transport_from_config(&serde_json::json!({ "transport": "stdio" })).unwrap(),
            "stdio"
        );
        assert_eq!(
            transport_from_config(&serde_json::json!({ "transport": "streamable_http" }))
                .unwrap(),
            "streamable_http"
        );
        assert!(transport_from_config(&serde_json::json!({ "transport": "websocket" })).is_err());
        assert!(transport_from_config(&serde_json::json!({})).is_err());
    }

    #[test]
    fn display_target_shapes() {
        let http = serde_json::json!({
            "transport": "streamable_http",
            "url": "https://example.com/mcp",
        });
        assert_eq!(display_target(&http, TRANSPORT_STREAMABLE_HTTP), "https://example.com/mcp");

        let stdio = serde_json::json!({
            "transport": "stdio",
            "command": "npx",
            "args": ["-y", "server"],
        });
        assert_eq!(display_target(&stdio, TRANSPORT_STDIO), "npx -y server");

        let bare = serde_json::json!({ "transport": "stdio", "command": "mcp-server" });
        assert_eq!(display_target(&bare, TRANSPORT_STDIO), "mcp-server");
    }

    #[test]
    fn test_parse_command_and_args() {
        // Case 1: user entered whole command in `command` input
        let (cmd, args) = parse_command_and_args("uvx --python 3.11 mcp-for-blender", &[]);
        assert_eq!(cmd, "uvx");
        assert_eq!(args, vec!["--python", "3.11", "mcp-for-blender"]);

        // Case 2: user entered `--python 3.11` as one arg row
        let (cmd, args) = parse_command_and_args(
            "uvx",
            &["--python 3.11".to_string(), "mcp-for-blender".to_string()],
        );
        assert_eq!(cmd, "uvx");
        assert_eq!(args, vec!["--python", "3.11", "mcp-for-blender"]);

        // Case 3: user entered whole tail in one arg row
        let (cmd, args) =
            parse_command_and_args("uvx", &["--python 3.11 mcp-for-blender".to_string()]);
        assert_eq!(cmd, "uvx");
        assert_eq!(args, vec!["--python", "3.11", "mcp-for-blender"]);

        // Case 4: quoted arguments stay intact
        let (cmd, args) = parse_command_and_args(
            "python3 -u",
            &["--msg \"hello world\"".to_string(), "script.py".to_string()],
        );
        assert_eq!(cmd, "python3");
        assert_eq!(args, vec!["-u", "--msg", "hello world", "script.py"]);

        // Case 5: normal separate args pass through unchanged
        let (cmd, args) = parse_command_and_args(
            "npx",
            &["-y".to_string(), "@modelcontextprotocol/server-everything".to_string()],
        );
        assert_eq!(cmd, "npx");
        assert_eq!(args, vec!["-y", "@modelcontextprotocol/server-everything"]);
    }

    #[tokio::test]
    async fn test_stdio_stderr_capture_on_failure() {
        let cfg = serde_json::json!({
            "transport": "stdio",
            "command": "python3",
            "args": ["-c", "import sys; sys.stderr.write('custom failure reason: connection refused\\n'); sys.exit(1)"],
            "env": {}
        });
        let err = connect(&cfg).await.err().expect("connect must fail");
        assert!(
            err.contains("custom failure reason: connection refused"),
            "error should contain captured child stderr, got: {err}"
        );
    }
}
