//! `mcp:connect` node — establishes an MCP connection (streamable_http or
//! stdio) with discovery snapshot and idempotent pool reuse.
//!
//! Config: `{ "transport", "url", "headers", "protocol_version", "command",
//! "args", "env" }` — the host resolved `{{variables}}` before sending.
//! Output: `{ "connection_id", "server_info", "transport", "url",
//! "protocol_version", "tools", "resources", "prompts", "timing" }`.
//!
//! Idempotency: a second connect for the same `execution_id` with the SAME
//! target (transport + display url) reuses the pooled connection — no new
//! MCP session. A DIFFERENT target replaces the stale entry (surfaced via
//! `ctx.log("warn", …)`).

use mpe_plugin_sdk::prelude::*;

use crate::client;
use crate::i18n;
use crate::pool::McpPool;

/// Thin `ExecuteContext` adapter: extracts `execution_id` (fallback
/// `"default"` for SDK tests that send none) and the node instance id as the
/// connection key, then delegates to [`connect_core`]. Logs the failure so
/// the host surfaces it in the report.
pub async fn execute(ctx: &mut ExecuteContext, pool: &McpPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let connection_id = ctx
        .node_instance_id()
        .unwrap_or("default")
        .to_string();
    let config = ctx.config().clone();
    match connect_core(pool, &exec_id, &connection_id, &config).await {
        Ok((result, replaced)) => {
            if replaced {
                ctx.log(
                    "warn",
                    i18n::t(
                        "MCP 连接目标已变更，旧连接已替换",
                        "MCP connection target changed; stale connection replaced",
                    ),
                );
            }
            result
        }
        Err(e) => {
            ctx.log("error", format!("MCP connect failed: {e}"));
            ExecuteResult::fail(format!("MCP connect failed: {e}"))
        }
    }
}

/// Core connect logic, testable without an [`ExecuteContext`] (the SDK's
/// `ExecuteContext::from_params` is `pub(crate)`, so plugin tests cannot
/// construct one).
///
/// Returns `(result, replaced)`; `replaced` is `true` only when an existing
/// connection with a DIFFERENT target was swapped out.
async fn connect_core(
    pool: &McpPool,
    exec_id: &str,
    connection_id: &str,
    config: &serde_json::Value,
) -> Result<(ExecuteResult, bool), String> {
    let conn = client::connect(config).await?;
    let transport = conn.transport.clone();
    let url = conn.url.clone();
    let protocol_version = config
        .get("protocol_version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("2025-11-25")
        .to_string();
    let server_info = conn.server_info.clone();
    let tools = conn.tools.clone();
    let resources = conn.resources.clone();
    let prompts = conn.prompts.clone();

    let (_, replaced) = pool.connect(exec_id, connection_id, conn, client::same_target);

    let output = serde_json::json!({
        "connection_id": connection_id,
        "server_info": server_info,
        "transport": transport,
        "url": url,
        "protocol_version": protocol_version,
        "tools": tools,
        "resources": resources,
        "prompts": prompts,
        "timing": { "connect_ms": 0 },
    });

    Ok((crate::ok_result(output), replaced))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// connect_core with a missing transport fails with a readable error
    /// before any network I/O.
    #[tokio::test]
    async fn connect_missing_transport_fails() {
        let pool = McpPool::default();
        let result = connect_core(&pool, "exec-1", "conn-1", &serde_json::json!({})).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("transport"));
    }

    /// Unsupported transport value fails with a readable error.
    #[tokio::test]
    async fn connect_unsupported_transport_fails() {
        let pool = McpPool::default();
        let result = connect_core(
            &pool,
            "exec-1",
            "conn-1",
            &serde_json::json!({ "transport": "websocket" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unsupported MCP transport"));
    }
}
