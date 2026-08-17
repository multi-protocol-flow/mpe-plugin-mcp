//! `mcp:call_tool` node — invokes a tool on the selected MCP connection.
//!
//! Config: `{ "connection_id", "tool_name", "arguments" }`. Output:
//! `{ "tool_name", "content", "is_error", "request", "timing" }` — a
//! tool-reported error (`is_error: true`, per MCP) is server DATA, not an
//! execution failure: the call completed and routes to `true`.

use std::time::Instant;

use mpe_plugin_sdk::prelude::*;
use rmcp::model::CallToolRequestParams;

use crate::i18n;
use crate::pool::McpPool;

/// Thin `ExecuteContext` adapter: extracts `execution_id` + config, then
/// delegates to [`call_tool_core`]. Logs failures so the host surfaces them.
pub async fn execute(ctx: &mut ExecuteContext, pool: &McpPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match call_tool_core(pool, &exec_id, &config).await {
        Ok(result) => result,
        Err(e) => {
            ctx.log("error", format!("MCP tool call failed: {e}"));
            ExecuteResult::fail(format!("MCP tool call failed: {e}"))
        }
    }
}

/// Core call-tool logic, testable without an [`ExecuteContext`].
async fn call_tool_core(
    pool: &McpPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let start = Instant::now();
    let connection_id = crate::nodes::resolve_connection_id(config)?;
    let tool_name = config
        .get("tool_name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            i18n::t("缺少 tool_name（请选择或输入工具名）", "Missing tool_name (select or type a tool name)").to_string()
        })?
        .to_string();

    let conn = pool.connection(exec_id, &connection_id).ok_or_else(|| {
        format!(
            "{}: `{}`",
            i18n::t(
                "MCP 连接不存在（请先添加并选择 mcp:connect 节点，或该连接已被关闭）",
                "MCP connection not found (add and select an mcp:connect node first, or the connection was closed)"
            ),
            connection_id
        )
    })?;
    let peer = conn.service.peer().clone();

    // MCP 协议要求 arguments 是 object；缺省传空对象。
    let args_map = config
        .get("arguments")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut call_params = CallToolRequestParams::new(tool_name.clone());
    if !args_map.is_empty() {
        call_params = call_params.with_arguments(args_map.clone());
    }

    let result = peer
        .call_tool(call_params)
        .await
        .map_err(|e| format!("{} '{tool_name}': {e}", i18n::t("工具调用失败", "tool call failed")))?;

    let timing_ms = start.elapsed().as_millis() as u64;
    let content: Vec<serde_json::Value> = result
        .content
        .iter()
        .map(|c| serde_json::to_value(c).unwrap_or(serde_json::Value::Null))
        .collect();
    let is_error = result.is_error.unwrap_or(false);

    let output = serde_json::json!({
        "tool_name": tool_name,
        "content": content,
        "is_error": is_error,
        "request": {
            "connection_id": connection_id,
            "tool_name": tool_name,
            "arguments": args_map,
        },
        "timing": { "call_ms": timing_ms, "total_ms": timing_ms },
    });
    Ok(crate::ok_result(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Missing connection_id fails at the pool gate with a readable error.
    #[tokio::test]
    async fn call_tool_without_connection_id_fails() {
        let pool = McpPool::default();
        let result = call_tool_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "tool_name": "echo", "arguments": {} }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("connection_id"));
    }

    /// Missing tool_name fails before any connection lookup.
    #[tokio::test]
    async fn call_tool_without_tool_name_fails() {
        let pool = McpPool::default();
        let result = call_tool_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "connection_id": "conn-1" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("tool_name"));
    }

    /// Unknown connection id fails at the pool lookup (offline-safe).
    #[tokio::test]
    async fn call_tool_unknown_connection_fails() {
        let pool = McpPool::default();
        let result = call_tool_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "connection_id": "nope", "tool_name": "echo" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("MCP connection not found"));
    }
}
