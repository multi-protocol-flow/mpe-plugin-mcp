//! `mcp:read_resource` node — reads a resource from the selected MCP
//! connection.
//!
//! Config: `{ "connection_id", "resource_uri" }`. Output:
//! `{ "resource_uri", "contents", "request", "timing" }`.

use std::time::Instant;

use mpe_plugin_sdk::prelude::*;
use rmcp::model::ReadResourceRequestParams;

use crate::i18n;
use crate::pool::McpPool;

/// Thin `ExecuteContext` adapter (see [`read_resource_core`]).
pub async fn execute(ctx: &mut ExecuteContext, pool: &McpPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match read_resource_core(pool, &exec_id, &config).await {
        Ok(result) => result,
        Err(e) => {
            ctx.log("error", format!("MCP resource read failed: {e}"));
            ExecuteResult::fail(format!("MCP resource read failed: {e}"))
        }
    }
}

/// Core read-resource logic, testable without an [`ExecuteContext`].
async fn read_resource_core(
    pool: &McpPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let start = Instant::now();
    let connection_id = crate::nodes::resolve_connection_id(config)?;
    let resource_uri = config
        .get("resource_uri")
        .and_then(serde_json::Value::as_str)
        .filter(|uri| !uri.is_empty())
        .ok_or_else(|| {
            i18n::t("缺少 resource_uri（请输入资源 URI）", "Missing resource_uri (type a resource URI)").to_string()
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

    let read_params = ReadResourceRequestParams::new(resource_uri.clone());
    let timeout_ms = config
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(30000);
    let timeout_dur = std::time::Duration::from_millis(timeout_ms.max(100));

    let result = tokio::time::timeout(timeout_dur, peer.read_resource(read_params))
        .await
        .map_err(|_| format!("{}: {}ms", i18n::t("资源读取超时", "resource read timed out"), timeout_ms))?
        .map_err(|e| format!("{} '{resource_uri}': {e}", i18n::t("资源读取失败", "resource read failed")))?;

    let timing_ms = start.elapsed().as_millis() as u64;
    let contents: Vec<serde_json::Value> = result
        .contents
        .iter()
        .map(|c| serde_json::to_value(c).unwrap_or(serde_json::Value::Null))
        .collect();

    let output = serde_json::json!({
        "resource_uri": resource_uri,
        "contents": contents,
        "request": { "connection_id": connection_id, "resource_uri": resource_uri },
        "timing": { "read_ms": timing_ms, "total_ms": timing_ms },
    });
    Ok(crate::ok_result(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Missing connection_id fails at the pool gate.
    #[tokio::test]
    async fn read_resource_without_connection_id_fails() {
        let pool = McpPool::default();
        let result = read_resource_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "resource_uri": "config://x" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("connection_id"));
    }

    /// Missing resource_uri fails before any connection lookup.
    #[tokio::test]
    async fn read_resource_without_uri_fails() {
        let pool = McpPool::default();
        let result = read_resource_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "connection_id": "conn-1" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("resource_uri"));
    }

    /// Unknown connection id fails at the pool lookup (offline-safe).
    #[tokio::test]
    async fn read_resource_unknown_connection_fails() {
        let pool = McpPool::default();
        let result = read_resource_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "connection_id": "nope", "resource_uri": "config://x" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("MCP connection not found"));
    }
}
