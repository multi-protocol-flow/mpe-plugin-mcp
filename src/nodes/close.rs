//! `mcp:close` node — releases the execution's pooled MCP connection(s).
//!
//! Config: `{}` — or an optional `connection_id` (the host node selector
//! over `mcp:connect` nodes) when the flow wants to tear down ONE of
//! several connections. With a selection, only that connection is released;
//! without one, every connection of the execution is released. Output:
//! `{ "connection_id" }` (the released id, or `null` for release-all).
//!
//! Idempotent: releasing an execution that never connected (or was already
//! released) succeeds silently — [`McpPool::release`] /
//! [`McpPool::release_connection`] are no-ops on missing keys. This is
//! release path 2 of the double-release design (the `flow_ended` hook is
//! path 1); a flow may run both, in either order.

use mpe_plugin_sdk::prelude::*;

use crate::pool::McpPool;

/// Thin `ExecuteContext` adapter (see [`close_core`]).
pub async fn execute(ctx: &mut ExecuteContext, pool: &McpPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match close_core(pool, &exec_id, &config) {
        Ok(result) => result,
        Err(e) => {
            ctx.log("error", format!("MCP close failed: {e}"));
            ExecuteResult::fail(format!("MCP close failed: {e}"))
        }
    }
}

/// Core close logic, testable without an [`ExecuteContext`].
///
/// Releases the execution's pooled connection(s). Idempotent: succeeds
/// whether or not the execution had a connection. With a config
/// `connection_id`, only that connection is released (multi-connect flows);
/// without one, every connection of the execution is released.
fn close_core<C: std::any::Any + Send + Sync>(
    pool: &McpPool<C>,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let released_id = match crate::nodes::resolve_connection_id(config) {
        Ok(id) => {
            pool.release_connection(exec_id, &id);
            serde_json::Value::String(id)
        }
        // 未选中（或空）→ 释放当前执行的全部连接。
        Err(_) => {
            pool.release(exec_id);
            serde_json::Value::Null
        }
    };
    let output = serde_json::json!({ "connection_id": released_id });
    Ok(crate::close_ok_result(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cheap, server-free payload for keying tests (no MCP server needed;
    /// mirrors the pool's own unit tests).
    #[derive(Default)]
    struct Dummy;

    fn seed(pool: &McpPool<Dummy>, exec_id: &str, id: &str) {
        pool.connect(exec_id, id, Dummy, |_, _| true);
    }

    /// A connected execution is released: all pool entries are gone after
    /// close without a selection.
    #[tokio::test]
    async fn close_releases_pool_entry() {
        let pool = McpPool::<Dummy>::default();
        seed(&pool, "exec-1", "conn-1");
        let result = close_core(&pool, "exec-1", &serde_json::json!({})).unwrap();
        assert!(result.success);
        assert!(pool.connection("exec-1", "conn-1").is_none());
    }

    /// With a `connection_id` selection, close releases ONLY that
    /// connection, leaving the others of the same execution alive.
    #[tokio::test]
    async fn close_releases_only_selected_connection() {
        let pool = McpPool::<Dummy>::default();
        seed(&pool, "exec-1", "conn-1");
        seed(&pool, "exec-1", "conn-2");
        let result = close_core(&pool, "exec-1", &serde_json::json!({ "connection_id": "conn-1" }))
            .unwrap();
        assert!(result.success);
        assert_eq!(
            result.output_data.as_ref().unwrap()["connection_id"],
            "conn-1"
        );
        assert!(pool.connection("exec-1", "conn-1").is_none());
        assert!(pool.connection("exec-1", "conn-2").is_some());
    }

    /// Close on an execution that never connected (or was already released)
    /// is a silent success — release is a no-op on a missing key.
    #[tokio::test]
    async fn close_without_connect_is_idempotent() {
        let pool = McpPool::<Dummy>::default();
        let result = close_core(&pool, "exec-1", &serde_json::json!({})).unwrap();
        assert!(result.success);
        assert_eq!(
            result.output_data.as_ref().unwrap()["connection_id"],
            serde_json::Value::Null
        );
    }
}
