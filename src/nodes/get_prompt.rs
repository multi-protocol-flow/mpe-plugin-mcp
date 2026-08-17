//! `mcp:get_prompt` node — fetches a prompt from the selected MCP
//! connection.
//!
//! Config: `{ "connection_id", "prompt_name", "arguments" }` (arguments:
//! object of string values, per the MCP prompt spec). Output:
//! `{ "prompt_name", "messages", "description", "request", "timing" }`.

use std::time::Instant;

use mpe_plugin_sdk::prelude::*;
use rmcp::model::GetPromptRequestParams;

use crate::i18n;
use crate::pool::McpPool;

/// Thin `ExecuteContext` adapter (see [`get_prompt_core`]).
pub async fn execute(ctx: &mut ExecuteContext, pool: &McpPool) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match get_prompt_core(pool, &exec_id, &config).await {
        Ok(result) => result,
        Err(e) => {
            ctx.log("error", format!("MCP get prompt failed: {e}"));
            ExecuteResult::fail(format!("MCP get prompt failed: {e}"))
        }
    }
}

/// Core get-prompt logic, testable without an [`ExecuteContext`].
async fn get_prompt_core(
    pool: &McpPool,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let start = Instant::now();
    let connection_id = crate::nodes::resolve_connection_id(config)?;
    let prompt_name = config
        .get("prompt_name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            i18n::t("缺少 prompt_name（请选择或输入提示词名）", "Missing prompt_name (select or type a prompt name)").to_string()
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

    // MCP 提示词参数是 string → string 映射；object 中的非字符串值被忽略。
    let args_map: serde_json::Map<String, serde_json::Value> = config
        .get("arguments")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut prompt_params = GetPromptRequestParams::new(prompt_name.clone());
    if !args_map.is_empty() {
        prompt_params = prompt_params.with_arguments(args_map.clone());
    }

    let result = peer
        .get_prompt(prompt_params)
        .await
        .map_err(|e| format!("{} '{prompt_name}': {e}", i18n::t("提示词获取失败", "prompt fetch failed")))?;

    let timing_ms = start.elapsed().as_millis() as u64;
    let messages: Vec<serde_json::Value> = result
        .messages
        .iter()
        .map(|m| serde_json::to_value(m).unwrap_or(serde_json::Value::Null))
        .collect();

    let output = serde_json::json!({
        "prompt_name": prompt_name,
        "messages": messages,
        "description": result.description,
        "request": {
            "connection_id": connection_id,
            "prompt_name": prompt_name,
            "arguments": args_map,
        },
        "timing": { "get_ms": timing_ms, "total_ms": timing_ms },
    });
    Ok(crate::ok_result(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Missing connection_id fails at the pool gate.
    #[tokio::test]
    async fn get_prompt_without_connection_id_fails() {
        let pool = McpPool::default();
        let result = get_prompt_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "prompt_name": "greet" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("connection_id"));
    }

    /// Missing prompt_name fails before any connection lookup.
    #[tokio::test]
    async fn get_prompt_without_name_fails() {
        let pool = McpPool::default();
        let result = get_prompt_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "connection_id": "conn-1" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("prompt_name"));
    }

    /// Unknown connection id fails at the pool lookup (offline-safe).
    #[tokio::test]
    async fn get_prompt_unknown_connection_fails() {
        let pool = McpPool::default();
        let result = get_prompt_core(
            &pool,
            "exec-1",
            &serde_json::json!({ "connection_id": "nope", "prompt_name": "greet" }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("MCP connection not found"));
    }
}
