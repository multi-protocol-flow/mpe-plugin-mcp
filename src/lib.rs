//! MCP sidecar plugin for MPE.
//!
//! Third-party plugin crate: depends only on `mpe-plugin-sdk` (which in turn
//! bundles the shared wire contract (`mpe-plugin-sdk::protocol`)). Never imports host
//! types (`flow-engine-core`, `flow-engine-plugin`, ...) — the plugin is a
//! separate process speaking JSON-RPC 2.0 over stdio.
//!
//! Node types: `mcp:connect` / `mcp:call_tool` / `mcp:read_resource` /
//! `mcp:get_prompt` / `mcp:close`. `execute` dispatches to `nodes/*.rs` by
//! the `config["type"]` discriminator. The four operation nodes
//! (connect/call_tool/read_resource/get_prompt) declare `in`/`true`/`false`
//! tri-ports (success routes `true`, failure goes through the host
//! `on_error` strategy to `false`); `mcp:close` keeps the single `in`/`out`
//! shape.
//!
//! Connection selection: the pool keys connections by the composite
//! `(execution_id, connection_id)` — the connect node registers under its
//! own instance id (`ctx.node_instance_id()`, which equals the canvas node
//! uuid), operation nodes pick their connection via the `connection_id`
//! field the host resolves into their config from the schema's
//! `x-node-selector`.
//!
//! `mcp:connect` additionally declares `capabilities.single_node: true`:
//! the host's single-node execution path (`execute_single_node` /
//! `mpe run node`) consults that capability instead of a hardcoded
//! whitelist, so the plugin is testable for reachability without a host
//! code change.
//!
//! Config panel: all five nodes carry an inline frontend (the panel HTML is
//! embedded in the binary via `include_str!`). The panel drives runtime
//! discovery (`uiCall("mcp.discover")`) through the host's `plugin_ui_call`
//! relay — see `docs/plugin-protocol.md` §12.

use std::future::Future;

use mpe_plugin_sdk::prelude::*;

mod client;
mod i18n;
mod nodes;
mod pool;
use pool::McpPool;

/// Process-level plugin singleton: node registry + per-execution pool.
#[derive(Default)]
pub struct McpPlugin {
    pub pool: McpPool,
}

/// Builds one MCP node description with host-facing metadata.
///
/// `default_config` is injected into a freshly added node; `properties` is the
/// JSON-Schema `properties` map (each value declares its own `type`) and
/// `required` the schema's required field list. Every node carries the
/// inline config-panel frontend (embedded HTML).
fn mcp_node(
    type_id: &str,
    display_name: &str,
    icon: &str,
    default_config: serde_json::Value,
    properties: serde_json::Value,
    required: &[&str],
    ports: Vec<PortDescription>,
    capabilities: Option<PluginCapabilities>,
) -> NodeDescription {
    let mut node = NodeDescription::new(type_id, display_name);
    node.category = Some("mcp".to_string());
    node.icon = Some(icon.to_string());
    node.color = Some("#F59E0B".to_string());
    node.ports = ports;
    node.default_config = default_config;
    node.capabilities = capabilities.unwrap_or_default();
    node.config_schema = Some(serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    }));
    node.frontend = Some(FrontendDescription {
        kind: "inline".into(),
        content: Some(include_str!("frontend/panel.html").to_string()),
        url: None, // 宿主归一化时生成 iframe URL
    });
    node
}

/// 操作节点（connect/call_tool/read_resource/get_prompt）的双输出端口：
/// `in`(输入) + `true`(成功) + `false`(失败)。失败不在此路由：节点返回
/// `ExecuteResult::fail` 后宿主按 `on_error` 策略（默认 RouteToFalse）自动
/// 走 `false` 端口。
fn operation_ports() -> Vec<PortDescription> {
    vec![
        PortDescription::new("in", i18n::t("输入", "Input"), PORT_KIND_IN),
        PortDescription::new("true", i18n::t("成功", "Success"), PORT_KIND_OUT),
        PortDescription::new("false", i18n::t("失败", "Failure"), PORT_KIND_OUT),
    ]
}

/// `mcp:close` 的单输出端口：`in`(输入) + `out`(输出)。
fn close_ports() -> Vec<PortDescription> {
    vec![
        PortDescription::new("in", i18n::t("输入", "Input"), PORT_KIND_IN),
        PortDescription::new("out", i18n::t("输出", "Output"), PORT_KIND_OUT),
    ]
}

/// 操作节点 config_schema 里的 `connection_id` 字段：宿主 `x-node-selector`
/// 渲染为对流程内 `mcp:connect` 节点的选择器，选中后把 connect 节点的
/// instance id 注入 config。
fn connection_id_property() -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "title": i18n::t("MCP 连接", "MCP Connection"),
        "x-node-selector": { "node_type": "mcp:connect" },
    })
}

/// 操作节点成功结果，显式选择 `true` 端口让宿主路由到下游成功分支。
///
/// 宿主把 execute 响应里的 `next_ports` 解析为连接路由
/// （flow-engine-plugin `wrapper.rs::resolve_next_node_ids`）：空列表意味着
/// "没有匹配的端口连接"，分支在此终止。因此所有成功路径都必须显式声明
/// `next_ports: ["true"]`，插件才能被宿主串联执行。失败不在此路由：返回
/// `ExecuteResult::fail` 后宿主按节点 `on_error` 策略处理（默认
/// RouteToFalse → `false` 端口），插件无需自报 false。
pub(crate) fn ok_result(output: serde_json::Value) -> ExecuteResult {
    ExecuteResult {
        next_ports: vec!["true".to_string()],
        ..ExecuteResult::ok(output)
    }
}

/// `mcp:close` 成功结果：close 只有单输出端口，路由到 `out`。
pub(crate) fn close_ok_result(output: serde_json::Value) -> ExecuteResult {
    ExecuteResult {
        next_ports: vec!["out".to_string()],
        ..ExecuteResult::ok(output)
    }
}

impl Plugin for McpPlugin {
    fn describe(&self) -> Vec<NodeDescription> {
        vec![
            // `mcp:connect` — 建立 MCP 连接（连接池按 (execution_id,
            // 节点实例) 复用）。声明 single_node capability：宿主单节点
            // 路径（execute_single_node / mpe run node）据此放行本节点的
            // 连接性验证。`discovered_*` 字段是面板「发现服务」写回的缓存。
            mcp_node(
                "mcp:connect",
                "MCP Connect",
                "plug",
                serde_json::json!({
                    "transport": "streamable_http",
                    "url": "",
                    "protocol_version": "2025-11-25",
                    "headers": {},
                    "command": "",
                    "args": [],
                    "env": {},
                    "timeout_ms": 30000,
                    "discovered_tools": [],
                    "discovered_resources": [],
                    "discovered_prompts": [],
                }),
                serde_json::json!({
                    "transport": { "type": "string", "enum": ["streamable_http", "stdio"], "description": i18n::t("传输模式", "Transport mode") },
                    "url": { "type": "string", "description": i18n::t("服务器 URL（streamable_http）", "Server URL (streamable_http)") },
                    "protocol_version": { "type": "string", "description": i18n::t("协议版本", "Protocol version") },
                    "headers": { "type": "object", "description": i18n::t("额外请求头", "Extra headers") },
                    "command": { "type": "string", "description": i18n::t("可执行文件（stdio）", "Executable (stdio)") },
                    "args": { "type": "array", "items": { "type": "string" }, "description": i18n::t("命令参数（stdio）", "Command args (stdio)") },
                    "env": { "type": "object", "description": i18n::t("环境变量（stdio）", "Environment (stdio)") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("连接超时（毫秒）", "Connect timeout (ms)") },
                }),
                &["transport"],
                operation_ports(),
                Some(PluginCapabilities {
                    single_node: true,
                    ..Default::default()
                }),
            ),
            // `mcp:call_tool` — 调用工具。
            mcp_node(
                "mcp:call_tool",
                "MCP Call Tool",
                "wrench",
                serde_json::json!({
                    "connection_id": "",
                    "tool_name": "",
                    "arguments": {},
                    "timeout_ms": 30000,
                }),
                serde_json::json!({
                    "connection_id": connection_id_property(),
                    "tool_name": { "type": "string", "description": i18n::t("工具名", "Tool name") },
                    "arguments": { "type": "object", "description": i18n::t("工具参数（按 input_schema）", "Tool arguments (per input_schema)") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("调用超时（毫秒）", "Call timeout (ms)") },
                }),
                &["connection_id", "tool_name"],
                operation_ports(),
                None,
            ),
            // `mcp:read_resource` — 读取资源。
            mcp_node(
                "mcp:read_resource",
                "MCP Read Resource",
                "file-search",
                serde_json::json!({
                    "connection_id": "",
                    "resource_uri": "",
                    "timeout_ms": 30000,
                }),
                serde_json::json!({
                    "connection_id": connection_id_property(),
                    "resource_uri": { "type": "string", "description": i18n::t("资源 URI", "Resource URI") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("读取超时（毫秒）", "Read timeout (ms)") },
                }),
                &["connection_id", "resource_uri"],
                operation_ports(),
                None,
            ),
            // `mcp:get_prompt` — 获取提示词。
            mcp_node(
                "mcp:get_prompt",
                "MCP Get Prompt",
                "message-square",
                serde_json::json!({
                    "connection_id": "",
                    "prompt_name": "",
                    "arguments": {},
                    "timeout_ms": 30000,
                }),
                serde_json::json!({
                    "connection_id": connection_id_property(),
                    "prompt_name": { "type": "string", "description": i18n::t("提示词名", "Prompt name") },
                    "arguments": { "type": "object", "description": i18n::t("提示词参数", "Prompt arguments") },
                    "timeout_ms": { "type": "integer", "description": i18n::t("获取超时（毫秒）", "Get prompt timeout (ms)") },
                }),
                &["connection_id", "prompt_name"],
                operation_ports(),
                None,
            ),
            // `mcp:close` — 主动释放连接。
            mcp_node(
                "mcp:close",
                "MCP Close",
                "unplug",
                serde_json::json!({
                    "connection_id": "",
                }),
                serde_json::json!({
                    "connection_id": connection_id_property(),
                }),
                &["connection_id"],
                close_ports(),
                None,
            ),
        ]
    }

    fn validate(&self, config: &serde_json::Value) -> Result<(), String> {
        let node_type = config
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match node_type {
            "mcp:connect" => {
                let transport = config
                    .get("transport")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                match transport {
                    "stdio" => {
                        let command = config
                            .get("command")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("");
                        if command.trim().is_empty() {
                            return Err(i18n::t(
                                "stdio 模式需要填写 command",
                                "stdio transport requires a command",
                            )
                            .to_string());
                        }
                    }
                    "streamable_http" => {
                        let url = config
                            .get("url")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("");
                        if url.trim().is_empty() {
                            return Err(i18n::t(
                                "streamable_http 模式需要填写 url",
                                "streamable_http transport requires a url",
                            )
                            .to_string());
                        }
                    }
                    other => {
                        return Err(format!(
                            "{}: `{other}`",
                            i18n::t(
                                "不支持的传输模式（streamable_http / stdio）",
                                "unsupported transport (streamable_http / stdio)",
                            )
                        ));
                    }
                }
                Ok(())
            }
            "mcp:call_tool" => {
                require_connection_id(config)?;
                if config
                    .get("tool_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(i18n::t("请填写 tool_name", "tool_name is required").to_string());
                }
                Ok(())
            }
            "mcp:read_resource" => {
                require_connection_id(config)?;
                if config
                    .get("resource_uri")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(
                        i18n::t("请填写 resource_uri", "resource_uri is required").to_string()
                    );
                }
                Ok(())
            }
            "mcp:get_prompt" => {
                require_connection_id(config)?;
                if config
                    .get("prompt_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(
                        i18n::t("请填写 prompt_name", "prompt_name is required").to_string()
                    );
                }
                Ok(())
            }
            "mcp:close" => require_connection_id(config),
            _ => Err(format!("{}: `{node_type}`", i18n::t("未知节点类型", "Unknown node type"))),
        }
    }

    // The runtime spawns execute futures, so the future must be Send; an
    // `async fn` cannot express that bound on stable (see `Plugin`).
    #[allow(clippy::manual_async_fn)]
    fn execute(&self, ctx: &mut ExecuteContext) -> impl Future<Output = ExecuteResult> + Send {
        async move {
            let node_type = ctx
                .config()
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match node_type {
                "mcp:connect" => nodes::connect::execute(ctx, &self.pool).await,
                "mcp:call_tool" => nodes::call_tool::execute(ctx, &self.pool).await,
                "mcp:read_resource" => nodes::read_resource::execute(ctx, &self.pool).await,
                "mcp:get_prompt" => nodes::get_prompt::execute(ctx, &self.pool).await,
                "mcp:close" => nodes::close::execute(ctx, &self.pool).await,
                other => ExecuteResult::fail(
                    i18n::t("未知节点类型", "Unknown node type").to_string()
                        + &format!(" `{other}`"),
                ),
            }
        }
    }

    // Flow-completion hook — release path 1 of the double-release design:
    // the host broadcasts flowEnded when a flow execution finishes (success,
    // failure, or cancellation), and the plugin releases the per-execution
    // connections here. The `mcp:close` node is release path 2 for flows
    // that disconnect mid-run; both may fire for one execution, and
    // `release` is a no-op on a missing entry, so double-release is
    // harmless.
    fn flow_ended(&self, execution_id: &str) {
        self.pool.release(execution_id);
    }

    // Config-panel discovery queries relayed by the host (`plugin_ui_call`).
    // Only `mcp.discover` is implemented; everything else is a readable
    // error (the SDK default would answer "uiCall not supported", which is
    // less actionable for a panel that knows it may probe).
    #[allow(clippy::manual_async_fn)]
    fn ui_call(&self, method: &str, params: serde_json::Value)
        -> impl Future<Output = Result<serde_json::Value, String>> + Send
    {
        let owned_method = method.to_string();
        async move {
            match owned_method.as_str() {
                "mcp.discover" => client::discover(&params).await,
                _ => Err(format!("unknown uiCall sub-method `{owned_method}`")),
            }
        }
    }
}

/// Validate helper: operation/close nodes require a non-empty
/// `connection_id` (the host `x-node-selector` fills it with a chosen
/// `mcp:connect` node's instance id).
fn require_connection_id(config: &serde_json::Value) -> Result<(), String> {
    if config
        .get("connection_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim()
        .is_empty()
    {
        return Err(i18n::t(
            "请选择 mcp:connect 连接节点",
            "Please select an mcp:connect connection node",
        )
        .to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Structural balance check for the panel's inline `<script>`: parens,
    /// brackets and braces must pair up outside of string literals and
    /// comments. A parse error in the panel JS means the host iframe never
    /// posts `ready` and the host config panel spins forever — the exact
    /// regression fixed in the `renderDiscovered` details blocks.
    fn panel_script_balanced(html: &str) -> bool {
        let Some(start) = html.find("<script>") else {
            return false;
        };
        let start = start + "<script>".len();
        let Some(rest) = html.get(start..) else {
            return false;
        };
        let Some(end) = rest.find("</script>") else {
            return false;
        };
        let js = &rest[..end];

        let mut stack: Vec<char> = Vec::new();
        let mut chars = js.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '/' if chars.peek() == Some(&'/') => {
                    for c in chars.by_ref() {
                        if c == '\n' {
                            break;
                        }
                    }
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    let mut prev = '\0';
                    for c in chars.by_ref() {
                        if prev == '*' && c == '/' {
                            break;
                        }
                        prev = c;
                    }
                }
                '\'' | '"' => {
                    let quote = c;
                    loop {
                        match chars.next() {
                            None => break,
                            Some('\\') => {
                                let _ = chars.next();
                            }
                            Some(c) if c == quote => break,
                            Some(_) => {}
                        }
                    }
                }
                '(' | '[' | '{' => stack.push(c),
                ')' | ']' | '}' => {
                    let Some(open) = stack.pop() else {
                        return false;
                    };
                    let pairs = [('(', ')'), ('[', ']'), ('{', '}')];
                    if !pairs.iter().any(|&(o, cl)| o == open && cl == c) {
                        return false;
                    }
                }
                _ => {}
            }
        }
        stack.is_empty()
    }

    /// The panel must implement the host bridge contract: it listens for the
    /// host's `init` message and signals readiness with `post("ready", …)`
    /// (clears the host's loading overlay).
    #[test]
    fn panel_script_has_bridge_and_balanced_js() {
        let nodes = McpPlugin::default().describe();
        for node in &nodes {
            let content = node
                .frontend
                .as_ref()
                .and_then(|f| f.content.as_deref())
                .unwrap_or_else(|| panic!("{} must declare inline frontend content", node.type_id));
            assert!(
                panel_script_balanced(content),
                "{} panel <script> must be balanced JS",
                node.type_id
            );
            assert!(
                content.contains("post(\"ready\"") || content.contains("post('ready'"),
                "{} panel must post the `ready` bridge message",
                node.type_id
            );
            assert!(
                content.contains("window.addEventListener(\"message\"")
                    || content.contains("window.addEventListener('message'"),
                "{} panel must listen for host messages",
                node.type_id
            );
        }
    }

    /// Every node must be GUI-usable in the host config panel: it carries a
    /// category, an inline frontend with non-empty content, a JSON-Schema
    /// `config_schema`, a schema-shaped `default_config` (no `type`
    /// discriminator key), and the correct port declarations.
    #[test]
    fn describe_metadata_shapes() {
        let nodes = McpPlugin::default().describe();
        assert_eq!(nodes.len(), 5, "all 5 mcp nodes must be described");

        let by_type: HashMap<&str, &NodeDescription> = nodes
            .iter()
            .map(|node| (node.type_id.as_str(), node))
            .collect();
        assert_eq!(by_type.len(), nodes.len(), "type_ids must be unique");

        for node in &nodes {
            assert_eq!(
                node.category.as_deref(),
                Some("mcp"),
                "{} must carry category `mcp`",
                node.type_id
            );
            assert!(node.icon.is_some(), "{} must declare an icon", node.type_id);
            assert_eq!(
                node.color.as_deref(),
                Some("#F59E0B"),
                "{} must declare the MCP amber color",
                node.type_id
            );
            assert!(
                node.default_config.get("type").is_none(),
                "{} default_config must not contain a `type` key",
                node.type_id
            );
            assert!(
                node.config_schema.is_some(),
                "{} must declare a config_schema",
                node.type_id
            );
            assert!(
                !node.ports.is_empty(),
                "{} must declare ports",
                node.type_id
            );
            let frontend = node
                .frontend
                .as_ref()
                .unwrap_or_else(|| panic!("{} must declare a frontend", node.type_id));
            assert_eq!(frontend.kind, "inline");
            assert!(
                frontend
                    .content
                    .as_ref()
                    .is_some_and(|c| c.len() >= 100),
                "{} inline panel content must be non-trivial",
                node.type_id
            );
            assert!(frontend.url.is_none(), "host generates the iframe url");
        }
    }

    /// `mcp:connect` declares the single-node capability (host connectivity
    /// test path); operation nodes do not.
    #[test]
    fn connect_declares_single_node_capability() {
        let nodes = McpPlugin::default().describe();
        let connect = nodes
            .iter()
            .find(|n| n.type_id == "mcp:connect")
            .expect("mcp:connect is described");
        assert_eq!(
            connect.capabilities.single_node,
            true,
            "mcp:connect must declare single_node capability"
        );

        for other in [
            "mcp:call_tool",
            "mcp:read_resource",
            "mcp:get_prompt",
            "mcp:close",
        ] {
            let node = nodes
                .iter()
                .find(|n| n.type_id == other)
                .expect("node is described");
            assert_ne!(
                node.capabilities.single_node,
                true,
                "{other} must NOT declare single_node capability"
            );
        }
    }

    /// Operation node config schemas expose the `connection_id` field with
    /// the `x-node-selector` pointing at `mcp:connect`.
    #[test]
    fn operation_schema_has_connection_selector() {
        let nodes = McpPlugin::default().describe();
        for node in &nodes {
            if node.type_id == "mcp:connect" {
                continue;
            }
            let schema = node.config_schema.as_ref().expect("schema");
            let connection = &schema["properties"]["connection_id"];
            assert_eq!(
                connection["x-node-selector"]["node_type"],
                "mcp:connect",
                "{} must expose an x-node-selector over mcp:connect",
                node.type_id
            );
        }
    }

    /// validate: connect requires transport-appropriate target; operation
    /// nodes require connection_id + their own key field.
    #[test]
    fn validate_rules() {
        let plugin = McpPlugin::default();
        // connect: stdio requires command
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:connect", "transport": "stdio", "command": "" }))
            .is_err());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:connect", "transport": "stdio", "command": "npx" }))
            .is_ok());
        // connect: streamable_http requires url
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:connect", "transport": "streamable_http", "url": "" }))
            .is_err());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:connect", "transport": "streamable_http", "url": "http://x" }))
            .is_ok());
        // connect: unsupported transport
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:connect", "transport": "ws" }))
            .is_err());
        // call_tool: connection_id + tool_name
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:call_tool", "connection_id": "", "tool_name": "echo" }))
            .is_err());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:call_tool", "connection_id": "c1", "tool_name": "" }))
            .is_err());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:call_tool", "connection_id": "c1", "tool_name": "echo" }))
            .is_ok());
        // read_resource / get_prompt / close: connection_id + own key
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:read_resource", "connection_id": "c1", "resource_uri": "config://x" }))
            .is_ok());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:read_resource", "connection_id": "c1" }))
            .is_err());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:get_prompt", "connection_id": "c1", "prompt_name": "greet" }))
            .is_ok());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:close", "connection_id": "c1" }))
            .is_ok());
        assert!(plugin
            .validate(&serde_json::json!({ "type": "mcp:close", "connection_id": "" }))
            .is_err());
        // unknown type
        assert!(plugin.validate(&serde_json::json!({ "type": "mcp:nope" })).is_err());
    }

    /// ui_call: only `mcp.discover` is accepted; anything else is a
    /// readable error. (`mcp.discover` itself needs a server — covered by
    /// the integration tests.)
    #[tokio::test]
    async fn ui_call_rejects_unknown_method() {
        let plugin = McpPlugin::default();
        let result = plugin.ui_call("mcp.unknown", serde_json::json!({})).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown uiCall sub-method"));
    }
}
