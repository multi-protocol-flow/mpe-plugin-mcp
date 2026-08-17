//! Stdio MCP test server used by the plugin's integration tests.
//!
//! A minimal rmcp 1.8 server exposing:
//! - tool `echo` — parameter `message` (string), returns it verbatim as text;
//! - resource `config://app` — JSON text payload;
//! - prompt `greet` — parameter `name` (required string), replies
//!   "Hello, {name}!".
//!
//! The `Service<RoleServer>` trait is implemented manually (no `tool!`
//! macros): the plugin's integration tests only exercise the standard
//! tools/list+call, resources/list+read, prompts/list+get flows, so the
//! request dispatch stays small. All other requests answer
//! `-32601` (`Method not found`).

use std::borrow::Cow;
use std::future::Future;

use rmcp::model::{
    Annotated, CallToolResult, ClientNotification, ClientRequest, Content, EmptyResult, ErrorCode,
    ErrorData, GetPromptResult, Implementation, InitializeResult, ListPromptsResult,
    ListResourcesResult, ListToolsResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageRole, RawResource, ReadResourceResult, ResourceContents, ServerCapabilities,
    ServerInfo, ServerResult, Tool,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer, Service, ServiceExt};
use rmcp::transport;

/// The tool/resource/prompt names the tests assert against.
pub const ECHO_TOOL: &str = "echo";
pub const CONFIG_RESOURCE: &str = "config://app";
pub const GREET_PROMPT: &str = "greet";

#[derive(Debug, Clone, Default)]
struct TestServer;

fn capabilities() -> ServerCapabilities {
    ServerCapabilities::builder()
        .enable_tools()
        .enable_resources()
        .enable_prompts()
        .build()
}

fn echo_tool() -> Tool {
    let schema: serde_json::Map<String, serde_json::Value> = serde_json::json!({
        "type": "object",
        "properties": { "message": { "type": "string", "description": "Text to echo" } },
        "required": ["message"],
    })
    .as_object()
    .cloned()
    .expect("literal schema is an object");
    Tool::new(ECHO_TOOL, "Echo the message back", std::sync::Arc::new(schema))
}

fn config_resource() -> Annotated<RawResource> {
    Annotated::new(
        RawResource::new(CONFIG_RESOURCE, "config").with_description("App config"),
        None,
    )
}

fn greet_prompt() -> Prompt {
    Prompt::new(
        GREET_PROMPT,
        Some("Greet the user"),
        Some(vec![PromptArgument::new("name").with_required(true)]),
    )
}

fn handle(request: ClientRequest) -> Result<ServerResult, ErrorData> {
    match request {
        ClientRequest::InitializeRequest(_) => Ok(ServerResult::InitializeResult(
            InitializeResult::new(capabilities())
                .with_server_info(Implementation::new("mcp-test-server", "0.1.0")),
        )),
        ClientRequest::PingRequest(_) => Ok(ServerResult::EmptyResult(EmptyResult::from(()))),
        ClientRequest::ListToolsRequest(_) => Ok(ServerResult::ListToolsResult(
            ListToolsResult::with_all_items(vec![echo_tool()]),
        )),
        ClientRequest::CallToolRequest(req) => {
            let name = req.params.name.to_string();
            let args = req.params.arguments.unwrap_or_default();
            if name == ECHO_TOOL {
                let message = args
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                Ok(ServerResult::CallToolResult(CallToolResult::success(vec![
                    Content::text(message),
                ])))
            } else {
                Err(ErrorData {
                    code: ErrorCode::METHOD_NOT_FOUND,
                    message: Cow::Borrowed("tool not found"),
                    data: None,
                })
            }
        }
        ClientRequest::ListResourcesRequest(_) => Ok(ServerResult::ListResourcesResult(
            ListResourcesResult::with_all_items(vec![config_resource()]),
        )),
        ClientRequest::ReadResourceRequest(req) => {
            let uri = req.params.uri.clone();
            Ok(ServerResult::ReadResourceResult(ReadResourceResult::new(
                vec![ResourceContents::text(r#"{"hello":"mcp"}"#, uri)],
            )))
        }
        ClientRequest::ListPromptsRequest(_) => Ok(ServerResult::ListPromptsResult(
            ListPromptsResult::with_all_items(vec![greet_prompt()]),
        )),
        ClientRequest::GetPromptRequest(req) => {
            let name = req.params.name.clone();
            let args = req.params.arguments.unwrap_or_default();
            if name != GREET_PROMPT {
                return Err(ErrorData {
                    code: ErrorCode::METHOD_NOT_FOUND,
                    message: Cow::Borrowed("prompt not found"),
                    data: None,
                });
            }
            let who = args
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("world")
                .to_string();
            Ok(ServerResult::GetPromptResult(GetPromptResult::new(vec![
                PromptMessage::new_text(PromptMessageRole::User, format!("Hello, {who}!")),
            ])))
        }
        _ => Err(ErrorData {
            code: ErrorCode::METHOD_NOT_FOUND,
            message: Cow::Borrowed("method not supported by test server"),
            data: None,
        }),
    }
}

impl Service<RoleServer> for TestServer {
    fn handle_request(
        &self,
        request: ClientRequest,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ServerResult, ErrorData>> + Send + '_ {
        async move { handle(request) }
    }

    fn handle_notification(
        &self,
        _notification: ClientNotification,
        _context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = Result<(), ErrorData>> + Send + '_ {
        async move { Ok(()) }
    }

    fn get_info(&self) -> ServerInfo {
        InitializeResult::new(capabilities())
            .with_server_info(Implementation::new("mcp-test-server", "0.1.0"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = TestServer;
    let _running = service.serve(transport::stdio()).await?;
    // The server task keeps reading stdin; stay alive until the client drops
    // the transport (EOF → the serve task ends on its own).
    std::future::pending::<()>().await;
    Ok(())
}
