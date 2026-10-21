# MPE MCP Plugin

![CI](https://github.com/multi-protocol-flow/mpe-plugin-mcp/actions/workflows/ci.yml/badge.svg)

> **Positioning**: this plugin is one of MPE's **official protocol plugins** — it
> depends on none of the host repository's code, only the public
> `mpe-plugin-sdk` (sidecar process + JSON-RPC over stdio, git tag `v0.1.0`).
> It adds Model Context Protocol (MCP) operations to MPE via rmcp, over
> streamable-http and stdio transports.
>
> This repository is independent of the host (the host repository's `.gitignore`
> ignores `/plugins/` and the host never builds it). It is built and released by
> this repository's own CI (GitHub Actions → Release artifacts).

---

## 0. How the plugin works in one minute

```
Host (mpe / mpe-cli)                    Plugin process (this crate)
   │  scans plugins/ dir                       │
   │  ── describe ───────────────────────────► │  returns node descriptions (type, ports, config schema)
   │  ◄─────────── node list ─────────────────  │
   │  ── execute(config, execution_id) ──────► │  runs the MCP operation
   │  ◄─────────── result / variable updates ─  │
   │  ── flowEnded(execution_id) ────────────► │  releases the per-execution connection pools
```

- **Transport**: stdin/stdout, one JSON document per line (JSON-RPC 2.0, LF-framed)
- **Resident**: `capabilities.streaming: true` → the process stays alive, the
  connection pools are reused across executions
- **Single-node verification**: `mcp:connect` declares
  `capabilities.single_node: true` → the host's `mpe run-node` / GUI test button
  can verify reachability without any host code change
- **No shared memory**: the plugin is a separate process; the host can only pass
  values as JSON

## 1. Project structure

```
mpe-plugin-mcp/
├── Cargo.toml            # standalone package, no host workspace dependency
├── plugin.json           # manifest scanned by the host (launch description, residency mode)
├── .github/workflows/ci.yml  # 3-platform build + offline tests + Release packaging
├── src/
│   ├── main.rs           # installs the rustls ring CryptoProvider, then calls __plugin_main::<McpPlugin>
│   ├── lib.rs            # McpPlugin: describe/execute/validate + inline config frontend
│   ├── client.rs         # rmcp client (streamable-http + stdio transports)
│   ├── i18n.rs           # MPE_LOCALE-driven zh-CN / en-US copy
│   ├── pool.rs           # per-execution connection pool (execution_id, connection_id)
│   ├── frontend/
│   │   └── panel.html    # inline config panel (embedded via include_str!), MCP discovery UI
│   ├── nodes/
│   │   ├── mod.rs        # per-node execute dispatch
│   │   ├── connect.rs    # mcp:connect
│   │   ├── call_tool.rs  # mcp:call_tool
│   │   ├── read_resource.rs  # mcp:read_resource
│   │   ├── get_prompt.rs # mcp:get_prompt
│   │   └── close.rs      # mcp:close
│   └── bin/
│       └── test_server.rs    # stdio MCP test server (echo tool / config resource / greet prompt)
└── tests/
    ├── roundtrip.rs      # offline stdio JSON-RPC roundtrip tests
    └── integration.rs    # end-to-end: spawn test server + plugin over real stdio (no external MCP server)
```

## 2. Node types

| type_id | ports | description |
|---------|-------|-------------|
| `mcp:connect` | in/true/false | establish an MCP connection. `transport` enum `streamable_http\|stdio`; streamable_http: `url`, `protocol_version` (default `2025-11-25`), `headers`; stdio: `command`, `args`, `env`. Discovery cache fields `discovered_tools`/`discovered_resources`/`discovered_prompts`. Declares `single_node: true` |
| `mcp:call_tool` | in/true/false | call a tool. Required `connection_id` (rendered by the host's `x-node-selector` as a selector for `mcp:connect` nodes), `tool_name`; `arguments` per the tool's input_schema |
| `mcp:read_resource` | in/true/false | read a resource. Required `connection_id`, `resource_uri` |
| `mcp:get_prompt` | in/true/false | fetch a prompt. Required `connection_id`, `prompt_name`; optional `arguments` |
| `mcp:close` | in/out | release a connection (optional `connection_id` selects one; omitted releases the whole execution) |

All five nodes carry an inline config panel (HTML embedded in the binary); the
panel drives runtime discovery (`uiCall("mcp.discover")`) through the host's
`plugin_ui_call` relay.

`mcp:call_tool`/`mcp:read_resource`/`mcp:get_prompt`/`mcp:close`'s
`connection_id` field is rendered by the host's `x-node-selector` as a selector
for `mcp:connect` nodes in the flow; once selected, the host injects the
connect node's instance id into the config and the plugin fetches the
connection from its pool.

## 3. Cargo.toml (standalone build)

```toml
[package]
name = "mpe-plugin-mcp"
version = "0.1.0"
edition = "2021"

[dependencies]
mpe-plugin-sdk = { git = "https://github.com/multi-protocol-flow/mpe-plugin-sdk.git", tag = "v0.1.0" }
rmcp = { version = "1.5", default-features = false, features = ["client", "transport-streamable-http-client-reqwest", "transport-child-process", "server", "transport-io"] }
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
rustls = { version = "0.23", features = ["ring"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }

[[bin]]
name = "mpe_plugin_mcp"
path = "src/main.rs"

[[bin]]
name = "mcp_test_server"
path = "src/bin/test_server.rs"
```

Note: `main.rs` cannot use `mpe_plugin_main!` (the macro generates its own
`fn main`, which would nest inside ours and never run); it installs the rustls
ring CryptoProvider first, then calls `__plugin_main::<McpPlugin>` directly.

## 4. Build and test

> **plugin.json `entry.command`**: a relative path in marketplace-contract form,
> `./mpe_plugin_mcp`. The current host resolves the command against its own CWD
> (it does not chdir into the plugin directory). When developing in this
> repository, if you want the host to launch the plugin directly, either change
> `command` to an absolute path on your machine, or start the host from the
> plugin directory.

```bash
# build the release binary (the host launches it via plugin.json entry.command)
# run from the repository root (mpe-plugin-mcp)
cargo build --release

# offline tests (no external MCP server needed):
# roundtrip = stdio JSON-RPC roundtrip; integration spawns the bundled
# mcp_test_server via CARGO_BIN_EXE and drives the real stdio transport
cargo test
```

## 5. Install and verify (host side)

```bash
# development: build into the repository root (the host scans MPE_PLUGIN_DIR or
# the plugins directory under its data dir)
# run from the repository root (mpe-plugin-mcp)
cargo build --release

# single-node reachability verification (capability-driven, no host whitelist change)
mpe run-node '{"type":"mcp:connect","transport":"stdio","command":"/path/to/your/mcp/server"}'
```

Full end-to-end flows are covered by the repository's tests (spawned test
server over real stdio), or can be assembled in the MPE editor.

## 6. Design notes

- **rmcp client**: both transports are served by the rmcp SDK — streamable-http
  over rustls-backed reqwest, and stdio as a child process (command/args/env).
- **Pool isolation**: connections are keyed by the composite
  `(execution_id, connection_id)`; concurrent executions never interfere.
- **Discovery cache**: `mcp:connect` caches discovered tools/resources/prompts
  back into its config, so the panel can render them after a `mcp.discover`
  call without re-querying the server.
- **Explicit success routing**: operation nodes return `next_ports: ["true"]`
  (`ok_result`) so the host's `resolve_next_ids` chains them downstream; an
  empty list would terminate the branch. Failures are handled by the host
  `on_error` strategy (default RouteToFalse) — the plugin never self-reports
  `false`.
- **Inline config panel**: the panel HTML is embedded in the binary
  (`include_str!`) and shipped with `describe`; no external assets needed.
- **Fully offline tests**: `integration.rs` spawns the bundled stdio test
  server (echo tool / config resource / greet prompt) and the plugin, then
  drives the real SDK runtime + connection pool + rmcp — no external MCP
  server required.

## 7. Relationship with the host

- Dependency: only `mpe-plugin-sdk` (`mpe-plugin-sdk::protocol` wire contract) —
  **never** host types such as `flow-engine-core`.
- Interaction: describe/execute/validate/shutdown RPCs + log notifications +
  flowEnded notifications.
- Host adaptation point: `capabilities.single_node` is read by the host's
  `execute_single_node` / `mpe run-node` capability gate
  (`NodePlugin::supports_single_node()` + `SidecarPlugin` passthrough); the
  plugin never needs to know host implementation details.

## License

MIT — see [LICENSE](./LICENSE).
