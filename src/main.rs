//! Binary entry point: `__plugin_main` wires `McpPlugin` into the SDK event
//! loop (tokio runtime + JSON-RPC over stdio). Exit code 0 = clean shutdown
//! (EOF on stdin after flushing the last response).
//!
//! The plugin is a standalone process and must install its own rustls
//! CryptoProvider (rmcp's streamable-http transport uses rustls-backed
//! reqwest): `install_rustls` mirrors the host's `init_rustls`.
//!
//! Note: `mpe_plugin_main!(McpPlugin)` cannot be used here because the
//! macro generates its own `fn main` (which would nest inside ours and
//! never run). We call the underlying `__plugin_main` directly so the
//! rustls install runs first.

use mpe_plugin_mcp::McpPlugin;

/// Installs the rustls ring CryptoProvider (rustls 0.23+ requires an
/// explicit backend). Best-effort: a second install (e.g. tests that already
/// installed it) is harmless.
fn install_rustls() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn main() {
    install_rustls();
    std::process::exit(mpe_plugin_sdk::__plugin_main::<McpPlugin>());
}
