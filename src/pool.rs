//! Per-execution MCP connection pool.
//!
//! Keys ready-to-use [`McpConnection`]s by the composite
//! `(execution_id, connection_id)` pair so that:
//!
//! - concurrent flow executions never share one MCP connection (calls from
//!   unrelated executions would interleave on the same server session), and
//! - multiple `mcp:connect` nodes can coexist inside ONE execution —
//!   operation nodes pick their connection via the `connection_id` the host
//!   resolves into their config (the connect node's own instance id).
//!
//! Connection-key semantics (same as the redis plugin, production-proven):
//! the connect node registers under its own instance id
//! (`ctx.node_instance_id()`), which equals the canvas node uuid; operation
//! nodes look up by the `connection_id` field the host's `x-node-selector`
//! fills with the chosen `mcp:connect` node's instance id.
//!
//! The SDK's [`ConnectionPool`] stores values under opaque `String` keys and
//! exposes no key enumeration, so this crate keeps a small side registry
//! (`execution_id → set of connection_ids`) to support releasing every
//! connection of one execution by prefix ([`McpPool::release`]).

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::client::McpConnection;

/// Execution + connection-keyed pool of payloads, shared across executions
/// in this plugin process (the plugin is resident:
/// `capabilities.streaming: true`, so the pool lives for the process
/// lifetime).
///
/// Concurrency: the underlying [`ConnectionPool`] may run an insert factory
/// more than once under contention; that is benign here because `connect`
/// only inserts after checking the existing entry. The side registry uses a
/// plain [`Mutex`] and is only ever touched with short critical sections.
pub struct McpPool<C: Any + Send + Sync = McpConnection> {
    inner: mpe_plugin_sdk::pool::ConnectionPool,
    /// `execution_id → set of registered connection_ids`, so
    /// [`McpPool::release`] can drop every connection of one execution even
    /// though the SDK pool exposes no key enumeration.
    connections: Mutex<HashMap<String, HashSet<String>>>,
    /// Keeps the payload type in the struct signature for typed accessors.
    _marker: PhantomData<fn() -> C>,
}

impl<C: Any + Send + Sync> Default for McpPool<C> {
    fn default() -> Self {
        Self {
            inner: mpe_plugin_sdk::pool::ConnectionPool::default(),
            connections: Mutex::new(HashMap::new()),
            _marker: PhantomData,
        }
    }
}

impl<C: Any + Send + Sync> McpPool<C> {
    /// Registers the ready `payload` for `(execution_id, connection_id)`.
    ///
    /// `key_match` decides whether an existing entry under the same key is
    /// the SAME target (reuse, no replacement) or a DIFFERENT one (drop
    /// stale + replace).
    ///
    /// Returns `(payload, replaced)` where `replaced` is `true` only when an
    /// entry with a DIFFERENT target existed and was replaced — the caller
    /// (connect node) can surface a warning via `ctx.log`. A fresh insertion
    /// reports `false`.
    pub fn connect(
        &self,
        execution_id: &str,
        connection_id: &str,
        payload: C,
        key_match: impl Fn(&C, &C) -> bool,
    ) -> (Arc<C>, bool) {
        let replaced = match self.connection(execution_id, connection_id) {
            Some(existing) if key_match(&existing, &payload) => {
                // Same target: reuse, do not touch the stored payload.
                return (existing, false);
            }
            Some(_) => {
                // Different target: drop the stale connection first so the
                // new one is the only entry under this key.
                self.inner.remove(&pool_key(execution_id, connection_id));
                true
            }
            None => false,
        };
        let created =
            self.inner
                .get_or_insert(pool_key(execution_id, connection_id), || payload);
        self.connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(execution_id.to_string())
            .or_default()
            .insert(connection_id.to_string());
        (created, replaced)
    }

    /// Returns the payload for `(execution_id, connection_id)`, or `None`
    /// when that connection has not been established (or already released).
    pub fn connection(&self, execution_id: &str, connection_id: &str) -> Option<Arc<C>> {
        self.inner
            .get::<C>(&pool_key(execution_id, connection_id))
    }

    /// Drops EVERY connection registered under `execution_id` (close node
    /// without a specific selection / `flow_ended` callback). A no-op when
    /// the execution has no entries.
    pub fn release(&self, execution_id: &str) {
        let ids: Vec<String> = self
            .connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(execution_id)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default();
        for id in ids {
            self.inner.remove(&pool_key(execution_id, &id));
        }
    }

    /// Drops ONE connection of `execution_id` (close node with a specific
    /// `connection_id` selection). A no-op when the entry is missing.
    pub fn release_connection(&self, execution_id: &str, connection_id: &str) -> bool {
        let removed = self.inner.remove(&pool_key(execution_id, connection_id));
        if removed {
            self.connections
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(execution_id.to_string())
                .or_default()
                .remove(connection_id);
        }
        removed
    }
}

/// Composite pool key: `execution_id` and `connection_id` are opaque host
/// strings, `:` is a safe separator because neither contains it (the host
/// generates both as plain uuids/ids).
fn pool_key(execution_id: &str, connection_id: &str) -> String {
    format!("{execution_id}:{connection_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cheap, server-free payload for keying tests (no MCP server needed).
    #[derive(Debug, Clone, PartialEq)]
    struct Dummy {
        target: String,
    }

    fn seed(pool: &McpPool<Dummy>, exec_id: &str, id: &str, target: &str) {
        pool.connect(
            exec_id,
            id,
            Dummy {
                target: target.to_string(),
            },
            |a, b| a.target == b.target,
        );
    }

    /// Same key + same target → reused, not replaced.
    #[test]
    fn same_target_is_reused() {
        let pool = McpPool::<Dummy>::default();
        let (first, replaced1) = pool.connect(
            "exec-1",
            "conn-1",
            Dummy {
                target: "server-a".to_string(),
            },
            |a, b| a.target == b.target,
        );
        assert!(!replaced1);
        let (second, replaced2) = pool.connect(
            "exec-1",
            "conn-1",
            Dummy {
                target: "server-a".to_string(),
            },
            |a, b| a.target == b.target,
        );
        assert!(!replaced2);
        assert!(Arc::ptr_eq(&first, &second), "same target must reuse the entry");
    }

    /// Same key + different target → stale entry replaced.
    #[test]
    fn different_target_is_replaced() {
        let pool = McpPool::<Dummy>::default();
        seed(&pool, "exec-1", "conn-1", "server-a");
        let (replacement, replaced) = pool.connect(
            "exec-1",
            "conn-1",
            Dummy {
                target: "server-b".to_string(),
            },
            |a, b| a.target == b.target,
        );
        assert!(replaced);
        assert_eq!(replacement.target, "server-b");
        assert_eq!(pool.connection("exec-1", "conn-1").unwrap().target, "server-b");
    }

    /// Executions are isolated: same connection id, different execution.
    #[test]
    fn executions_are_isolated() {
        let pool = McpPool::<Dummy>::default();
        seed(&pool, "exec-1", "conn-1", "server-a");
        seed(&pool, "exec-2", "conn-1", "server-a");
        assert!(pool.connection("exec-1", "conn-1").is_some());
        assert!(pool.connection("exec-2", "conn-1").is_some());
        pool.release("exec-1");
        assert!(pool.connection("exec-1", "conn-1").is_none());
        assert!(pool.connection("exec-2", "conn-1").is_some());
    }

    /// release drops every connection of one execution; missing entries are
    /// a silent no-op.
    #[test]
    fn release_drops_all_and_is_idempotent() {
        let pool = McpPool::<Dummy>::default();
        seed(&pool, "exec-1", "conn-1", "server-a");
        seed(&pool, "exec-1", "conn-2", "server-b");
        pool.release("exec-1");
        assert!(pool.connection("exec-1", "conn-1").is_none());
        assert!(pool.connection("exec-1", "conn-2").is_none());
        pool.release("exec-1"); // idempotent
        pool.release("never-connected"); // no-op
    }

    /// release_connection drops exactly one connection.
    #[test]
    fn release_connection_drops_selected_only() {
        let pool = McpPool::<Dummy>::default();
        seed(&pool, "exec-1", "conn-1", "server-a");
        seed(&pool, "exec-1", "conn-2", "server-b");
        assert!(pool.release_connection("exec-1", "conn-1"));
        assert!(pool.connection("exec-1", "conn-1").is_none());
        assert!(pool.connection("exec-1", "conn-2").is_some());
        assert!(!pool.release_connection("exec-1", "conn-1")); // already gone
    }
}
