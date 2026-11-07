use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use rconet_proto::CommandOutput;
use rconet_types::ServerId;
use tokio::sync::{RwLock, Semaphore, broadcast};
use tokio::task::{AbortHandle, JoinSet};

use crate::client::Client;
use crate::error::Error;
use crate::state::{CommandOptions, ConnectionState};

/// Default maximum number of servers queried simultaneously during a fleet broadcast.
pub const DEFAULT_FLEET_CONCURRENCY: usize = 16;

/// Upper bound on fleet broadcast fan-out concurrency.
pub const MAX_FLEET_CONCURRENCY: usize = 512;

/// Default bounded capacity of the [`FleetManager`] lifecycle event broadcast channel.
pub const DEFAULT_FLEET_EVENT_CAPACITY: usize = 256;

/// Target selection filter for [`FleetManager::broadcast`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetFilter {
    /// Every server currently registered in the fleet.
    All,
    /// Only servers carrying the specified metadata tag.
    WithTag(String),
    /// Explicit allowlist of [`ServerId`] identifiers.
    ServerIds(BTreeSet<ServerId>),
}

/// Lifecycle and connection-state events aggregated across all servers in a [`FleetManager`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetEvent {
    /// A server was added to (or replaced inside) the fleet registry.
    ServerRegistered {
        /// Identifier of the registered server.
        server_id: ServerId,
        /// Initial connection state at registration time.
        state: ConnectionState,
    },
    /// A server was removed from the fleet registry.
    ServerRemoved {
        /// Identifier of the removed server.
        server_id: ServerId,
    },
    /// A registered server transitioned to a new [`ConnectionState`].
    StateChanged {
        /// Identifier of the server whose connection state changed.
        server_id: ServerId,
        /// Updated state (`Ready`, `Reconnecting`, or `Closed`).
        state: ConnectionState,
    },
}

struct WatcherGuard(AbortHandle);

impl Drop for WatcherGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct ServerEntry {
    client: Client,
    tags: BTreeSet<String>,
    _watcher: WatcherGuard,
}

/// Multi-server RCON registry supporting targeted execution, tag-filtered broadcasts with
/// bounded concurrency, instantaneous health snapshots, and reactive event subscriptions.
#[derive(Clone)]
pub struct FleetManager {
    servers: Arc<RwLock<BTreeMap<ServerId, ServerEntry>>>,
    broadcast_semaphore: Arc<Semaphore>,
    event_tx: broadcast::Sender<FleetEvent>,
    max_concurrency: usize,
}

impl Default for FleetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for FleetManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FleetManager")
            .field("max_concurrency", &self.max_concurrency)
            .finish()
    }
}

impl FleetManager {
    /// Creates an empty [`FleetManager`] with [`DEFAULT_FLEET_CONCURRENCY`] (`16`).
    #[must_use]
    pub fn new() -> Self {
        Self::with_max_concurrency(DEFAULT_FLEET_CONCURRENCY)
    }

    /// Creates an empty [`FleetManager`] capping simultaneous broadcast executions at `max_concurrency`.
    #[must_use]
    pub fn with_max_concurrency(max_concurrency: usize) -> Self {
        let bounded = max_concurrency.clamp(1, MAX_FLEET_CONCURRENCY);
        let (event_tx, _) = broadcast::channel(DEFAULT_FLEET_EVENT_CAPACITY);
        Self {
            servers: Arc::new(RwLock::new(BTreeMap::new())),
            broadcast_semaphore: Arc::new(Semaphore::new(bounded)),
            event_tx,
            max_concurrency: bounded,
        }
    }

    /// Subscribes to aggregated [`FleetEvent`] notifications across all registered servers.
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<FleetEvent> {
        self.event_tx.subscribe()
    }

    /// Registers (or replaces) a server entry in the fleet, returning any previous [`Client`].
    pub async fn register<I, T>(&self, id: ServerId, client: Client, tags: I) -> Option<Client>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let tag_set = tags.into_iter().map(Into::into).collect();
        let initial_state = client.state();
        let watcher = spawn_state_watcher(id.clone(), client.clone(), self.event_tx.clone());

        let prev = {
            let mut guard = self.servers.write().await;
            guard
                .insert(
                    id.clone(),
                    ServerEntry {
                        client,
                        tags: tag_set,
                        _watcher: watcher,
                    },
                )
                .map(|entry| entry.client)
        };

        let _ = self.event_tx.send(FleetEvent::ServerRegistered {
            server_id: id,
            state: initial_state,
        });

        prev
    }

    /// Removes a server from the registry without shutting down its [`Client`].
    pub async fn remove(&self, id: &ServerId) -> Option<Client> {
        let removed = {
            let mut guard = self.servers.write().await;
            guard.remove(id).map(|entry| entry.client)
        };
        if removed.is_some() {
            let _ = self.event_tx.send(FleetEvent::ServerRemoved {
                server_id: id.clone(),
            });
        }
        removed
    }

    /// Number of servers currently registered.
    pub async fn len(&self) -> usize {
        self.servers.read().await.len()
    }

    /// Returns `true` if no servers are registered.
    pub async fn is_empty(&self) -> bool {
        self.servers.read().await.is_empty()
    }

    /// Executes `command` on a single registered server using default [`CommandOptions`].
    pub async fn execute(&self, id: &ServerId, command: &str) -> Result<CommandOutput, Error> {
        self.execute_with(id, command, CommandOptions::default())
            .await
    }

    /// Executes `command` on a single registered server with explicit [`CommandOptions`].
    pub async fn execute_with(
        &self,
        id: &ServerId,
        command: &str,
        options: CommandOptions,
    ) -> Result<CommandOutput, Error> {
        let client = {
            let guard = self.servers.read().await;
            guard
                .get(id)
                .map(|entry| entry.client.clone())
                .ok_or_else(|| Error::UnknownServer {
                    server_id: id.clone(),
                })?
        };
        client.execute_with(command, options).await
    }

    /// Broadcasts `command` concurrently across all servers matching `filter`.
    ///
    /// Failures on individual servers are captured inside the returned map without aborting
    /// healthy peers. Concurrency is bounded by `max_concurrency`.
    pub async fn broadcast(
        &self,
        filter: &FleetFilter,
        command: &str,
        options: CommandOptions,
    ) -> BTreeMap<ServerId, Result<CommandOutput, Error>> {
        let targets: Vec<(ServerId, Client)> = {
            let guard = self.servers.read().await;
            guard
                .iter()
                .filter(|(id, entry)| matches_filter(filter, id, &entry.tags))
                .map(|(id, entry)| (id.clone(), entry.client.clone()))
                .collect()
        };

        let mut join_set = JoinSet::new();
        for (server_id, client) in targets {
            let sem = Arc::clone(&self.broadcast_semaphore);
            let cmd = command.to_owned();
            join_set.spawn(async move {
                let Ok(_permit) = sem.acquire_owned().await else {
                    return (server_id, Err(Error::ClientClosed));
                };
                let res = client.execute_with(&cmd, options).await;
                (server_id, res)
            });
        }

        let mut results = BTreeMap::new();
        while let Some(joined) = join_set.join_next().await {
            if let Ok((server_id, outcome)) = joined {
                results.insert(server_id, outcome);
            }
        }
        results
    }

    /// Returns a point-in-time [`ConnectionState`] map across all registered servers.
    pub async fn health_snapshot(&self) -> BTreeMap<ServerId, ConnectionState> {
        let guard = self.servers.read().await;
        guard
            .iter()
            .map(|(id, entry)| (id.clone(), entry.client.state()))
            .collect()
    }

    /// Removes and gracefully shuts down every registered server session concurrently.
    pub async fn shutdown_all(&self) {
        let entries: Vec<(ServerId, ServerEntry)> = {
            let mut guard = self.servers.write().await;
            let map = std::mem::take(&mut *guard);
            map.into_iter().collect()
        };

        let mut join_set = JoinSet::new();
        for (server_id, entry) in entries {
            let _ = self.event_tx.send(FleetEvent::ServerRemoved { server_id });
            join_set.spawn(async move {
                let _ = entry.client.shutdown().await;
            });
        }
        while join_set.join_next().await.is_some() {}
    }
}

fn spawn_state_watcher(
    server_id: ServerId,
    client: Client,
    event_tx: broadcast::Sender<FleetEvent>,
) -> WatcherGuard {
    let mut state_rx = client.subscribe_state();
    // Mark current initial value seen so only genuine transitions emit `StateChanged`.
    let _ = *state_rx.borrow_and_update();

    let handle = tokio::spawn(async move {
        while state_rx.changed().await.is_ok() {
            let state = *state_rx.borrow_and_update();
            let is_closed = state.is_closed();
            let _ = event_tx.send(FleetEvent::StateChanged {
                server_id: server_id.clone(),
                state,
            });
            if is_closed {
                break;
            }
        }
    });

    WatcherGuard(handle.abort_handle())
}

fn matches_filter(filter: &FleetFilter, id: &ServerId, tags: &BTreeSet<String>) -> bool {
    match filter {
        FleetFilter::All => true,
        FleetFilter::WithTag(tag) => tags.contains(tag),
        FleetFilter::ServerIds(allowlist) => allowlist.contains(id),
    }
}
