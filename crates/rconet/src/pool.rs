use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use rconet_proto::CommandOutput;
use rconet_types::{MAX_TIMEOUT_DURATION, TypeError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout};

use crate::client::Client;
#[cfg(feature = "tokio-tcp")]
use crate::client::ClientBuilder;
use crate::error::Error;
use crate::state::CommandOptions;

/// Hard upper bound on simultaneous RCON sockets opened by a single [`ServerPool`].
///
/// Game servers enforce strict admin connection limits and may auto-ban IPs that open
/// dozens of concurrent RCON sockets.
pub const MAX_POOL_SESSIONS: usize = 32;

/// Default idle session lifetime (`5` minutes) before a pooled [`Client`] is evicted on checkout.
pub const DEFAULT_MAX_IDLE_DURATION: Duration = Duration::from_secs(300);

type BoxClientFuture = Pin<Box<dyn Future<Output = Result<Client, Error>> + Send>>;
type ConnectorFn = dyn Fn() -> BoxClientFuture + Send + Sync;

struct IdleEntry {
    client: Client,
    returned_at: Instant,
}

/// Bounded, lazily-populated pool of authenticated [`Client`] sessions to a single server.
///
/// Useful when a server allows multiple parallel RCON connections and callers want to avoid
/// head-of-line blocking behind slow commands on a single half-duplex TCP socket.
#[derive(Clone)]
pub struct ServerPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    connector: Arc<ConnectorFn>,
    max_sessions: usize,
    acquire_timeout: Duration,
    max_idle_duration: Duration,
    semaphore: Arc<Semaphore>,
    idle: Mutex<Vec<IdleEntry>>,
}

impl fmt::Debug for ServerPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let idle_len = self
            .inner
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("ServerPool")
            .field("max_sessions", &self.inner.max_sessions)
            .field("idle_sessions", &idle_len)
            .field("acquire_timeout", &self.inner.acquire_timeout)
            .field("max_idle_duration", &self.inner.max_idle_duration)
            .finish()
    }
}

impl ServerPool {
    /// Creates a new lazy [`ServerPool`] over Tokio TCP capped at `max_sessions` (`1..=32`).
    #[cfg(feature = "tokio-tcp")]
    pub fn new(
        builder: ClientBuilder,
        max_sessions: usize,
        acquire_timeout: Duration,
    ) -> Result<Self, Error> {
        Self::with_connector(
            move || {
                let b = builder.clone();
                async move { b.connect().await }
            },
            max_sessions,
            acquire_timeout,
        )
    }

    /// Creates a new lazy [`ServerPool`] using a custom asynchronous [`Client`] factory.
    ///
    /// Useful for pooling [`Client::from_connection`] sessions over custom streams (such as
    /// TLS tunnels, SSH port-forwarded channels, or deterministic test transports).
    pub fn with_connector<F, Fut>(
        connector: F,
        max_sessions: usize,
        acquire_timeout: Duration,
    ) -> Result<Self, Error>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Client, Error>> + Send + 'static,
    {
        if max_sessions == 0 || max_sessions > MAX_POOL_SESSIONS {
            return Err(Error::Config(TypeError::InvalidFrameLimit {
                field: "max_sessions",
                value: max_sessions,
            }));
        }
        if acquire_timeout.is_zero() {
            return Err(Error::Config(TypeError::ZeroTimeout {
                field: "acquire_timeout",
            }));
        }
        if acquire_timeout > MAX_TIMEOUT_DURATION {
            return Err(Error::Config(TypeError::ExcessiveTimeout {
                field: "acquire_timeout",
                max: MAX_TIMEOUT_DURATION,
            }));
        }

        let boxed_connector: Arc<ConnectorFn> = Arc::new(move || Box::pin(connector()));

        Ok(Self {
            inner: Arc::new(PoolInner {
                connector: boxed_connector,
                max_sessions,
                acquire_timeout,
                max_idle_duration: DEFAULT_MAX_IDLE_DURATION,
                semaphore: Arc::new(Semaphore::new(max_sessions)),
                idle: Mutex::new(Vec::with_capacity(max_sessions)),
            }),
        })
    }

    /// Overrides the maximum duration an idle session may sit in the pool before being replaced
    /// on checkout (default: [`DEFAULT_MAX_IDLE_DURATION`], `5` minutes).
    pub fn with_max_idle(mut self, max_idle: Duration) -> Result<Self, Error> {
        if max_idle.is_zero() {
            return Err(Error::Config(TypeError::ZeroTimeout { field: "max_idle" }));
        }
        if max_idle > MAX_TIMEOUT_DURATION {
            return Err(Error::Config(TypeError::ExcessiveTimeout {
                field: "max_idle",
                max: MAX_TIMEOUT_DURATION,
            }));
        }
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.max_idle_duration = max_idle;
        } else {
            self.inner = Arc::new(PoolInner {
                connector: Arc::clone(&self.inner.connector),
                max_sessions: self.inner.max_sessions,
                acquire_timeout: self.inner.acquire_timeout,
                max_idle_duration: max_idle,
                semaphore: Arc::new(Semaphore::new(self.inner.max_sessions)),
                idle: Mutex::new(Vec::with_capacity(self.inner.max_sessions)),
            });
        }
        Ok(self)
    }

    /// Maximum concurrent [`Client`] sessions permitted by this pool.
    #[must_use]
    pub fn max_sessions(&self) -> usize {
        self.inner.max_sessions
    }

    /// Number of currently idle [`Client`] sessions sitting in the pool.
    #[must_use]
    pub fn idle_count(&self) -> usize {
        self.inner
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Checks out a healthy [`PooledClient`] guard, opening a new session lazily if needed.
    pub async fn acquire(&self) -> Result<PooledClient, Error> {
        let permit = timeout(
            self.inner.acquire_timeout,
            Arc::clone(&self.inner.semaphore).acquire_owned(),
        )
        .await
        .map_err(|_| Error::PoolAcquireTimeout {
            timeout: self.inner.acquire_timeout,
        })?
        .map_err(|_| Error::ClientClosed)?;

        // Pop idle clients until we find one that is currently Ready and within its idle TTL,
        // discarding any closed or expired sessions.
        while let Some(entry) = self.pop_idle() {
            if entry.client.state().is_ready()
                && entry.returned_at.elapsed() <= self.inner.max_idle_duration
            {
                return Ok(PooledClient {
                    client: Some(entry.client),
                    pool: Arc::clone(&self.inner),
                    _permit: permit,
                });
            }
            entry.client.signal_shutdown();
        }

        let fresh = (self.inner.connector)().await?;
        Ok(PooledClient {
            client: Some(fresh),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }

    /// Acquires a session, executes `command` with conservative defaults, and returns the session.
    pub async fn execute(&self, command: &str) -> Result<CommandOutput, Error> {
        self.execute_with(command, CommandOptions::default()).await
    }

    /// Acquires a session, executes `command` with `options`, and returns the session.
    pub async fn execute_with(
        &self,
        command: &str,
        options: CommandOptions,
    ) -> Result<CommandOutput, Error> {
        let lease = self.acquire().await?;
        lease.execute_with(command, options).await
    }

    /// Closes the pool and gracefully shuts down all currently idle sessions.
    pub async fn shutdown(&self) {
        self.inner.semaphore.close();
        let drained: Vec<IdleEntry> = {
            let mut guard = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut *guard)
        };
        for entry in drained {
            let _ = entry.client.shutdown().await;
        }
    }

    fn pop_idle(&self) -> Option<IdleEntry> {
        let mut guard = self
            .inner
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        guard.pop()
    }
}

/// RAII lease holding an authenticated [`Client`] checked out from a [`ServerPool`].
///
/// When dropped, the underlying [`Client`] is returned to the pool only if the pool remains open
/// and the session is still in [`crate::ConnectionState::Ready`].
pub struct PooledClient {
    client: Option<Client>,
    pool: Arc<PoolInner>,
    _permit: OwnedSemaphorePermit,
}

impl fmt::Debug for PooledClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledClient")
            .field("client", &self.client)
            .finish()
    }
}

impl PooledClient {
    /// Executes `command` on the leased [`Client`].
    pub async fn execute(&self, command: &str) -> Result<CommandOutput, Error> {
        self.execute_with(command, CommandOptions::default()).await
    }

    /// Executes `command` with explicit [`CommandOptions`] on the leased [`Client`].
    pub async fn execute_with(
        &self,
        command: &str,
        options: CommandOptions,
    ) -> Result<CommandOutput, Error> {
        let Some(client) = &self.client else {
            return Err(Error::ClientClosed);
        };
        client.execute_with(command, options).await
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if !self.pool.semaphore.is_closed() && client.state().is_ready() {
                let mut guard = self
                    .pool
                    .idle
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                guard.push(IdleEntry {
                    client,
                    returned_at: Instant::now(),
                });
            } else {
                client.signal_shutdown();
            }
        }
    }
}
