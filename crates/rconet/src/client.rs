use std::fmt;
use std::time::Duration;

use rconet_proto::{CommandOutput, DialectProfile, Preset};
use rconet_transport::FramedConnection;
use rconet_types::{SecretPassword, TimeoutConfig};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch};

use crate::actor::{ReconnectFn, SessionActor, SessionRequest};
use crate::error::Error;
use crate::policy::{ReconnectPolicy, ReplayMode};
use crate::state::{CommandOptions, ConnectionState};

/// Default bounded capacity of the session actor command channel.
pub const DEFAULT_QUEUE_CAPACITY: usize = 64;

/// Maximum bounded capacity permitted for the session actor command channel.
pub const MAX_QUEUE_CAPACITY: usize = 65_536;

/// Asynchronous, cheaply cloneable RCON client handle backed by a cancellation-safe session actor.
///
/// Cloning a [`Client`] shares the same underlying background session actor so multiple
/// tasks can safely queue commands concurrently without corrupting half-duplex wire framing.
#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<SessionRequest>,
    state_rx: watch::Receiver<ConnectionState>,
    shutdown_tx: watch::Sender<bool>,
    queue_capacity: usize,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("state", &*self.state_rx.borrow())
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

impl Client {
    /// Creates a [`ClientBuilder`] with conservative production defaults.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Spawns a cancellation-safe [`Client`] over an already-authenticated [`FramedConnection`].
    ///
    /// Useful for running RCON over custom streams (such as TLS wrappers, SSH port-forwarded
    /// streams, or `tokio::io::DuplexStream` in deterministic tests). Because no reconnect
    /// factory is attached, automatic reconnection is disabled.
    #[must_use]
    pub fn from_connection<S>(connection: FramedConnection<S>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::spawn_actor(
            connection,
            None,
            ReconnectPolicy::Disabled,
            ReplayMode::NeverReplay,
            DEFAULT_QUEUE_CAPACITY,
        )
    }

    pub(crate) fn spawn_actor<S>(
        initial_conn: FramedConnection<S>,
        reconnector: Option<ReconnectFn<S>>,
        reconnect_policy: ReconnectPolicy,
        replay_mode: ReplayMode,
        queue_capacity: usize,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let bounded_cap = queue_capacity.clamp(1, MAX_QUEUE_CAPACITY);
        let initial_state = ConnectionState::Ready {
            session_id: initial_conn.session_id(),
        };

        let (tx, rx) = mpsc::channel(bounded_cap);
        let (state_tx, state_rx) = watch::channel(initial_state);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let actor = SessionActor::new(
            initial_conn,
            reconnector,
            reconnect_policy,
            replay_mode,
            rx,
            state_tx,
            shutdown_rx,
        );
        tokio::spawn(actor.run());

        Self {
            tx,
            state_rx,
            shutdown_tx,
            queue_capacity: bounded_cap,
        }
    }

    /// Executes `command` using conservative defaults (`NonIdempotent`, client-default timeout).
    ///
    /// # Cancellation Guarantee
    ///
    /// Dropping the returned future does **not** corrupt the underlying TCP stream:
    /// - If the request has not yet left the queue, it is skipped without writing to the socket.
    /// - If the request is already in flight on the wire, the background session actor retains
    ///   ownership and drains its response frames within the protocol deadline before servicing
    ///   subsequent commands (or retires the socket if draining fails).
    pub async fn execute(&self, command: &str) -> Result<CommandOutput, Error> {
        self.execute_with(command, CommandOptions::default()).await
    }

    /// Executes `command` with explicit per-request [`CommandOptions`].
    pub async fn execute_with(
        &self,
        command: &str,
        options: CommandOptions,
    ) -> Result<CommandOutput, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = SessionRequest::Execute {
            command: command.to_owned(),
            options,
            reply_tx,
        };

        self.tx.send(req).await.map_err(|_| Error::ClientClosed)?;
        reply_rx.await.map_err(|_| Error::ClientClosed)?
    }

    /// Enqueues `command` without waiting if the bounded request queue is currently full,
    /// returning [`Error::QueueFull`] immediately on backpressure.
    pub async fn try_execute(
        &self,
        command: &str,
        options: CommandOptions,
    ) -> Result<CommandOutput, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = SessionRequest::Execute {
            command: command.to_owned(),
            options,
            reply_tx,
        };

        self.tx.try_send(req).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => Error::QueueFull {
                capacity: self.queue_capacity,
            },
            mpsc::error::TrySendError::Closed(_) => Error::ClientClosed,
        })?;

        reply_rx.await.map_err(|_| Error::ClientClosed)?
    }

    /// Returns a snapshot of the current [`ConnectionState`].
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Subscribes to asynchronous [`ConnectionState`] transitions (`Ready`, `Reconnecting`, `Closed`).
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    /// Signals any in-progress reconnect sleep to abort, flushes and closes the active stream,
    /// and waits for the background session actor to terminate cleanly.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let _ = self.shutdown_tx.send(true);
        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .tx
            .send(SessionRequest::Shutdown { ack_tx })
            .await
            .is_ok()
        {
            let _ = ack_rx.await;
        }
        Ok(())
    }
}

/// Ergonomic, redacted builder for configuring and connecting a [`Client`].
#[derive(Clone)]
pub struct ClientBuilder {
    address: Option<String>,
    password: Option<SecretPassword>,
    dialect: DialectProfile,
    connect_timeout: Duration,
    auth_timeout: Duration,
    command_timeout: Duration,
    frame_io_timeout: Duration,
    reconnect_policy: ReconnectPolicy,
    replay_mode: ReplayMode,
    queue_capacity: usize,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("address", &self.address)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("dialect", &self.dialect)
            .field("connect_timeout", &self.connect_timeout)
            .field("auth_timeout", &self.auth_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("frame_io_timeout", &self.frame_io_timeout)
            .field("reconnect_policy", &self.reconnect_policy)
            .field("replay_mode", &self.replay_mode)
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

impl ClientBuilder {
    /// Creates a new builder using [`Preset::Source`], [`ReconnectPolicy::Disabled`], and [`ReplayMode::NeverReplay`].
    #[must_use]
    pub fn new() -> Self {
        let defaults = TimeoutConfig::default();
        Self {
            address: None,
            password: None,
            dialect: DialectProfile::default(),
            connect_timeout: defaults.connect_timeout(),
            auth_timeout: defaults.auth_timeout(),
            command_timeout: defaults.command_timeout(),
            frame_io_timeout: defaults.frame_io_timeout(),
            reconnect_policy: ReconnectPolicy::Disabled,
            replay_mode: ReplayMode::NeverReplay,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }

    /// Sets the target `"host:port"` endpoint address.
    #[must_use]
    pub fn address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    /// Sets the authentication password (stored inside [`SecretPassword`]).
    #[must_use]
    pub fn password(mut self, password: impl Into<SecretPassword>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Selects a named server protocol [`Preset`].
    #[must_use]
    pub fn preset(mut self, preset: Preset) -> Self {
        self.dialect = DialectProfile::from_preset(preset);
        self
    }

    /// Sets a customized [`DialectProfile`].
    #[must_use]
    pub fn dialect(mut self, dialect: DialectProfile) -> Self {
        self.dialect = dialect;
        self
    }

    /// Applies an explicit validated [`TimeoutConfig`].
    #[must_use]
    pub fn timeouts(mut self, timeouts: TimeoutConfig) -> Self {
        self.connect_timeout = timeouts.connect_timeout();
        self.auth_timeout = timeouts.auth_timeout();
        self.command_timeout = timeouts.command_timeout();
        self.frame_io_timeout = timeouts.frame_io_timeout();
        self
    }

    /// Sets the TCP connection establishment timeout.
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the authentication handshake timeout.
    #[must_use]
    pub const fn auth_timeout(mut self, timeout: Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }

    /// Sets the default end-to-end command execution timeout.
    #[must_use]
    pub const fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Sets the per-frame socket read/write stall timeout.
    #[must_use]
    pub const fn frame_io_timeout(mut self, timeout: Duration) -> Self {
        self.frame_io_timeout = timeout;
        self
    }

    /// Configures automatic session reconnection behavior.
    #[must_use]
    pub const fn reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = policy;
        self
    }

    /// Configures conservative command replay behavior on [`rconet_types::CommandPhase::OutcomeUnknown`].
    #[must_use]
    pub const fn replay_mode(mut self, replay_mode: ReplayMode) -> Self {
        self.replay_mode = replay_mode;
        self
    }

    /// Sets the bounded session actor command queue capacity (`1..=65536`).
    #[must_use]
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.clamp(1, MAX_QUEUE_CAPACITY);
        self
    }

    /// Validates configuration, establishes a TCP connection, completes RCON authentication,
    /// and spawns the background session actor.
    #[cfg(feature = "tokio-tcp")]
    pub async fn connect(self) -> Result<Client, Error> {
        let address =
            self.address
                .filter(|addr| !addr.trim().is_empty())
                .ok_or(Error::InvalidAddress {
                    reason: "address must be specified and non-empty",
                })?;

        let password = self.password.unwrap_or_else(|| SecretPassword::new(""));

        let timeouts = TimeoutConfig::new(
            self.connect_timeout,
            self.auth_timeout,
            self.command_timeout,
            self.frame_io_timeout,
        )?;

        let initial_conn =
            FramedConnection::connect_tcp(&address, &password, self.dialect, timeouts)
                .await
                .map_err(Error::from_transport)?;

        let reconnector = if matches!(self.reconnect_policy, ReconnectPolicy::Disabled) {
            None
        } else {
            Some(crate::actor::tcp_reconnector(
                address,
                password,
                self.dialect,
                timeouts,
            ))
        };

        Ok(Client::spawn_actor(
            initial_conn,
            reconnector,
            self.reconnect_policy,
            self.replay_mode,
            self.queue_capacity,
        ))
    }
}
