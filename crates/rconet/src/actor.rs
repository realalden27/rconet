use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rconet_proto::{CommandOutput, DialectProfile};
use rconet_transport::{FramedConnection, TransportError};
use rconet_types::{CommandPhase, Idempotency, SecretPassword, TimeoutConfig, TypeError};
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "tokio-tcp")]
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::error::Error;
use crate::policy::{ReconnectPolicy, ReplayMode};
use crate::state::{CommandOptions, ConnectionState};

pub(crate) enum SessionRequest {
    Execute {
        command: String,
        options: CommandOptions,
        reply_tx: oneshot::Sender<Result<CommandOutput, Error>>,
    },
    Shutdown {
        ack_tx: oneshot::Sender<()>,
    },
}

pub(crate) type BoxConnectFuture<S> =
    Pin<Box<dyn Future<Output = Result<FramedConnection<S>, TransportError>> + Send>>;

pub(crate) type ReconnectFn<S> = Arc<dyn Fn() -> BoxConnectFuture<S> + Send + Sync>;

#[cfg(feature = "tokio-tcp")]
pub(crate) fn tcp_reconnector(
    address: String,
    password: SecretPassword,
    dialect: DialectProfile,
    timeouts: TimeoutConfig,
) -> ReconnectFn<TcpStream> {
    Arc::new(move || {
        let addr = address.clone();
        let pw = password.clone();
        Box::pin(async move { FramedConnection::connect_tcp(addr, &pw, dialect, timeouts).await })
    })
}

pub(crate) struct SessionActor<S> {
    conn: Option<FramedConnection<S>>,
    reconnector: Option<ReconnectFn<S>>,
    dialect: DialectProfile,
    timeouts: TimeoutConfig,
    reconnect_policy: ReconnectPolicy,
    replay_mode: ReplayMode,
    rx: mpsc::Receiver<SessionRequest>,
    state_tx: watch::Sender<ConnectionState>,
    shutdown_rx: watch::Receiver<bool>,
    last_command_completed: Option<Instant>,
}

impl<S> SessionActor<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        initial_conn: FramedConnection<S>,
        reconnector: Option<ReconnectFn<S>>,
        reconnect_policy: ReconnectPolicy,
        replay_mode: ReplayMode,
        rx: mpsc::Receiver<SessionRequest>,
        state_tx: watch::Sender<ConnectionState>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        let dialect = initial_conn.dialect();
        let timeouts = initial_conn.timeouts();
        Self {
            conn: Some(initial_conn),
            reconnector,
            dialect,
            timeouts,
            reconnect_policy,
            replay_mode,
            rx,
            state_tx,
            shutdown_rx,
            last_command_completed: None,
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(req) = self.rx.recv().await {
            match req {
                SessionRequest::Execute {
                    command,
                    options,
                    reply_tx,
                } => {
                    self.handle_execute(command, options, reply_tx).await;
                    if self.state_tx.borrow().is_closed() {
                        break;
                    }
                },
                SessionRequest::Shutdown { ack_tx } => {
                    self.graceful_close().await;
                    let _ = ack_tx.send(());
                    break;
                },
            }
        }

        // Clean up underlying stream if all Client handles were dropped without explicit shutdown.
        self.graceful_close().await;
        self.fail_remaining_queued();
    }

    async fn handle_execute(
        &mut self,
        command: String,
        options: CommandOptions,
        reply_tx: oneshot::Sender<Result<CommandOutput, Error>>,
    ) {
        // Caller dropped the execute() future while queued -> zero wire bytes sent (`NotSent`).
        if reply_tx.is_closed() {
            return;
        }

        let effective_timeout = options
            .timeout()
            .unwrap_or_else(|| self.timeouts.command_timeout());
        if effective_timeout.is_zero() {
            let _ = reply_tx.send(Err(Error::Config(TypeError::ZeroTimeout {
                field: "command_timeout",
            })));
            return;
        }

        let mut replays_used: u8 = 0;

        loop {
            if let Err(err) = self.ensure_healthy_connection().await {
                let _ = reply_tx.send(Err(err));
                return;
            }

            // Re-check caller cancellation right before pacing/socket dispatch (`NotSent`).
            if reply_tx.is_closed() {
                return;
            }

            self.apply_pacing_if_needed().await;
            if reply_tx.is_closed() {
                return;
            }

            let Some(conn) = self.conn.as_mut() else {
                let _ = reply_tx.send(Err(Error::ClientClosed));
                return;
            };

            // CRITICAL CANCELLATION INVARIANT:
            // `execute_command_with_timeout` runs exclusively under the actor's ownership and is
            // NEVER aborted when `reply_tx` closes. All response (and sentinel) frames are read
            // to completion within `effective_timeout` so TCP stream framing stays aligned.
            let exec_result = conn
                .execute_command_with_timeout(&command, effective_timeout)
                .await;
            self.last_command_completed = Some(Instant::now());

            match exec_result {
                Ok(output) => {
                    let _ = reply_tx.send(Ok(output));
                    return;
                },
                Err(transport_err) => {
                    let is_healthy = self.conn.as_ref().is_some_and(FramedConnection::is_healthy);

                    // Pre-write validation failures leave the stream completely untouched and healthy.
                    if transport_err.command_phase() == Some(CommandPhase::NotSent) && is_healthy {
                        let _ = reply_tx.send(Err(Error::from_transport(transport_err)));
                        return;
                    }

                    // Any post-write or stream-corrupting error immediately retires the socket.
                    self.conn = None;

                    if transport_err.is_auth_rejected() {
                        warn!("server rejected credentials during session; closing permanently");
                        let _ = self.state_tx.send(ConnectionState::Closed);
                        let _ = reply_tx.send(Err(Error::AuthRejected));
                        return;
                    }

                    if self.should_replay(options.idempotency(), replays_used) {
                        replays_used = replays_used.saturating_add(1);
                        debug!(
                            attempt = replays_used,
                            "replaying caller-marked idempotent command after unknown outcome"
                        );
                        continue;
                    }

                    // Conservative default: surface the exact `OutcomeUnknown` error immediately
                    // without replaying, then heal the session in the background if configured.
                    let _ = reply_tx.send(Err(Error::from_transport(transport_err)));
                    if !matches!(self.reconnect_policy, ReconnectPolicy::Disabled)
                        && self.reconnector.is_some()
                    {
                        let _ = self.ensure_healthy_connection().await;
                    } else {
                        let _ = self.state_tx.send(ConnectionState::Closed);
                    }
                    return;
                },
            }
        }
    }

    fn should_replay(&self, idempotency: Idempotency, replays_used: u8) -> bool {
        if self.reconnector.is_none() || matches!(self.reconnect_policy, ReconnectPolicy::Disabled)
        {
            return false;
        }
        match (self.replay_mode, idempotency) {
            (ReplayMode::ReplayIdempotentOnly { max_replays }, Idempotency::Idempotent) => {
                replays_used < max_replays
            },
            _ => false,
        }
    }

    async fn apply_pacing_if_needed(&mut self) {
        let Some(min_interval) = self.dialect.min_command_interval() else {
            return;
        };
        let Some(last) = self.last_command_completed else {
            return;
        };
        let elapsed = last.elapsed();
        if let Some(remaining) = min_interval.checked_sub(elapsed)
            && !remaining.is_zero()
        {
            tokio::time::sleep(remaining).await;
        }
    }

    async fn ensure_healthy_connection(&mut self) -> Result<(), Error> {
        if let Some(conn) = &self.conn
            && conn.is_healthy()
        {
            return Ok(());
        }
        self.conn = None;

        if self.state_tx.borrow().is_closed() {
            return Err(Error::ClientClosed);
        }

        let Some(reconnector) = self.reconnector.clone() else {
            let _ = self.state_tx.send(ConnectionState::Closed);
            return Err(Error::ClientClosed);
        };

        if matches!(self.reconnect_policy, ReconnectPolicy::Disabled) {
            let _ = self.state_tx.send(ConnectionState::Closed);
            return Err(Error::ClientClosed);
        }

        let mut attempt: u32 = 1;
        let mut last_err: Option<TransportError> = None;

        while let Some(delay) = self.reconnect_policy.delay_for_attempt(attempt) {
            let _ = self
                .state_tx
                .send(ConnectionState::Reconnecting { attempt });

            if !self.sleep_interruptible(delay).await {
                let _ = self.state_tx.send(ConnectionState::Closed);
                return Err(Error::ClientClosed);
            }

            match (reconnector)().await {
                Ok(new_conn) => {
                    let sid = new_conn.session_id();
                    self.conn = Some(new_conn);
                    self.last_command_completed = None;
                    let _ = self
                        .state_tx
                        .send(ConnectionState::Ready { session_id: sid });
                    debug!(%sid, attempt, "RCON session reconnected");
                    return Ok(());
                },
                Err(err) => {
                    if err.is_auth_rejected() {
                        let _ = self.state_tx.send(ConnectionState::Closed);
                        return Err(Error::AuthRejected);
                    }
                    last_err = Some(err);
                    attempt = attempt.saturating_add(1);
                },
            }
        }

        let _ = self.state_tx.send(ConnectionState::Closed);
        let attempts_made = attempt.saturating_sub(1);
        match last_err {
            Some(err) => Err(Error::ReconnectExhausted {
                attempts: attempts_made,
                last_error: Box::new(err),
            }),
            None => Err(Error::ClientClosed),
        }
    }

    async fn sleep_interruptible(&mut self, duration: Duration) -> bool {
        if *self.shutdown_rx.borrow() {
            return false;
        }
        tokio::select! {
            _ = tokio::time::sleep(duration) => !*self.shutdown_rx.borrow(),
            changed = self.shutdown_rx.changed() => {
                changed.is_ok() && !*self.shutdown_rx.borrow()
            }
        }
    }

    async fn graceful_close(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            let _ = conn.shutdown().await;
        }
        let _ = self.state_tx.send(ConnectionState::Closed);
    }

    fn fail_remaining_queued(&mut self) {
        self.rx.close();
        while let Ok(req) = self.rx.try_recv() {
            match req {
                SessionRequest::Execute { reply_tx, .. } => {
                    let _ = reply_tx.send(Err(Error::ClientClosed));
                },
                SessionRequest::Shutdown { ack_tx } => {
                    let _ = ack_tx.send(());
                },
            }
        }
    }
}
