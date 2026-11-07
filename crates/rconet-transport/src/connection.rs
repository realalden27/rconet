#[cfg(feature = "tokio-tcp")]
use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use rconet_proto::{
    AuthStep, CommandExchange, CommandOutput, CommandStep, DialectProfile, ProtocolState, RawFrame,
};
use rconet_types::{
    CommandPhase, MAX_TIMEOUT_DURATION, PacketId, SecretPassword, SessionId, TimeoutConfig,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(feature = "tokio-tcp")]
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::{Instant, timeout};
use tracing::instrument;

use crate::error::{CommandFailureKind, TransportError};

/// Initial capacity reserved for the inbound frame read buffer (`8 KiB`).
const INITIAL_READ_BUF_CAPACITY: usize = 8 * 1024;

/// Framed asynchronous RCON connection over any `AsyncRead + AsyncWrite` byte stream.
///
/// Tracks stream integrity explicitly: if any post-write timeout, EOF, I/O failure, or
/// protocol violation occurs, the connection marks itself **unusable** (`!is_healthy()`)
/// so callers can never read misaligned frames on subsequent commands.
#[derive(Debug)]
pub struct FramedConnection<S> {
    session_id: SessionId,
    stream: S,
    proto: ProtocolState,
    timeouts: TimeoutConfig,
    read_buf: BytesMut,
    healthy: bool,
}

#[cfg(feature = "tokio-tcp")]
impl FramedConnection<TcpStream> {
    /// Connects to a TCP RCON endpoint, enables `TCP_NODELAY`, and completes authentication.
    #[instrument(
        name = "rconet.tcp_connect",
        skip_all,
        fields(dialect = ?dialect.termination())
    )]
    pub async fn connect_tcp<A: ToSocketAddrs>(
        addr: A,
        password: &SecretPassword,
        dialect: DialectProfile,
        timeouts: TimeoutConfig,
    ) -> Result<Self, TransportError> {
        let tcp = timeout(timeouts.connect_timeout(), TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::ConnectTimeout {
                timeout: timeouts.connect_timeout(),
            })?
            .map_err(TransportError::ConnectIo)?;

        // Disable Nagle's algorithm so small RCON command + sentinel frames flush immediately.
        let _ = tcp.set_nodelay(true);

        Self::handshake(tcp, password, dialect, timeouts).await
    }

    /// Returns the remote peer [`SocketAddr`] of an active TCP connection.
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.stream.peer_addr()
    }
}

impl<S> FramedConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Performs the RCON authentication handshake over an already-established byte stream.
    #[instrument(
        name = "rconet.handshake",
        skip_all,
        fields(session_id = %SessionId::next())
    )]
    pub async fn handshake(
        stream: S,
        password: &SecretPassword,
        dialect: DialectProfile,
        timeouts: TimeoutConfig,
    ) -> Result<Self, TransportError> {
        let mut conn = Self::new_unauthenticated(stream, dialect, timeouts);
        conn.authenticate(password).await?;
        Ok(conn)
    }

    /// Wraps `stream` without performing authentication (useful for low-level testing).
    #[must_use]
    pub fn new_unauthenticated(
        stream: S,
        dialect: DialectProfile,
        timeouts: TimeoutConfig,
    ) -> Self {
        Self {
            session_id: SessionId::next(),
            stream,
            proto: ProtocolState::new(dialect),
            timeouts,
            read_buf: BytesMut::with_capacity(INITIAL_READ_BUF_CAPACITY),
            healthy: true,
        }
    }

    /// Unique local [`SessionId`] assigned to this connection instance.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns `true` if no framing, timeout, or I/O error has tainted this stream.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.healthy
    }

    /// Returns the active [`DialectProfile`].
    #[must_use]
    pub const fn dialect(&self) -> DialectProfile {
        self.proto.dialect()
    }

    /// Returns the configured [`TimeoutConfig`].
    #[must_use]
    pub const fn timeouts(&self) -> TimeoutConfig {
        self.timeouts
    }

    /// Executes the RCON authentication handshake bounded by `timeouts.auth_timeout()`.
    #[instrument(name = "rconet.authenticate", skip_all, fields(session_id = %self.session_id))]
    pub async fn authenticate(&mut self, password: &SecretPassword) -> Result<(), TransportError> {
        let (mut exchange, wire) = self.proto.begin_auth(password).map_err(|err| {
            self.healthy = false;
            TransportError::AuthProtocol(err)
        })?;

        let auth_deadline = bounded_deadline(self.timeouts.auth_timeout());

        if let Err(err) = self.write_all_bounded(&wire, auth_deadline).await {
            self.healthy = false;
            return Err(match err {
                CommandFailureKind::DeadlineExceeded { .. }
                | CommandFailureKind::WriteTimeout { .. } => TransportError::AuthTimeout {
                    timeout: self.timeouts.auth_timeout(),
                },
                CommandFailureKind::WriteIo(io_err) => TransportError::ConnectIo(io_err),
                _ => TransportError::AuthClosedByPeer,
            });
        }

        loop {
            let frame = match self.read_frame_bounded(auth_deadline).await {
                Ok(frame) => frame,
                Err(CommandFailureKind::DeadlineExceeded { .. })
                | Err(CommandFailureKind::FrameTimeout { .. }) => {
                    self.healthy = false;
                    return Err(TransportError::AuthTimeout {
                        timeout: self.timeouts.auth_timeout(),
                    });
                },
                Err(CommandFailureKind::UnexpectedEof) => {
                    self.healthy = false;
                    return Err(TransportError::AuthClosedByPeer);
                },
                Err(CommandFailureKind::ReadIo(io_err)) => {
                    self.healthy = false;
                    return Err(TransportError::ConnectIo(io_err));
                },
                Err(CommandFailureKind::Protocol(proto_err)) => {
                    self.healthy = false;
                    return Err(TransportError::AuthProtocol(proto_err));
                },
                Err(_) => {
                    self.healthy = false;
                    return Err(TransportError::AuthClosedByPeer);
                },
            };

            match exchange.on_frame(frame) {
                Ok(AuthStep::NeedMoreFrames) => {},
                Ok(AuthStep::Authenticated) => return Ok(()),
                Ok(_) => {},
                Err(proto_err) => {
                    self.healthy = false;
                    return Err(TransportError::AuthProtocol(proto_err));
                },
            }
        }
    }

    /// Executes a single RCON command using the configured `command_timeout`.
    pub async fn execute_command(
        &mut self,
        command: &str,
    ) -> Result<CommandOutput, TransportError> {
        self.execute_command_with_timeout(command, self.timeouts.command_timeout())
            .await
    }

    /// Executes a single RCON command bounded by `command_timeout`.
    ///
    /// - Pre-write validation errors leave the stream healthy and report [`CommandPhase::NotSent`].
    /// - Any failure once socket writing begins marks the stream unhealthy (`!is_healthy()`)
    ///   and reports [`CommandPhase::OutcomeUnknown`].
    #[instrument(
        name = "rconet.execute_command",
        skip_all,
        fields(session_id = %self.session_id, cmd_bytes = command.len())
    )]
    pub async fn execute_command_with_timeout(
        &mut self,
        command: &str,
        command_timeout: Duration,
    ) -> Result<CommandOutput, TransportError> {
        if !self.healthy {
            return Err(TransportError::Command {
                phase: CommandPhase::NotSent,
                kind: CommandFailureKind::SessionTainted,
            });
        }

        let (mut exchange, wire) =
            self.proto
                .begin_command(command)
                .map_err(|err| TransportError::Command {
                    phase: CommandPhase::NotSent,
                    kind: CommandFailureKind::Validation(err),
                })?;

        let deadline = bounded_deadline(command_timeout);

        if let Err(kind) = self.write_all_bounded(&wire, deadline).await {
            self.healthy = false;
            let mapped_kind = remap_deadline_kind(kind, command_timeout);
            return Err(TransportError::Command {
                phase: CommandPhase::OutcomeUnknown,
                kind: mapped_kind,
            });
        }

        self.drive_command_response(&mut exchange, deadline, command_timeout)
            .await
    }

    /// Gracefully flushes and shuts down the write half of the underlying byte stream.
    pub async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.healthy = false;
        timeout(self.timeouts.frame_io_timeout(), self.stream.shutdown())
            .await
            .map_err(|_| TransportError::ConnectTimeout {
                timeout: self.timeouts.frame_io_timeout(),
            })?
            .map_err(TransportError::ConnectIo)
    }

    /// Monitors an idle stream for remote EOF (`FIN`), `RST`, or unexpected unsolicited frames.
    ///
    /// Cancel-safe for use inside `tokio::select!`. Absorbs delayed 18-byte SRCDS sentinel
    /// trailers transparently and marks `self.healthy = false` upon disconnect or unexpected
    /// wire data before returning.
    pub async fn monitor_idle(&mut self) {
        if !self.healthy {
            return;
        }

        loop {
            self.drain_buffered_trailer();
            match self.proto.codec().decode(&mut self.read_buf) {
                Ok(Some(frame)) => {
                    if !self.proto.try_absorb_immediate_trailer(&frame) {
                        self.healthy = false;
                        return;
                    }
                },
                Ok(None) => {
                    if !self.read_idle_chunk().await {
                        self.healthy = false;
                        return;
                    }
                },
                Err(_) => {
                    self.healthy = false;
                    return;
                },
            }
        }
    }

    async fn read_idle_chunk(&mut self) -> bool {
        if self.read_buf.is_empty() {
            matches!(self.stream.read_buf(&mut self.read_buf).await, Ok(n) if n > 0)
        } else {
            matches!(
                timeout(
                    self.timeouts.frame_io_timeout(),
                    self.stream.read_buf(&mut self.read_buf),
                )
                .await,
                Ok(Ok(n)) if n > 0
            )
        }
    }

    async fn drive_command_response(
        &mut self,
        exchange: &mut CommandExchange,
        deadline: Instant,
        command_timeout: Duration,
    ) -> Result<CommandOutput, TransportError> {
        let mut active_coalesce_window: Option<Duration> = None;

        loop {
            let next_frame = match active_coalesce_window {
                None => self.read_frame_bounded(deadline).await.map(Some),
                Some(idle_window) => self.read_next_coalesced_frame(deadline, idle_window).await,
            };

            let frame = match next_frame {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    return self.proto.finish_coalesced(exchange).map_err(|proto_err| {
                        self.healthy = false;
                        TransportError::Command {
                            phase: CommandPhase::OutcomeUnknown,
                            kind: CommandFailureKind::Protocol(proto_err),
                        }
                    });
                },
                Err(kind) => {
                    self.healthy = false;
                    return Err(TransportError::Command {
                        phase: CommandPhase::OutcomeUnknown,
                        kind: remap_deadline_kind(kind, command_timeout),
                    });
                },
            };

            match self.proto.advance_command(exchange, frame) {
                Ok(CommandStep::NeedMoreFrames) => {
                    active_coalesce_window = None;
                },
                Ok(CommandStep::Coalescing { idle_window }) => {
                    active_coalesce_window = Some(idle_window);
                },
                Ok(CommandStep::Completed(output)) => {
                    self.drain_buffered_trailer();
                    return Ok(output);
                },
                Ok(_) => {},
                Err(proto_err) => {
                    self.healthy = false;
                    return Err(TransportError::Command {
                        phase: CommandPhase::OutcomeUnknown,
                        kind: CommandFailureKind::Protocol(proto_err),
                    });
                },
            }
        }
    }

    async fn read_next_coalesced_frame(
        &mut self,
        overall_deadline: Instant,
        idle_window: Duration,
    ) -> Result<Option<RawFrame>, CommandFailureKind> {
        match self.proto.codec().decode(&mut self.read_buf) {
            Ok(Some(frame)) => return Ok(Some(frame)),
            Ok(None) => {},
            Err(proto_err) => return Err(CommandFailureKind::Protocol(proto_err)),
        }

        if self.read_buf.is_empty() {
            let Some(remaining) = overall_deadline.checked_duration_since(Instant::now()) else {
                return Err(CommandFailureKind::DeadlineExceeded {
                    timeout: Duration::ZERO,
                });
            };
            if idle_window.is_zero() {
                return Ok(None);
            }
            let wait_for_window = idle_window <= remaining;
            let wait_dur = remaining.min(idle_window);

            match timeout(wait_dur, self.stream.read_buf(&mut self.read_buf)).await {
                Ok(Ok(0)) => return Err(CommandFailureKind::UnexpectedEof),
                Ok(Ok(_n)) => {},
                Ok(Err(io_err)) => return Err(CommandFailureKind::ReadIo(io_err)),
                Err(_) => {
                    return if wait_for_window {
                        Ok(None)
                    } else {
                        Err(CommandFailureKind::DeadlineExceeded { timeout: remaining })
                    };
                },
            }
        }

        self.read_frame_bounded(overall_deadline).await.map(Some)
    }

    /// If classic SRCDS already delivered its 18-byte second sentinel packet inside the same
    /// TCP segment as the primary sentinel packet, consume it in `O(1)` without cloning `read_buf`.
    fn drain_buffered_trailer(&mut self) {
        const TRAILER_WIRE_LEN: usize = 18;
        let Some(wire) = self.read_buf.get(..TRAILER_WIRE_LEN) else {
            return;
        };
        if wire.get(..4) != Some(&14i32.to_le_bytes()[..])
            || wire.get(8..18) != Some(&[0, 0, 0, 0, 0, 1, 0, 0, 0, 0][..])
        {
            return;
        }
        let Some(id_slice) = wire.get(4..8) else {
            return;
        };
        let Ok(id_bytes) = <[u8; 4]>::try_from(id_slice) else {
            return;
        };
        let candidate = RawFrame::response_value(
            PacketId::from_raw(i32::from_le_bytes(id_bytes)),
            Bytes::from_static(&[0x00, 0x01, 0x00, 0x00]),
        );
        if self.proto.try_absorb_immediate_trailer(&candidate) {
            self.read_buf.advance(TRAILER_WIRE_LEN);
        }
    }

    async fn write_all_bounded(
        &mut self,
        bytes: &[u8],
        overall_deadline: Instant,
    ) -> Result<(), CommandFailureKind> {
        let Some(remaining) = overall_deadline.checked_duration_since(Instant::now()) else {
            return Err(CommandFailureKind::DeadlineExceeded {
                timeout: Duration::ZERO,
            });
        };
        let frame_timeout = self.timeouts.frame_io_timeout();
        let effective = remaining.min(frame_timeout);

        match timeout(effective, async {
            self.stream.write_all(bytes).await?;
            self.stream.flush().await
        })
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(io_err)) => Err(CommandFailureKind::WriteIo(io_err)),
            Err(_) => {
                if remaining <= frame_timeout {
                    Err(CommandFailureKind::DeadlineExceeded { timeout: remaining })
                } else {
                    Err(CommandFailureKind::WriteTimeout {
                        timeout: frame_timeout,
                    })
                }
            },
        }
    }

    async fn read_frame_bounded(
        &mut self,
        overall_deadline: Instant,
    ) -> Result<RawFrame, CommandFailureKind> {
        let codec = self.proto.codec();

        loop {
            match codec.decode(&mut self.read_buf) {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => {},
                Err(proto_err) => return Err(CommandFailureKind::Protocol(proto_err)),
            }

            let Some(remaining) = overall_deadline.checked_duration_since(Instant::now()) else {
                return Err(CommandFailureKind::DeadlineExceeded {
                    timeout: Duration::ZERO,
                });
            };
            let frame_timeout = self.timeouts.frame_io_timeout();
            let effective = remaining.min(frame_timeout);

            match timeout(effective, self.stream.read_buf(&mut self.read_buf)).await {
                Ok(Ok(0)) => return Err(CommandFailureKind::UnexpectedEof),
                Ok(Ok(_n)) => {},
                Ok(Err(io_err)) => return Err(CommandFailureKind::ReadIo(io_err)),
                Err(_) => {
                    return if remaining <= frame_timeout {
                        Err(CommandFailureKind::DeadlineExceeded { timeout: remaining })
                    } else {
                        Err(CommandFailureKind::FrameTimeout {
                            timeout: frame_timeout,
                        })
                    };
                },
            }
        }
    }
}

fn bounded_deadline(duration: Duration) -> Instant {
    let clamped = duration.min(MAX_TIMEOUT_DURATION);
    let now = Instant::now();
    now.checked_add(clamped)
        .or_else(|| now.checked_add(Duration::from_secs(3_600)))
        .unwrap_or(now)
}

fn remap_deadline_kind(
    kind: CommandFailureKind,
    configured_command_timeout: Duration,
) -> CommandFailureKind {
    match kind {
        CommandFailureKind::DeadlineExceeded { .. } => CommandFailureKind::DeadlineExceeded {
            timeout: configured_command_timeout,
        },
        other => other,
    }
}
