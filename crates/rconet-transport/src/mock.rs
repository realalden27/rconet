//! Deterministic in-process RCON server harness for unit and integration testing.
//!
//! Binds strictly to loopback (`127.0.0.1:0`) and supports configurable authentication
//! modes, multi-packet responses, classic SRCDS sentinel trailers, 1-byte TCP trickle
//! writes, mid-flight disconnects, artificial delays, and raw malformed wire injection.

use std::collections::HashMap;
#[cfg(feature = "tokio-tcp")]
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rconet_proto::{FrameCodec, PacketKind, RawFrame};
use rconet_types::{FrameLimits, PacketId};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(feature = "tokio-tcp")]
use tokio::net::TcpListener;
use tokio::sync::watch;
#[cfg(feature = "tokio-tcp")]
use tokio::task::JoinHandle;

/// Trailer payload emitted by classic Valve SRCDS after an empty sentinel echo.
const SRCDS_TRAILER_BYTES: &[u8] = &[0x00, 0x01, 0x00, 0x00];

/// Authentication handshake style simulated by [`MockRconServer`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MockAuthMode {
    /// Emits an empty `RESPONSE_VALUE` (`0`) frame prior to `AUTH_RESPONSE` (`2`), matching SRCDS.
    #[default]
    SourceWithPreliminaryEmpty,
    /// Emits `AUTH_RESPONSE` (`2`) directly without a preceding `RESPONSE_VALUE` frame.
    DirectAuthResponse,
    /// Closes the TCP connection immediately without replying when the password is wrong.
    CloseImmediatelyOnFailure,
}

/// How [`MockRconServer`] responds to client sentinel (`RESPONSE_VALUE`, type `0`) requests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MockSentinelMode {
    /// Emits an empty `RESPONSE_VALUE` frame followed by the 4-byte `00 01 00 00` SRCDS trailer.
    #[default]
    SourceDualPacket,
    /// Emits a single `RESPONSE_VALUE` frame (`"Unknown request 0"`) with no second trailer.
    SinglePacketWarning,
    /// Drops `RESPONSE_VALUE` requests silently without replying.
    IgnoreSilent,
}

/// Scripted server behavior triggered when a specific command string is executed.
#[derive(Debug, Clone)]
pub enum MockReply {
    /// Replies with a single `RESPONSE_VALUE` packet carrying UTF-8 text.
    Single(String),
    /// Splits the reply across multiple sequential `RESPONSE_VALUE` packets.
    MultiPacket(Vec<String>),
    /// Replies with raw arbitrary payload bytes (for example, non-UTF-8 sequences).
    RawPayload(Bytes),
    /// Sleeps for `delay` before sending a single reply (for timeout and cancellation testing).
    Delayed {
        /// Artificial server processing pause.
        delay: Duration,
        /// Response text sent once `delay` elapses.
        body: String,
    },
    /// Accepts the command (recording it as executed) and immediately drops the socket.
    DropConnectionMidFlight,
    /// Drops the connection on the first execution of this command, then replies normally on subsequent executions.
    DropFirstThenSingle(String),
    /// Writes arbitrary raw bytes onto the socket (for malformed-frame and desync testing).
    InjectRawWire(Bytes),
}

/// Configuration for [`MockRconServer`].
#[derive(Debug, Clone)]
pub struct MockServerConfig {
    /// Expected authentication password.
    pub password: String,
    /// Authentication handshake style.
    pub auth_mode: MockAuthMode,
    /// Sentinel echo style.
    pub sentinel_mode: MockSentinelMode,
    /// Optional byte chunk size used to fragment all server writes (e.g. `Some(1)` for 1-byte trickle).
    pub write_chunk_size: Option<usize>,
    /// Scripted replies keyed by exact command string. Unmatched commands echo `"echo: <cmd>"`.
    pub routes: HashMap<String, MockReply>,
}

impl Default for MockServerConfig {
    fn default() -> Self {
        Self {
            password: "test-password".to_string(),
            auth_mode: MockAuthMode::default(),
            sentinel_mode: MockSentinelMode::default(),
            write_chunk_size: None,
            routes: HashMap::new(),
        }
    }
}

impl MockServerConfig {
    /// Creates a config requiring `password`.
    #[must_use]
    pub fn with_password(password: impl Into<String>) -> Self {
        Self {
            password: password.into(),
            ..Self::default()
        }
    }

    /// Registers a scripted [`MockReply`] for `command`.
    #[must_use]
    pub fn route(mut self, command: impl Into<String>, reply: MockReply) -> Self {
        self.routes.insert(command.into(), reply);
        self
    }

    /// Sets the authentication handshake mode.
    #[must_use]
    pub const fn auth_mode(mut self, mode: MockAuthMode) -> Self {
        self.auth_mode = mode;
        self
    }

    /// Sets the sentinel response mode.
    #[must_use]
    pub const fn sentinel_mode(mut self, mode: MockSentinelMode) -> Self {
        self.sentinel_mode = mode;
        self
    }

    /// Fragments every outbound server write into chunks of `size` bytes.
    #[must_use]
    pub const fn write_chunk_size(mut self, size: usize) -> Self {
        self.write_chunk_size = Some(size);
        self
    }
}

#[derive(Debug, Default)]
struct RecordedState {
    executed_commands: Vec<String>,
    #[cfg(feature = "tokio-tcp")]
    connections_accepted: usize,
}

/// Deterministic loopback RCON server recording executed commands for test assertions.
#[cfg(feature = "tokio-tcp")]
#[derive(Debug)]
pub struct MockRconServer {
    local_addr: SocketAddr,
    state: Arc<Mutex<RecordedState>>,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
}

#[cfg(feature = "tokio-tcp")]
impl MockRconServer {
    /// Binds to `127.0.0.1:0` and spawns the background accept loop.
    pub async fn start(config: MockServerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let state = Arc::new(Mutex::new(RecordedState::default()));
        let shared_cfg = Arc::new(config);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let loop_state = Arc::clone(&state);
        let accept_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((stream, _peer)) = accepted else {
                            break;
                        };
                        let _ = stream.set_nodelay(true);
                        {
                            let mut guard = loop_state.lock().unwrap_or_else(PoisonError::into_inner);
                            guard.connections_accepted = guard.connections_accepted.saturating_add(1);
                        }
                        let conn_cfg = Arc::clone(&shared_cfg);
                        let conn_state = Arc::clone(&loop_state);
                        let conn_shutdown = shutdown_rx.clone();
                        tokio::spawn(async move {
                            serve_stream(stream, &conn_cfg, &conn_state, conn_shutdown).await;
                        });
                    }
                }
            }
        });

        Ok(Self {
            local_addr,
            state,
            shutdown_tx,
            accept_task,
        })
    }

    /// Ephemeral loopback address (`127.0.0.1:<port>`) where the server is listening.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Snapshot of all commands received and executed across connections in arrival order.
    #[must_use]
    pub fn executed_commands(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .executed_commands
            .clone()
    }

    /// Total number of TCP connections accepted since server startup.
    #[must_use]
    pub fn connections_accepted(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connections_accepted
    }

    /// Stops accepting new connections and signals active handlers to exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
    }
}

/// Serves a single arbitrary `AsyncRead + AsyncWrite` stream using [`MockServerConfig`].
pub async fn serve_duplex_stream<S>(stream: S, config: MockServerConfig) -> Vec<String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let state = Arc::new(Mutex::new(RecordedState::default()));
    let (_tx, rx) = watch::channel(false);
    serve_stream(stream, &config, &state, rx).await;
    let guard = state.lock().unwrap_or_else(PoisonError::into_inner);
    guard.executed_commands.clone()
}

async fn serve_stream<S>(
    mut stream: S,
    config: &MockServerConfig,
    state: &Arc<Mutex<RecordedState>>,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let codec = FrameCodec::new(FrameLimits::source_default());
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut authenticated = false;

    loop {
        let frame = match read_next_client_frame(
            &mut stream,
            &codec,
            &mut read_buf,
            &mut shutdown_rx,
        )
        .await
        {
            Some(frame) => frame,
            None => return,
        };

        match frame.kind {
            PacketKind::Auth => {
                let ok = frame.payload.as_ref() == config.password.as_bytes();
                if !handle_auth_frame(&mut stream, &codec, config, frame.id, ok).await {
                    return;
                }
                authenticated = ok;
            },
            PacketKind::ExecOrAuthResponse => {
                if !authenticated {
                    let _ = send_frames(
                        &mut stream,
                        &codec,
                        &[RawFrame::auth_response(PacketId::AUTH_FAILURE)],
                        config.write_chunk_size,
                    )
                    .await;
                    return;
                }
                let cmd_text = String::from_utf8_lossy(&frame.payload).into_owned();
                let occurrence = {
                    let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);
                    guard.executed_commands.push(cmd_text.clone());
                    guard
                        .executed_commands
                        .iter()
                        .filter(|seen| *seen == &cmd_text)
                        .count()
                };
                if !handle_exec_frame(&mut stream, &codec, config, frame.id, &cmd_text, occurrence)
                    .await
                {
                    return;
                }
            },
            PacketKind::ResponseValue => {
                let sent = handle_sentinel_frame(&mut stream, &codec, config, frame.id).await;
                if !sent {
                    return;
                }
            },
            _ => {},
        }
    }
}

async fn read_next_client_frame<S>(
    stream: &mut S,
    codec: &FrameCodec,
    read_buf: &mut BytesMut,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<RawFrame>
where
    S: AsyncRead + Unpin,
{
    loop {
        match codec.decode(read_buf) {
            Ok(Some(frame)) => return Some(frame),
            Ok(None) => {},
            Err(_) => return None,
        }

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return None;
                }
            }
            read_res = stream.read_buf(read_buf) => {
                match read_res {
                    Ok(0) | Err(_) => return None,
                    Ok(_) => {}
                }
            }
        }
    }
}

async fn handle_auth_frame<S>(
    stream: &mut S,
    codec: &FrameCodec,
    config: &MockServerConfig,
    req_id: PacketId,
    password_ok: bool,
) -> bool
where
    S: AsyncWrite + Unpin,
{
    if !password_ok && config.auth_mode == MockAuthMode::CloseImmediatelyOnFailure {
        return false;
    }

    let reply_id = if password_ok {
        req_id
    } else {
        PacketId::AUTH_FAILURE
    };

    let frames = match config.auth_mode {
        MockAuthMode::SourceWithPreliminaryEmpty => vec![
            RawFrame::response_value(req_id, &[][..]),
            RawFrame::auth_response(reply_id),
        ],
        MockAuthMode::DirectAuthResponse | MockAuthMode::CloseImmediatelyOnFailure => {
            vec![RawFrame::auth_response(reply_id)]
        },
    };

    send_frames(stream, codec, &frames, config.write_chunk_size).await
}

async fn handle_exec_frame<S>(
    stream: &mut S,
    codec: &FrameCodec,
    config: &MockServerConfig,
    req_id: PacketId,
    cmd_text: &str,
    occurrence: usize,
) -> bool
where
    S: AsyncWrite + Unpin,
{
    let reply = config
        .routes
        .get(cmd_text)
        .cloned()
        .unwrap_or_else(|| MockReply::Single(format!("echo: {cmd_text}")));

    match reply {
        MockReply::Single(body) => {
            send_frames(
                stream,
                codec,
                &[RawFrame::response_value(req_id, body)],
                config.write_chunk_size,
            )
            .await
        },
        MockReply::MultiPacket(chunks) => {
            let frames: Vec<RawFrame> = chunks
                .into_iter()
                .map(|part| RawFrame::response_value(req_id, part))
                .collect();
            send_frames(stream, codec, &frames, config.write_chunk_size).await
        },
        MockReply::RawPayload(raw) => {
            send_frames(
                stream,
                codec,
                &[RawFrame::response_value(req_id, raw)],
                config.write_chunk_size,
            )
            .await
        },
        MockReply::Delayed { delay, body } => {
            tokio::time::sleep(delay).await;
            send_frames(
                stream,
                codec,
                &[RawFrame::response_value(req_id, body)],
                config.write_chunk_size,
            )
            .await
        },
        MockReply::DropConnectionMidFlight => false,
        MockReply::DropFirstThenSingle(body) => {
            if occurrence <= 1 {
                false
            } else {
                send_frames(
                    stream,
                    codec,
                    &[RawFrame::response_value(req_id, body)],
                    config.write_chunk_size,
                )
                .await
            }
        },
        MockReply::InjectRawWire(raw_wire) => {
            write_wire_bytes(stream, &raw_wire, config.write_chunk_size).await
        },
    }
}

async fn handle_sentinel_frame<S>(
    stream: &mut S,
    codec: &FrameCodec,
    config: &MockServerConfig,
    sentinel_id: PacketId,
) -> bool
where
    S: AsyncWrite + Unpin,
{
    match config.sentinel_mode {
        MockSentinelMode::SourceDualPacket => {
            let frames = [
                RawFrame::response_value(sentinel_id, &[][..]),
                RawFrame::response_value(sentinel_id, SRCDS_TRAILER_BYTES),
            ];
            send_frames(stream, codec, &frames, config.write_chunk_size).await
        },
        MockSentinelMode::SinglePacketWarning => {
            let frames = [RawFrame::response_value(sentinel_id, "Unknown request 0")];
            send_frames(stream, codec, &frames, config.write_chunk_size).await
        },
        MockSentinelMode::IgnoreSilent => true,
    }
}

async fn send_frames<S>(
    stream: &mut S,
    codec: &FrameCodec,
    frames: &[RawFrame],
    chunk_size: Option<usize>,
) -> bool
where
    S: AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    for frame in frames {
        if codec.encode(frame, &mut buf).is_err() {
            return false;
        }
    }
    write_wire_bytes(stream, &buf, chunk_size).await
}

async fn write_wire_bytes<S>(stream: &mut S, wire: &[u8], chunk_size: Option<usize>) -> bool
where
    S: AsyncWrite + Unpin,
{
    match chunk_size {
        Some(size) if size > 0 => {
            for slice in wire.chunks(size) {
                if stream.write_all(slice).await.is_err() || stream.flush().await.is_err() {
                    return false;
                }
                tokio::task::yield_now().await;
            }
            true
        },
        _ => stream.write_all(wire).await.is_ok() && stream.flush().await.is_ok(),
    }
}
