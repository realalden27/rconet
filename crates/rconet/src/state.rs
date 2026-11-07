use std::time::Duration;

use rconet_types::{Idempotency, SessionId};

/// Observable lifecycle state of a [`crate::Client`] session.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Initial handshake in progress.
    Connecting,
    /// Authenticated and ready to dispatch commands on `session_id`.
    Ready {
        /// Identifier of the currently active authenticated transport generation.
        session_id: SessionId,
    },
    /// Transport dropped or became tainted; attempting backoff reconnection.
    Reconnecting {
        /// Current 1-based reconnection attempt number.
        attempt: u32,
    },
    /// Permanently shut down or closed after unrecoverable failure / exhausted retries.
    Closed,
}

impl ConnectionState {
    /// Returns `true` when an authenticated transport is currently active.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    /// Returns `true` when the session has permanently terminated.
    #[must_use]
    pub const fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }
}

/// Per-command execution options passed to [`crate::Client::execute_with`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CommandOptions {
    timeout: Option<Duration>,
    idempotency: Idempotency,
}

impl CommandOptions {
    /// Creates default conservative options (`NonIdempotent`, client-default timeout).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timeout: None,
            idempotency: Idempotency::NonIdempotent,
        }
    }

    /// Overrides the protocol command deadline for this specific execution.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the caller-declared [`Idempotency`] classification.
    #[must_use]
    pub const fn with_idempotency(mut self, idempotency: Idempotency) -> Self {
        self.idempotency = idempotency;
        self
    }

    /// Shorthand marking this command [`Idempotency::Idempotent`].
    #[must_use]
    pub const fn idempotent(self) -> Self {
        self.with_idempotency(Idempotency::Idempotent)
    }

    /// Configured per-command timeout override, if any.
    #[must_use]
    pub const fn timeout(self) -> Option<Duration> {
        self.timeout
    }

    /// Caller-declared [`Idempotency`].
    #[must_use]
    pub const fn idempotency(self) -> Idempotency {
        self.idempotency
    }
}
