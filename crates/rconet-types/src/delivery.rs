/// Precise lifecycle stage reached by a command request with respect to the wire.
///
/// Reconnection and retry logic inspect `CommandPhase` so that commands whose bytes may
/// have already reached the remote server are **never** silently re-sent.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandPhase {
    /// Request has not yet entered a socket write operation (for example, rejected by
    /// local validation, expired in queue, or failed while the session was already disconnected).
    NotSent,

    /// Command bytes were handed to the transport and written cleanly, prior to reading
    /// response frames.
    Sent,

    /// Command (and optional sentinel frame) flushed to the transport; awaiting complete
    /// correlated response frames.
    InFlight,

    /// All expected response frames arrived and were validated cleanly.
    Completed,

    /// A socket write failed after starting, a read encountered EOF/error, or a protocol
    /// deadline elapsed after transmission began.
    ///
    /// **Important:** A failed socket write does *not* prove zero bytes were transmitted;
    /// TCP segments may have already crossed the network before the error surfaced.
    /// Therefore any post-initiation failure transitions to `OutcomeUnknown`.
    OutcomeUnknown,
}

impl CommandPhase {
    /// Returns `true` if any command bytes may have been transmitted to the remote server.
    ///
    /// Only [`CommandPhase::NotSent`] returns `false`.
    #[must_use]
    pub const fn may_have_executed(self) -> bool {
        !matches!(self, Self::NotSent)
    }
}

/// Caller declaration describing whether duplicate execution of a command is tolerable.
///
/// RCON protocols do not carry server-side idempotency tokens. Declaring a command
/// [`Idempotency::Idempotent`] is an explicit caller-level assertion that executing the
/// command more than once (for example, a read-only `status` or `list` query) is acceptable
/// within the caller's application context *if* an explicit retry policy is also enabled.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Idempotency {
    /// Command may mutate server state (default). Never replayed once transmission starts.
    #[default]
    NonIdempotent,

    /// Caller asserts the command is side-effect-free and tolerates duplicate execution.
    Idempotent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_not_sent_guarantees_zero_remote_execution() {
        assert!(!CommandPhase::NotSent.may_have_executed());
        assert!(CommandPhase::Sent.may_have_executed());
        assert!(CommandPhase::InFlight.may_have_executed());
        assert!(CommandPhase::Completed.may_have_executed());
        assert!(CommandPhase::OutcomeUnknown.may_have_executed());
    }

    #[test]
    fn default_idempotency_is_conservative() {
        assert_eq!(Idempotency::default(), Idempotency::NonIdempotent);
    }
}
