use std::time::Duration;

use crate::error::TypeError;

/// Length of the outer 32-bit little-endian frame size field in bytes.
pub const WIRE_LENGTH_FIELD_BYTES: usize = 4;

/// Minimum legal value of the wire `length` header (`4B id + 4B type + 2B NUL` pad).
pub const WIRE_MIN_BODY_HEADER_BYTES: usize = 10;

/// Default maximum inbound single-frame body+header length (`16 KiB`).
///
/// Standard RCON servers emit up to `4096 + 10 = 4106` bytes per frame; `16 KiB`
/// accommodates slightly larger server frames while blocking memory-exhaustion headers.
pub const DEFAULT_MAX_INBOUND_FRAME_BYTES: usize = 16 * 1024;

/// Hard upper ceiling (`1 MiB`) allowed for any custom inbound single-frame limit.
pub const HARD_MAX_INBOUND_FRAME_BYTES: usize = 1024 * 1024;

/// Default maximum total accumulated response size across multi-packet fragments (`4 MiB`).
pub const DEFAULT_MAX_ACCUMULATED_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Hard upper ceiling (`64 MiB`) for accumulated multi-packet command responses.
pub const HARD_MAX_ACCUMULATED_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Standard Source RCON maximum outbound command payload length (`4096` bytes).
pub const SOURCE_MAX_COMMAND_BYTES: usize = 4096;

/// Vanilla Minecraft Java Edition RCON maximum outbound command payload length (`1446` bytes).
///
/// Derived from the server's fixed `1460`-byte socket read buffer minus `14` bytes of wire framing
/// (`4B length + 4B request_id + 4B type + 2B NUL` pad).
pub const MINECRAFT_JAVA_MAX_COMMAND_BYTES: usize = 1446;

/// Validated byte-size guardrails enforced by wire codecs and response assemblers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLimits {
    max_inbound_frame_bytes: usize,
    max_command_bytes: usize,
    max_accumulated_response_bytes: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_inbound_frame_bytes: DEFAULT_MAX_INBOUND_FRAME_BYTES,
            max_command_bytes: SOURCE_MAX_COMMAND_BYTES,
            max_accumulated_response_bytes: DEFAULT_MAX_ACCUMULATED_RESPONSE_BYTES,
        }
    }
}

impl FrameLimits {
    /// Constructs validated frame and payload limits.
    pub const fn new(
        max_inbound_frame_bytes: usize,
        max_command_bytes: usize,
        max_accumulated_response_bytes: usize,
    ) -> Result<Self, TypeError> {
        if max_inbound_frame_bytes < WIRE_MIN_BODY_HEADER_BYTES
            || max_inbound_frame_bytes > HARD_MAX_INBOUND_FRAME_BYTES
        {
            return Err(TypeError::InvalidFrameLimit {
                field: "max_inbound_frame_bytes",
                value: max_inbound_frame_bytes,
            });
        }
        if max_command_bytes == 0 || max_command_bytes > HARD_MAX_INBOUND_FRAME_BYTES {
            return Err(TypeError::InvalidFrameLimit {
                field: "max_command_bytes",
                value: max_command_bytes,
            });
        }
        if max_accumulated_response_bytes < max_inbound_frame_bytes
            || max_accumulated_response_bytes > HARD_MAX_ACCUMULATED_RESPONSE_BYTES
        {
            return Err(TypeError::InvalidFrameLimit {
                field: "max_accumulated_response_bytes",
                value: max_accumulated_response_bytes,
            });
        }
        Ok(Self {
            max_inbound_frame_bytes,
            max_command_bytes,
            max_accumulated_response_bytes,
        })
    }

    /// Maximum accepted value of the 32-bit wire `length` field before allocation.
    #[must_use]
    pub const fn max_inbound_frame_bytes(self) -> usize {
        self.max_inbound_frame_bytes
    }

    /// Maximum accepted byte length of an outbound command string prior to encoding.
    #[must_use]
    pub const fn max_command_bytes(self) -> usize {
        self.max_command_bytes
    }

    /// Maximum cumulative payload bytes assembled for a single multi-packet command.
    #[must_use]
    pub const fn max_accumulated_response_bytes(self) -> usize {
        self.max_accumulated_response_bytes
    }

    /// Returns a copy with an updated outbound command byte limit.
    pub const fn with_max_command_bytes(self, max_command_bytes: usize) -> Result<Self, TypeError> {
        Self::new(
            self.max_inbound_frame_bytes,
            max_command_bytes,
            self.max_accumulated_response_bytes,
        )
    }
}

/// Validated timeout configuration for connection, authentication, and command execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutConfig {
    connect_timeout: Duration,
    auth_timeout: Duration,
    command_timeout: Duration,
    frame_io_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            auth_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(10),
            frame_io_timeout: Duration::from_secs(5),
        }
    }
}

impl TimeoutConfig {
    /// Constructs validated non-zero timeouts.
    pub const fn new(
        connect_timeout: Duration,
        auth_timeout: Duration,
        command_timeout: Duration,
        frame_io_timeout: Duration,
    ) -> Result<Self, TypeError> {
        if connect_timeout.is_zero() {
            return Err(TypeError::ZeroTimeout {
                field: "connect_timeout",
            });
        }
        if auth_timeout.is_zero() {
            return Err(TypeError::ZeroTimeout {
                field: "auth_timeout",
            });
        }
        if command_timeout.is_zero() {
            return Err(TypeError::ZeroTimeout {
                field: "command_timeout",
            });
        }
        if frame_io_timeout.is_zero() {
            return Err(TypeError::ZeroTimeout {
                field: "frame_io_timeout",
            });
        }
        Ok(Self {
            connect_timeout,
            auth_timeout,
            command_timeout,
            frame_io_timeout,
        })
    }

    /// Maximum duration allowed for TCP/WebSocket transport establishment.
    #[must_use]
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Maximum duration allowed for the RCON authentication handshake.
    #[must_use]
    pub const fn auth_timeout(self) -> Duration {
        self.auth_timeout
    }

    /// End-to-end protocol deadline for completing an in-flight command.
    #[must_use]
    pub const fn command_timeout(self) -> Duration {
        self.command_timeout
    }

    /// Per-frame socket read/write stall deadline.
    #[must_use]
    pub const fn frame_io_timeout(self) -> Duration {
        self.frame_io_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_limits_reject_underflow_and_excessive_values() {
        assert!(FrameLimits::new(9, 100, 1024).is_err());
        assert!(FrameLimits::new(HARD_MAX_INBOUND_FRAME_BYTES + 1, 100, 2_000_000).is_err());
        assert!(FrameLimits::new(1024, 0, 2048).is_err());
        assert!(FrameLimits::new(4096, 1024, 2048).is_err());
        assert!(
            FrameLimits::default()
                .with_max_command_bytes(MINECRAFT_JAVA_MAX_COMMAND_BYTES)
                .is_ok()
        );
    }

    #[test]
    fn timeout_config_rejects_zero_durations() {
        let zero = Duration::ZERO;
        let one = Duration::from_secs(1);
        assert!(TimeoutConfig::new(zero, one, one, one).is_err());
        assert!(TimeoutConfig::new(one, zero, one, one).is_err());
        assert!(TimeoutConfig::new(one, one, zero, one).is_err());
        assert!(TimeoutConfig::new(one, one, one, zero).is_err());
        assert!(TimeoutConfig::new(one, one, one, one).is_ok());
    }
}
