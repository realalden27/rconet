use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::TypeError;

/// Signed 32-bit packet identifier carried in binary RCON headers.
///
/// Client-originated requests always use strictly positive values (`>= 1`).
/// Servers emit `-1` (`PacketId::AUTH_FAILURE`) when authentication is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PacketId(i32);

impl PacketId {
    /// Sentinel value (`-1`) returned by RCON servers on authentication failure.
    pub const AUTH_FAILURE: Self = Self(-1);

    /// Lowest valid client-allocated request identifier (`1`).
    pub const MIN_POSITIVE: Self = Self(1);

    /// Highest client-allocated identifier before wrapping back to `1`.
    ///
    /// Leaves headroom below `i32::MAX` so paired sentinel identifiers never overflow.
    pub const MAX_ALLOCATABLE: Self = Self(i32::MAX - 2);

    /// Wraps any wire-decoded signed 32-bit integer.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        Self(raw)
    }

    /// Validates that `raw` is strictly positive (`>= 1`).
    pub const fn try_positive(raw: i32) -> Result<Self, TypeError> {
        if raw >= 1 {
            Ok(Self(raw))
        } else {
            Err(TypeError::NonPositivePacketId(raw))
        }
    }

    /// Returns the raw signed 32-bit wire representation.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0
    }

    /// Returns `true` when equal to `-1` (`AUTH_FAILURE`).
    #[must_use]
    pub const fn is_auth_failure(self) -> bool {
        self.0 == -1
    }

    /// Returns `true` when strictly greater than zero.
    #[must_use]
    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }
}

impl fmt::Display for PacketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Deterministic, overflow-safe allocator for positive [`PacketId`] values.
#[derive(Debug, Clone)]
pub struct PacketIdAllocator {
    next: i32,
}

impl Default for PacketIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketIdAllocator {
    /// Creates an allocator starting at `PacketId::MIN_POSITIVE` (`1`).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: PacketId::MIN_POSITIVE.as_i32(),
        }
    }

    /// Creates an allocator starting at a caller-chosen positive seed (useful for tests).
    pub const fn with_seed(seed: PacketId) -> Result<Self, TypeError> {
        if seed.as_i32() >= 1 && seed.as_i32() <= PacketId::MAX_ALLOCATABLE.as_i32() {
            Ok(Self {
                next: seed.as_i32(),
            })
        } else {
            Err(TypeError::NonPositivePacketId(seed.as_i32()))
        }
    }

    /// Allocates a single strictly positive [`PacketId`], wrapping cleanly before `i32::MAX`.
    pub fn next_id(&mut self) -> PacketId {
        let current = self.next;
        self.next = match current.checked_add(1) {
            Some(candidate) if candidate <= PacketId::MAX_ALLOCATABLE.as_i32() => candidate,
            _ => PacketId::MIN_POSITIVE.as_i32(),
        };
        PacketId(current)
    }

    /// Allocates two distinct, sequential positive [`PacketId`] values `(command_id, sentinel_id)`.
    pub fn next_pair(&mut self) -> (PacketId, PacketId) {
        let first = self.next_id();
        let second = self.next_id();
        (first, second)
    }
}

/// Validated identifier naming a logical game server in multi-server registries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerId(String);

impl ServerId {
    /// Maximum allowed character length for a [`ServerId`].
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs a [`ServerId`].
    ///
    /// Allowed characters: ASCII alphanumeric, `-`, `_`, `.`, and `:`.
    pub fn new(raw: impl Into<String>) -> Result<Self, TypeError> {
        let value = raw.into();
        if value.is_empty() || value.len() > Self::MAX_LEN {
            return Err(TypeError::InvalidServerId {
                reason: "length must be between 1 and 128 bytes",
            });
        }
        let valid_chars = value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
        if !valid_chars {
            return Err(TypeError::InvalidServerId {
                reason: "contains characters outside [A-Za-z0-9-_.:]",
            });
        }
        Ok(Self(value))
    }

    /// Borrows the validated identifier string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monotonically increasing identifier tracking an individual connection lifecycle generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(u64);

impl SessionId {
    /// Generates a unique monotonic [`SessionId`] within the current process.
    #[must_use]
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the numeric generation identifier.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ses_{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_id_allocator_wraps_without_overflow_or_duplicates_in_pairs() {
        let seed = PacketId::from_raw(PacketId::MAX_ALLOCATABLE.as_i32() - 1);
        let mut alloc = PacketIdAllocator::with_seed(seed).unwrap_or_default();

        let (a, b) = alloc.next_pair();
        let (c, d) = alloc.next_pair();

        assert_eq!(a.as_i32(), PacketId::MAX_ALLOCATABLE.as_i32() - 1);
        assert_eq!(b.as_i32(), PacketId::MAX_ALLOCATABLE.as_i32());
        assert_eq!(c.as_i32(), 1);
        assert_eq!(d.as_i32(), 2);
        assert!(a.is_positive() && b.is_positive() && c.is_positive() && d.is_positive());
        assert_ne!(a, b);
        assert_ne!(b, c);
    }

    #[test]
    fn packet_id_validation_rejects_zero_and_negative() {
        assert!(PacketId::try_positive(0).is_err());
        assert!(PacketId::try_positive(-1).is_err());
        assert!(PacketId::AUTH_FAILURE.is_auth_failure());
        assert!(!PacketId::MIN_POSITIVE.is_auth_failure());
    }

    #[test]
    fn server_id_validates_bounds_and_charset() {
        assert!(ServerId::new("eu-survival-01.prod:25575").is_ok());
        assert!(ServerId::new("").is_err());
        assert!(ServerId::new("bad space").is_err());
        assert!(ServerId::new("x".repeat(ServerId::MAX_LEN + 1)).is_err());
    }

    #[test]
    fn session_id_is_strictly_monotonic() {
        let first = SessionId::next();
        let second = SessionId::next();
        assert!(second > first);
    }
}
