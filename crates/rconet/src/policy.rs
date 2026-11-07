use std::time::Duration;

/// Minimum backoff delay (`10 ms`) enforced between automatic reconnection attempts
/// so a `Duration::ZERO` configuration can never trigger an unthrottled reconnect spin loop.
pub const MIN_RECONNECT_DELAY: Duration = Duration::from_millis(10);

/// Policy controlling whether and how a [`crate::Client`] re-establishes dropped connections.
///
/// Note: Explicit credential rejections (`Error::AuthRejected`) **never** trigger automatic
/// reconnection regardless of policy, preventing accidental IP bans on misconfigured servers.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum ReconnectPolicy {
    /// Do not reconnect automatically; close the session permanently once the socket drops.
    #[default]
    Disabled,

    /// Reconnect using bounded exponential backoff with automatic `±20%` jitter.
    #[non_exhaustive]
    ExponentialBackoff {
        /// Delay before the first reconnect attempt (clamped to `>= 10 ms`).
        initial_delay: Duration,
        /// Upper bound on backoff delay between attempts (clamped to `>= initial_delay`).
        max_delay: Duration,
        /// Exponential growth factor (`>= 1.0`). Non-finite or `< 1.0` values clamp to `1.0`.
        multiplier: f64,
        /// Maximum consecutive attempts (`None` = unlimited while client handles exist).
        max_attempts: Option<u32>,
    },
}

impl ReconnectPolicy {
    /// Constructs a [`ReconnectPolicy::ExponentialBackoff`] configuration.
    #[must_use]
    pub const fn exponential(
        initial_delay: Duration,
        max_delay: Duration,
        multiplier: f64,
        max_attempts: Option<u32>,
    ) -> Self {
        Self::ExponentialBackoff {
            initial_delay,
            max_delay,
            multiplier,
            max_attempts,
        }
    }

    /// Sensible conservative exponential backoff (`100 ms` ➔ `10 s`, factor `2.0`, max `8` attempts).
    #[must_use]
    pub const fn exponential_default() -> Self {
        Self::exponential(
            Duration::from_millis(100),
            Duration::from_secs(10),
            2.0,
            Some(8),
        )
    }

    /// Computes the deterministic base backoff delay for 1-based `attempt` (`1, 2, 3, ...`)
    /// without panicking on overflow, `NaN`, `Infinity`, or `Duration::ZERO`.
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: u32) -> Option<Duration> {
        match *self {
            Self::Disabled => None,
            Self::ExponentialBackoff {
                initial_delay,
                max_delay,
                multiplier,
                max_attempts,
            } => {
                if let Some(limit) = max_attempts
                    && attempt > limit
                {
                    return None;
                }

                let effective_max = max_delay.max(MIN_RECONNECT_DELAY);
                let effective_initial = initial_delay.max(MIN_RECONNECT_DELAY).min(effective_max);

                let safe_mult = if multiplier.is_finite() && multiplier >= 1.0 {
                    multiplier
                } else {
                    1.0
                };
                let exp = i32::try_from(attempt.saturating_sub(1).min(64)).unwrap_or(64);
                let factor = safe_mult.powi(exp);
                let base_secs = effective_initial.as_secs_f64();
                let scaled_secs = base_secs * factor;

                if !scaled_secs.is_finite() || scaled_secs < 0.0 {
                    return Some(effective_max);
                }

                let computed = Duration::try_from_secs_f64(scaled_secs).unwrap_or(effective_max);
                Some(computed.clamp(MIN_RECONNECT_DELAY, effective_max))
            },
        }
    }

    /// Computes the backoff delay for `attempt` with `±20%` (`[0.80, 1.20)`) SplitMix64 jitter
    /// mixed from `seed` and `attempt` to prevent thundering-herd reconnect storms.
    #[must_use]
    pub fn delay_for_attempt_jittered(&self, attempt: u32, seed: u64) -> Option<Duration> {
        let base = self.delay_for_attempt(attempt)?;
        let mixed = splitmix64(seed ^ u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let unit = ((mixed >> 11) as f64) * (1.0 / ((1u64 << 53) as f64));
        let jitter_factor = 0.80 + (unit * 0.40);
        let jittered_secs = base.as_secs_f64() * jitter_factor;
        let jittered = Duration::try_from_secs_f64(jittered_secs).unwrap_or(base);
        Some(jittered.max(MIN_RECONNECT_DELAY))
    }
}

const fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Explicit policy governing command replay when a transport fails.
///
/// Commands in [`rconet_types::CommandPhase::NotSent`] (for example, queued while a reconnect
/// is already in progress) have never touched a socket and are always safe to send once
/// reconnected. `ReplayMode` governs what happens when a command fails in
/// [`rconet_types::CommandPhase::OutcomeUnknown`].
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ReplayMode {
    /// **Default:** Never automatically replay any command once socket writing has started,
    /// even if marked [`rconet_types::Idempotency::Idempotent`].
    #[default]
    NeverReplay,

    /// Permits at most `max_replays` automatic replays **only** for commands explicitly marked
    /// [`rconet_types::Idempotency::Idempotent`] by the caller.
    ///
    /// [`rconet_types::Idempotency::NonIdempotent`] commands are still never replayed.
    ReplayIdempotentOnly {
        /// Maximum replay attempts for an idempotent command (`1..=5` recommended).
        max_replays: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_handles_nan_infinity_and_huge_attempts_without_panicking() {
        let policy = ReconnectPolicy::ExponentialBackoff {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            max_attempts: Some(4),
        };

        assert_eq!(
            policy.delay_for_attempt(1),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            policy.delay_for_attempt(2),
            Some(Duration::from_millis(200))
        );
        assert_eq!(
            policy.delay_for_attempt(4),
            Some(Duration::from_millis(800))
        );
        assert_eq!(policy.delay_for_attempt(5), None);

        let hostile_nan = ReconnectPolicy::ExponentialBackoff {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
            multiplier: f64::NAN,
            max_attempts: None,
        };
        assert_eq!(
            hostile_nan.delay_for_attempt(999),
            Some(Duration::from_millis(50))
        );

        let hostile_inf = ReconnectPolicy::ExponentialBackoff {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(3),
            multiplier: f64::INFINITY,
            max_attempts: None,
        };
        assert_eq!(
            hostile_inf.delay_for_attempt(10),
            Some(Duration::from_secs(1))
        );

        let zero_floor = ReconnectPolicy::exponential(Duration::ZERO, Duration::ZERO, 2.0, Some(2));
        assert_eq!(zero_floor.delay_for_attempt(1), Some(MIN_RECONNECT_DELAY));

        for seed in 1..=32u64 {
            let jittered = policy
                .delay_for_attempt_jittered(2, seed)
                .unwrap_or_default();
            assert!(jittered >= Duration::from_millis(160));
            assert!(jittered <= Duration::from_millis(240));
        }
    }
}
