use std::time::Duration;

use rconet_types::FrameLimits;

use crate::encoding::PayloadEncoding;

/// Strategy used to determine when all response frames for an executed command have arrived.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TerminationMode {
    /// Appends an empty `SERVERDATA_RESPONSE_VALUE` (`0`) sentinel frame carrying a second
    /// positive [`rconet_types::PacketId`] immediately behind each command frame.
    ///
    /// Because compliant Source servers process inbound frames sequentially, receiving the
    /// echoed sentinel identifier proves all preceding response chunks have arrived without
    /// introducing idle-wait polling latency.
    ///
    /// Also safely absorbs the optional trailing `0x00010000` second acknowledgment packet
    /// emitted by classic Source Dedicated Servers (SRCDS).
    SourceSentinel,

    /// Expects exactly one `SERVERDATA_RESPONSE_VALUE` (`0`) frame per command and transmits
    /// no trailing sentinel packet.
    ///
    /// Appropriate for servers whose responses fit in a single frame or whose parser rejects
    /// empty `RESPONSE_VALUE` echo frames. If a server unexpectedly emits multiple frames in
    /// `SinglePacket` mode, subsequent exchanges detect the leftover frame via strict
    /// [`rconet_types::PacketId`] correlation rather than silently returning stale output.
    SinglePacket,

    /// Collects one or more `SERVERDATA_RESPONSE_VALUE` (`0`) frames without sending a trailing
    /// sentinel packet, completing once no further frame arrives within `idle_window` after the
    /// most recent fragment.
    ///
    /// Ideal for servers whose console parser rejects empty sentinel packets (or logs warnings
    /// such as `"Unknown request 0"`) when commands may still emit multi-packet responses.
    CoalesceWindow {
        /// Quiet window after receiving a response fragment before finalizing the output.
        idle_window: Duration,
    },
}

/// Named protocol presets tuned to verified server implementations.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Standard Source / Valve Dedicated Server (SRCDS) binary RCON over TCP.
    ///
    /// - Max command payload: `4096` bytes.
    /// - Termination: [`TerminationMode::SourceSentinel`] for deterministic multi-packet reassembly.
    #[default]
    Source,

    /// Modern Minecraft Java Edition (`>= 1.14.3`) RCON over TCP.
    ///
    /// - Max command payload: `1446` bytes (`1460`-byte server read buffer minus `14` header/pad bytes).
    /// - Termination: [`TerminationMode::SinglePacket`] by default so servers and proxies do not
    ///   log `"Unknown request 0"` warnings on every command. Callers running commands whose
    ///   output exceeds `4096` bytes can opt into [`TerminationMode::CoalesceWindow`] (zero
    ///   console warnings) or [`TerminationMode::SourceSentinel`] via [`DialectProfile::with_termination`].
    /// - Pacing: No artificial delay (thread-safety issue `MC-72390` was resolved in 1.14.3;
    ///   use [`DialectProfile::minecraft_java_legacy`] for `< 1.14.3` servers).
    MinecraftJava,

    /// Generic single-packet TCP RCON (`4096`-byte command cap, no sentinel packet).
    ///
    /// Suitable for game servers where sending an empty sentinel frame receives no reply or errors.
    SinglePacket,

    /// Single-packet TCP RCON with Base64 UTF-8 payload wrapping.
    #[cfg(feature = "base64")]
    Base64Tcp,
}

/// Complete wire-level behavioral profile driving `rconet-proto` state machines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialectProfile {
    limits: FrameLimits,
    termination: TerminationMode,
    encoding: PayloadEncoding,
    auth_encoding: PayloadEncoding,
    min_command_interval: Option<Duration>,
    max_pre_auth_frames: usize,
}

impl Default for DialectProfile {
    fn default() -> Self {
        Self::from_preset(Preset::Source)
    }
}

impl From<Preset> for DialectProfile {
    fn from(preset: Preset) -> Self {
        Self::from_preset(preset)
    }
}

impl DialectProfile {
    /// Constructs a [`DialectProfile`] from a standard [`Preset`].
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        match preset {
            Preset::Source => Self {
                limits: FrameLimits::source_default(),
                termination: TerminationMode::SourceSentinel,
                encoding: PayloadEncoding::LossyUtf8,
                auth_encoding: PayloadEncoding::LossyUtf8,
                min_command_interval: None,
                max_pre_auth_frames: 8,
            },
            Preset::MinecraftJava => Self {
                limits: FrameLimits::minecraft_java_default(),
                termination: TerminationMode::SinglePacket,
                encoding: PayloadEncoding::LossyUtf8,
                auth_encoding: PayloadEncoding::LossyUtf8,
                min_command_interval: None,
                max_pre_auth_frames: 4,
            },
            Preset::SinglePacket => Self {
                limits: FrameLimits::source_default(),
                termination: TerminationMode::SinglePacket,
                encoding: PayloadEncoding::LossyUtf8,
                auth_encoding: PayloadEncoding::LossyUtf8,
                min_command_interval: None,
                max_pre_auth_frames: 8,
            },
            #[cfg(feature = "base64")]
            Preset::Base64Tcp => Self {
                limits: FrameLimits::source_default(),
                termination: TerminationMode::SinglePacket,
                encoding: PayloadEncoding::Base64Utf8,
                auth_encoding: PayloadEncoding::LossyUtf8,
                min_command_interval: None,
                max_pre_auth_frames: 8,
            },
        }
    }

    /// Profile for legacy Minecraft Java Edition servers (`< 1.14.3`) affected by `MC-72390`,
    /// applying a conservative `3 ms` minimum interval between consecutive commands.
    #[must_use]
    pub const fn minecraft_java_legacy() -> Self {
        Self::from_preset(Preset::MinecraftJava)
            .with_min_command_interval(Some(Duration::from_millis(3)))
    }

    /// Returns the active [`FrameLimits`].
    #[must_use]
    pub const fn limits(self) -> FrameLimits {
        self.limits
    }

    /// Returns the response [`TerminationMode`].
    #[must_use]
    pub const fn termination(self) -> TerminationMode {
        self.termination
    }

    /// Returns the command/response [`PayloadEncoding`].
    #[must_use]
    pub const fn encoding(self) -> PayloadEncoding {
        self.encoding
    }

    /// Returns the credential [`PayloadEncoding`] used during authentication.
    #[must_use]
    pub const fn auth_encoding(self) -> PayloadEncoding {
        self.auth_encoding
    }

    /// Returns the minimum spacing enforced between consecutive outbound commands, if any.
    #[must_use]
    pub const fn min_command_interval(self) -> Option<Duration> {
        self.min_command_interval
    }

    /// Maximum preliminary `RESPONSE_VALUE` packets tolerated prior to `AUTH_RESPONSE`.
    #[must_use]
    pub const fn max_pre_auth_frames(self) -> usize {
        self.max_pre_auth_frames
    }

    /// Overrides the [`FrameLimits`].
    #[must_use]
    pub const fn with_limits(mut self, limits: FrameLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Overrides the response [`TerminationMode`].
    #[must_use]
    pub const fn with_termination(mut self, termination: TerminationMode) -> Self {
        self.termination = termination;
        self
    }

    /// Overrides the command/response [`PayloadEncoding`].
    #[must_use]
    pub const fn with_encoding(mut self, encoding: PayloadEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Overrides the authentication credential [`PayloadEncoding`].
    #[must_use]
    pub const fn with_auth_encoding(mut self, auth_encoding: PayloadEncoding) -> Self {
        self.auth_encoding = auth_encoding;
        self
    }

    /// Sets or clears minimum inter-command pacing.
    #[must_use]
    pub const fn with_min_command_interval(mut self, interval: Option<Duration>) -> Self {
        self.min_command_interval = interval;
        self
    }
}
