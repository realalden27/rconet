//! Runtime-independent RCON wire framing, dialect profiles, and protocol state machines.
//!
//! `rconet-proto` operates strictly on in-memory byte buffers and typed protocol state
//! machines without depending on any asynchronous runtime or OS socket layer.
//!
//! # Examples
//!
//! ```
//! use rconet_proto::{CommandStep, DialectProfile, Preset, ProtocolState, RawFrame};
//!
//! let mut proto = ProtocolState::new(DialectProfile::from(Preset::Source));
//! let (mut exchange, outbound_wire) = proto.begin_command("status")?;
//! assert!(!outbound_wire.is_empty());
//!
//! let cmd_id = exchange.command_id();
//! let sentinel_id = exchange.sentinel_id().expect("Source preset uses sentinel");
//!
//! assert_eq!(
//!     proto.advance_command(&mut exchange, RawFrame::response_value(cmd_id, "players: 4")),
//!     Ok(CommandStep::NeedMoreFrames)
//! );
//! let step = proto.advance_command(&mut exchange, RawFrame::response_value(sentinel_id, ""));
//! assert!(matches!(step, Ok(CommandStep::Completed(ref out)) if out.body() == "players: 4"));
//! # Ok::<(), rconet_proto::ProtoError>(())
//! ```

#![warn(missing_docs)]

mod dialect;
mod encoding;
mod error;
mod frame;
mod state;
#[cfg(feature = "webrcon")]
mod webrcon;

pub use dialect::{DialectProfile, Preset, TerminationMode};
pub use encoding::PayloadEncoding;
pub use error::ProtoError;
pub use frame::{FrameCodec, PacketKind, RawFrame};
pub use state::{
    AuthExchange, AuthStep, CommandExchange, CommandOutput, CommandStep, ProtocolState,
};
#[cfg(feature = "webrcon")]
pub use webrcon::{DEFAULT_WEBRCON_CLIENT_NAME, WebRconEvent, WebRconMessageKind, WebRconRequest};
