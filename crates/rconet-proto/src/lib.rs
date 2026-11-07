//! Runtime-independent RCON wire framing, dialect profiles, and protocol state machines.
//!
//! `rconet-proto` operates strictly on in-memory byte buffers and typed protocol events
//! without depending on any asynchronous runtime or socket implementation.
