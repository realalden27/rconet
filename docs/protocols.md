# Supported RCON Protocols & Dialect Matrix

Despite sharing a common heritage, game servers implement Remote Console (RCON) with incompatible buffer limits, authentication sequences, and multi-packet termination behaviors. Assuming every server behaves identically causes truncated commands, console warning spam, or hung connections.

`rconet-proto` models these differences explicitly through `DialectProfile` and `Preset`.

---

## 1. Binary TCP Frame Layout

Binary TCP RCON frames use little-endian signed 32-bit integers:

```text
+-------------------+-------------------+-------------------+------------------------+------------+
| Length (i32 LE)   | PacketId (i32 LE) | Type (i32 LE)     | Payload (N bytes)      | Pad (2B)   |
| = 10 + N          | Correlation ID    | 3 | 2 | 0 | other | UTF-8 / Raw / Base64   | 0x00, 0x00 |
+-------------------+-------------------+-------------------+------------------------+------------+
|<-- 4 bytes ------>|<------ `Length` bytes (minimum 10: 4B ID + 4B Type + 2B NUL pad) --------->|
```

### Standard Packet Types (`PacketKind`)
- `3` (`Auth` / `SERVERDATA_AUTH`) — Client authentication request carrying the RCON password.
- `2` (`AuthResponse` / `SERVERDATA_AUTH_RESPONSE`) — Server authentication outcome (`PacketId == request_id` on success; `PacketId == -1` on rejection) **or** client command execution (`SERVERDATA_EXECCOMMAND`, which reuses integer `2` in the client-to-server direction).
- `0` (`ResponseValue` / `SERVERDATA_RESPONSE_VALUE`) — Server command output frame (and client sentinel probe in `SourceSentinel` mode).
- `Unknown(i32)` — Preserved losslessly by `FrameCodec` and rejected deterministically by `ProtocolState` when unexpected during auth or command exchanges.

---

## 2. Dialect Preset Comparison

| Preset | Max Command Payload | Max Inbound Frame | Max Total Response | Termination | Encoding | Default Pacing |
| :--- | ---: | ---: | ---: | :--- | :--- | :--- |
| `Preset::Source` | `4,096 B` | `32,768 B` | `4 MiB` | `SourceSentinel` | `LossyUtf8` | None |
| `Preset::MinecraftJava` | `1,446 B` | `32,768 B` | `4 MiB` | `SinglePacket` | `LossyUtf8` | None (`3 ms` in legacy profile) |
| `Preset::SinglePacket` | `4,096 B` | `32,768 B` | `4 MiB` | `SinglePacket` | `LossyUtf8` | None |
| `Preset::Base64Tcp` | `4,096 B` | `32,768 B` | `4 MiB` | `SinglePacket` | `Base64Utf8` | None |

---

## 3. Verified Server Protocol Behaviors

### 3.1 Valve / Source Engine (`Preset::Source`)

1. **Authentication Handshake:**
   - Upon receiving `SERVERDATA_AUTH (3)`, classic Valve `SRCDS` emits an empty `SERVERDATA_RESPONSE_VALUE (0)` frame **before** emitting `SERVERDATA_AUTH_RESPONSE (2)`.
   - Other Source-compatible servers emit `SERVERDATA_AUTH_RESPONSE (2)` immediately.
   - `AuthExchange` accepts `0..=4` preliminary `RESPONSE_VALUE` frames before `AUTH_RESPONSE`, capping the count at `4` so a broken or malicious endpoint cannot trap the client in an infinite pre-auth loop.
2. **Deterministic Multi-Packet Termination (`TerminationMode::SourceSentinel`):**
   - Large command outputs (such as `cvarlist` or `status` on full servers) split across multiple `RESPONSE_VALUE` frames with no `"end-of-response"` bit in the header.
   - Rather than sleeping for an arbitrary idle window, `rconet` allocates two distinct positive `PacketId`s (`command_id` and `sentinel_id`) and pipelines an empty `RESPONSE_VALUE (0)` frame carrying `sentinel_id` immediately after the `EXECCOMMAND (2)` frame.
   - Because TCP RCON processes frames in FIFO order, every `RESPONSE_VALUE` carrying `command_id` arrives strictly before the echoed `sentinel_id` frame.
3. **SRCDS Empty-Sentinel Trailer Absorption:**
   - When `SRCDS` receives an empty `RESPONSE_VALUE (0)` sentinel, it replies with **two** frames carrying `sentinel_id`:
     1. An empty `RESPONSE_VALUE` (`payload = b""`).
     2. Immediately afterward, a second `RESPONSE_VALUE` with 4-byte payload `[0x00, 0x01, 0x00, 0x00]`.
   - Non-SRCDS servers that echo sentinel frames emit only a single frame.
   - `ProtocolState` records `pending_sentinel_trailer = Some(sentinel_id)` *only* when the first sentinel frame had an empty payload, drains the `00 01 00 00` trailer immediately if already buffered in `FramedConnection`, or absorbs at most **one** matching `00 01 00 00` frame on the next read—achieving zero idle delay on both server types.

### 3.2 Minecraft Java Edition (`Preset::MinecraftJava`)

1. **`1,446`-Byte Command Payload Limit:**
   - Vanilla Minecraft Java Edition reads inbound RCON packets into a fixed `1,460`-byte buffer (`RconClient.java`).
   - Subtracting the `4`-byte `Length` field, `4`-byte `PacketId`, `4`-byte `Type`, and `2`-byte trailing NUL pad (`14` bytes total) leaves **`1,446` bytes** maximum for the encoded command string.
   - Commands exceeding `1,446` bytes cause the server to fail parsing or drop the connection; `Preset::MinecraftJava` rejects `> 1,446`-byte commands locally at `CommandPhase::NotSent` (`ProtoError::CommandTooLong`).
2. **Termination Trade-Off (`SinglePacket` Default vs `SourceSentinel`):**
   - Vanilla Minecraft Java splits responses larger than `4,096` bytes across multiple frames, **but** sending a `RESPONSE_VALUE (0)` sentinel causes the server to reply with `"Unknown request 0"` (hex `0`) and log warnings on certain modded derivatives.
   - `Preset::MinecraftJava` therefore defaults to `TerminationMode::SinglePacket` for clean console operation on typical commands (`list`, `whitelist`, `tp`, `say`, `gamerule`).
   - Callers expecting `> 4,096`-byte vanilla outputs (e.g., large `data get entity` dumps) can opt into sentinel framing explicitly:
     ```rust
     use rconet::{DialectProfile, Preset, TerminationMode};

     let dialect = DialectProfile::from(Preset::MinecraftJava)
         .with_termination(TerminationMode::SourceSentinel);
     ```
     Because `ProtocolState` handles non-empty sentinel replies (`"Unknown request 0"`) cleanly without expecting an SRCDS trailer, multi-packet Minecraft Java responses terminate accurately when opted in.
3. **Inter-Command Pacing (`minecraft_java_legacy`):**
   - Historical Minecraft Java versions prior to `1.14.3` suffered from an RCON thread-safety bug (`MC-72390`) under rapid back-to-back packets.
   - Modern servers (`1.14.3+`) fixed this, so `Preset::MinecraftJava` imposes **zero** artificial latency. For `< 1.14.3` servers, `DialectProfile::minecraft_java_legacy()` configures a `3 ms` minimum interval enforced inside `SessionActor`.

### 3.3 Payload Encodings (`PayloadEncoding`)

- **`LossyUtf8` (Default):** Outbound commands/passwords reject interior `0x00` bytes (which would prematurely terminate C-strings on game servers). Inbound responses convert invalid UTF-8 sequences via `String::from_utf8_lossy` (`U+FFFD`) so custom byte codes or Latin-1 player nicknames never drop valid server output.
- **`StrictUtf8`:** Rejects non-UTF-8 inbound bytes with `ProtoError::InvalidUtf8`.
- **`Base64Utf8` (`Preset::Base64Tcp`):** Base64-encodes outbound command strings and Base64-decodes inbound response payloads for game servers with defective multi-byte C-string RCON parsers.

### 3.4 WebRCON JSON Schema (`feature = "webrcon"`)

WebSocket RCON servers exchange UTF-8 JSON objects:

```json
// Outbound request (`WebRconRequest`):
{ "Identifier": 42, "Message": "serverinfo", "Name": "rconet" }

// Inbound response or broadcast (`WebRconEvent`):
{ "Identifier": 42, "Message": "...", "Type": "Generic", "Stacktrace": "" }
```

- **Command Replies:** Echo the positive `Identifier` (`> 0`) sent by the client.
- **Unsolicited Broadcasts:** Carry `Identifier <= 0` (`0` or `-1`) for asynchronous chat, warning, and console log events (`WebRconEvent::is_unsolicited_broadcast()`).
- **Forward Compatibility:** `WebRconMessageKind` maps unrecognized server `Type` strings to `WebRconMessageKind::Unknown` rather than failing deserialization.
