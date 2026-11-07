# Security Threat Model & Operational Hardening

`rconet` is designed to operate safely against untrusted, buggy, or actively hostile network peers while preventing accidental credential disclosure inside client applications.

---

## 1. Threat Model

| Surface | Threat | Mitigation in `rconet` |
| :--- | :--- | :--- |
| **Inbound TCP Bytes** | Malicious or corrupted server sends `Length = i32::MIN`, `Length = 0`, or `Length = 2_000_000_000` to trigger integer underflow panics or OOM crashes. | `#![forbid(unsafe_code)]`, `indexing_slicing = "deny"`, checked header bounds (`< 10` -> `FrameTooSmall`), and strict pre-allocation check (`> max_inbound_frame_bytes` -> `FrameExceedsLimit`) **before** reserving buffer memory. |
| **Unbounded Multi-Packet Streams** | Rogue server streams endless `RESPONSE_VALUE` chunks without ever echoing the sentinel ID, or sends endless pre-auth `RESPONSE_VALUE` packets. | `FrameLimits::max_accumulated_response_bytes` (default `4 MiB`), `MAX_PRE_AUTH_FRAMES = 4`, plus hard per-frame (`frame_io_timeout`) and end-to-end (`command_timeout`) deadlines. |
| **Slowloris / Stalled Socket** | Peer accepts TCP connection or reads half a command frame, then stalls indefinitely. | Separate `connect_timeout`, `auth_timeout`, `frame_io_timeout`, and `command_timeout` enforced via `tokio::time::timeout`; any stalled write/read taints and closes the stream. |
| **Application Logs & Panics** | RCON password leaks into `Debug` output, error messages, crash reports, or `tracing` telemetry. | `SecretPassword` zeroes backing memory on `Drop` (`zeroize`), formats as `[REDACTED]` in `Debug` and `Display`, is redacted in `ClientBuilder`'s `Debug`, and is excluded from all `#[instrument(skip_all)]` spans and error variants. |
| **Reconnect Amplification / Auto-Ban** | Client with wrong password reconnects in a tight loop until the game server firewalls the management host. | `Error::AuthRejected` (`PacketId == -1`) permanently transitions `SessionActor` to `ConnectionState::Closed` regardless of `ReconnectPolicy`. `ServerPool` caps concurrent sockets at `MAX_POOL_SESSIONS = 32`. |
| **Duplicate Command Execution** | Network blip mid-command causes naive retry logic to execute `ban`, `kick`, or economy grant twice. | `ReplayMode::NeverReplay` is the default; post-write transport errors return `CommandPhase::OutcomeUnknown` without re-sending. |

---

## 2. Plaintext RCON Network Reality

> [!CAUTION]
> **Binary TCP RCON has zero built-in encryption or cryptographic authentication.** Passwords and administrative commands travel in cleartext over TCP and are vulnerable to passive packet capture and active TCP session hijacking on untrusted networks.

### Mandatory Production Deployment Practices

1. **Never Expose Raw TCP RCON to the Public Internet:**
   - Bind the game server's RCON listener strictly to loopback (`127.0.0.1` / `::1`) or an isolated management interface.
   - Block external ingress to RCON ports (`25575`, `27015`, etc.) in host and cloud security-group firewalls.
2. **Tunnel Remote Administration Over Encrypted Channels:**
   - **SSH Port Forwarding:** Forward a local ephemeral port to `127.0.0.1:<rcon_port>` on the game host and connect `rconet::Client` to `127.0.0.1:<local_port>`, or pass an authenticated `AsyncRead + AsyncWrite` tunnel stream directly to `FramedConnection::handshake` + `Client::from_connection`.
   - **WireGuard / Private Mesh VPN:** Route traffic exclusively over mutually authenticated encrypted tunnel interfaces.
   - **TLS Termination Proxy:** Place a mutual-TLS proxy on `127.0.0.1` alongside the game server and wrap the client's `TcpStream` in `tokio-rustls` before calling `FramedConnection::handshake`.
3. **Use Dedicated, High-Entropy Passwords:**
   - Generate unique random RCON passwords per server instance and load them from secret managers or environment variables directly into `SecretPassword` rather than committing configuration files.
