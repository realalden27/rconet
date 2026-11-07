#[cfg(feature = "base64")]
use base64::Engine as _;
use bytes::Bytes;
use zeroize::Zeroizing;

use crate::error::ProtoError;

/// Text and byte encoding applied to command requests and response bodies.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PayloadEncoding {
    /// Encodes requests as UTF-8 and decodes responses with UTF-8 replacement characters (`U+FFFD`)
    /// if a game server emits non-UTF-8 player nicknames or truncated multi-byte sequences.
    #[default]
    LossyUtf8,

    /// Requires inbound response bytes to be valid UTF-8, returning [`ProtoError::InvalidUtf8`] otherwise.
    StrictUtf8,

    /// Base64-encodes outbound UTF-8 strings and Base64-decodes inbound response bodies.
    ///
    /// Useful for servers whose native console parser mishandles raw multi-byte UTF-8 payloads.
    #[cfg(feature = "base64")]
    Base64Utf8,
}

impl PayloadEncoding {
    /// Encodes an outbound command payload and validates that no interior NUL (`0x00`) byte is present.
    pub fn encode_command(self, command: &str, max_bytes: usize) -> Result<Bytes, ProtoError> {
        if command.as_bytes().contains(&0) {
            return Err(ProtoError::CommandContainsNullByte);
        }

        let encoded = match self {
            Self::LossyUtf8 | Self::StrictUtf8 => Bytes::copy_from_slice(command.as_bytes()),
            #[cfg(feature = "base64")]
            Self::Base64Utf8 => {
                let b64 = base64::engine::general_purpose::STANDARD.encode(command.as_bytes());
                Bytes::from(b64.into_bytes())
            },
        };

        if encoded.len() > max_bytes {
            return Err(ProtoError::CommandTooLong {
                length: encoded.len(),
                max: max_bytes,
            });
        }
        Ok(encoded)
    }

    /// Encodes an outbound authentication credential into a [`Zeroizing`] byte buffer
    /// without creating unzeroized heap copies or leaking secret contents in errors.
    pub fn encode_password(
        self,
        password: &str,
        max_bytes: usize,
    ) -> Result<Zeroizing<Vec<u8>>, ProtoError> {
        if password.as_bytes().contains(&0) {
            return Err(ProtoError::PasswordContainsNullByte);
        }

        match self {
            Self::LossyUtf8 | Self::StrictUtf8 => {
                if password.len() > max_bytes {
                    return Err(ProtoError::PasswordTooLong {
                        length: password.len(),
                        max: max_bytes,
                    });
                }
                let mut buf = Zeroizing::new(Vec::with_capacity(password.len()));
                buf.extend_from_slice(password.as_bytes());
                Ok(buf)
            },
            #[cfg(feature = "base64")]
            Self::Base64Utf8 => {
                let out_len = base64::encoded_len(password.len(), true).ok_or(
                    ProtoError::PasswordTooLong {
                        length: usize::MAX,
                        max: max_bytes,
                    },
                )?;
                if out_len > max_bytes {
                    return Err(ProtoError::PasswordTooLong {
                        length: out_len,
                        max: max_bytes,
                    });
                }
                let mut buf = Zeroizing::new(vec![0u8; out_len]);
                let written = base64::engine::general_purpose::STANDARD
                    .encode_slice(password.as_bytes(), &mut buf)
                    .map_err(|_| ProtoError::PasswordTooLong {
                        length: out_len,
                        max: max_bytes,
                    })?;
                buf.truncate(written);
                Ok(buf)
            },
        }
    }

    /// Decodes an assembled response byte buffer into a Rust [`String`].
    pub fn decode_response(self, raw: &[u8]) -> Result<String, ProtoError> {
        match self {
            Self::LossyUtf8 => Ok(String::from_utf8_lossy(raw).into_owned()),
            Self::StrictUtf8 => std::str::from_utf8(raw)
                .map(str::to_owned)
                .map_err(|_| ProtoError::InvalidUtf8),
            #[cfg(feature = "base64")]
            Self::Base64Utf8 => {
                let trimmed = trim_ascii_whitespace(raw);
                if trimmed.is_empty() {
                    return Ok(String::new());
                }
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(trimmed)
                    .map_err(|_| ProtoError::InvalidBase64)?;
                Ok(String::from_utf8_lossy(&decoded).into_owned())
            },
        }
    }
}

#[cfg(feature = "base64")]
fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |idx| idx.saturating_add(1));
    bytes.get(start..end).unwrap_or(&[])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lossy_utf8_survives_invalid_byte_sequences_in_player_names() {
        let raw = b"Connected: Player_\xFF\xFE_1\n";
        let decoded = PayloadEncoding::LossyUtf8.decode_response(raw);
        assert!(decoded.is_ok());
        assert!(decoded.unwrap_or_default().contains("Player_"));
    }

    #[test]
    fn strict_utf8_rejects_invalid_sequences() {
        let raw = b"bad \xFF sequence";
        assert_eq!(
            PayloadEncoding::StrictUtf8.decode_response(raw),
            Err(ProtoError::InvalidUtf8)
        );
    }

    #[test]
    fn rejects_interior_null_bytes_in_commands_and_passwords() {
        assert_eq!(
            PayloadEncoding::LossyUtf8.encode_command("say hi\0bad", 1024),
            Err(ProtoError::CommandContainsNullByte)
        );
        assert_eq!(
            PayloadEncoding::LossyUtf8.encode_password("sec\0ret", 1024),
            Err(ProtoError::PasswordContainsNullByte)
        );
    }

    #[cfg(feature = "base64")]
    #[test]
    fn base64_encoding_roundtrips_multibyte_unicode() {
        let cmd = "Broadcast こんにちは世界 🌍";
        let wire = PayloadEncoding::Base64Utf8
            .encode_command(cmd, 4096)
            .unwrap_or_default();
        let decoded = PayloadEncoding::Base64Utf8.decode_response(&wire);
        assert_eq!(decoded.as_deref(), Ok(cmd));

        let pw = PayloadEncoding::Base64Utf8.encode_password("s3cr3t!", 64);
        assert!(pw.is_ok());
        assert_eq!(
            PayloadEncoding::Base64Utf8.encode_password("s3cr3t!", 4),
            Err(ProtoError::PasswordTooLong { length: 12, max: 4 })
        );
    }
}
