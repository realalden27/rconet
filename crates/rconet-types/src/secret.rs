use std::fmt;

use zeroize::Zeroizing;

/// Redacted container for RCON passwords and authentication tokens.
///
/// Underlying secret bytes are overwritten with zeroes when dropped, and both
/// [`fmt::Debug`] and [`fmt::Display`] always emit a fixed placeholder so credentials
/// cannot accidentally leak into logs, `tracing` spans, or panic/error messages.
#[derive(Clone)]
pub struct SecretPassword {
    inner: Zeroizing<String>,
}

impl SecretPassword {
    /// Wraps a password string inside a zeroize-on-drop container.
    #[must_use]
    pub fn new(password: impl Into<String>) -> Self {
        Self {
            inner: Zeroizing::new(password.into()),
        }
    }

    /// Deliberately explicit accessor used only at protocol serialization boundaries.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.inner.as_str()
    }

    /// Returns the byte length of the stored secret without exposing its contents.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the stored password is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl From<String> for SecretPassword {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretPassword {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for SecretPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretPassword([REDACTED])")
    }
}

impl fmt::Display for SecretPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_display_never_expose_secret() {
        let secret_text = "hunter2-super-secret-!@#$%^&*()";
        let pw = SecretPassword::new(secret_text);

        let debug_out = format!("{pw:?}");
        let display_out = format!("{pw}");

        assert_eq!(debug_out, "SecretPassword([REDACTED])");
        assert_eq!(display_out, "[REDACTED]");
        assert!(!debug_out.contains("hunter2"));
        assert!(!display_out.contains("hunter2"));
        assert_eq!(pw.expose_secret(), secret_text);
        assert_eq!(pw.len(), secret_text.len());
        assert!(!pw.is_empty());
    }

    #[test]
    fn empty_password_reports_empty() {
        let pw = SecretPassword::from("");
        assert!(pw.is_empty());
        assert_eq!(pw.len(), 0);
        assert_eq!(format!("{pw:?}"), "SecretPassword([REDACTED])");
    }
}
