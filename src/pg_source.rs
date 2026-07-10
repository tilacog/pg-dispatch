use fallible_iterator::FallibleIterator;
use postgres::Client;
use tracing::debug;

use crate::error::Error;
use crate::traits::NotificationSource;

/// Production [`NotificationSource`] backed by a real `PostgreSQL` connection.
///
/// Connects to the database, issues `LISTEN <channel>`, and yields
/// notification payloads as they arrive.
pub struct PgNotificationSource {
    client: Client,
}

impl PgNotificationSource {
    /// Connect to the database, issue `LISTEN <channel>`, and return a
    /// notification source ready to yield payloads.
    ///
    /// The channel name is validated to prevent SQL injection — it must
    /// match a valid unquoted `PostgreSQL` identifier (alphanumeric + underscore,
    /// starting with a letter or underscore).
    ///
    /// # Errors
    /// Returns [`Error::Connect`] if the connection fails,
    /// [`Error::Listen`] if the `LISTEN` command fails, or
    /// [`Error::InvalidChannel`] if the channel name is invalid.
    pub fn connect(db_url: &str, channel: &str) -> Result<Self, Error> {
        validate_channel_name(channel)?;

        let mut client = Client::connect(db_url, postgres::NoTls)?;

        client.batch_execute(&format!("LISTEN {channel}"))?;

        debug!(channel, "LISTEN issued");

        Ok(Self { client })
    }
}

impl NotificationSource for PgNotificationSource {
    fn next_payload(&mut self) -> Option<String> {
        let mut notifications = self.client.notifications();
        let mut iter = notifications.blocking_iter();
        match iter.next() {
            Ok(Some(notification)) => {
                let payload = notification.payload().to_owned();
                debug!(payload = &payload, "notification received");
                Some(payload)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, "notification iterator error");
                None
            }
        }
    }
}

/// Validate a `PostgreSQL` channel name for safe interpolation into `LISTEN`.
///
/// A valid unquoted identifier consists of alphanumeric characters and
/// underscores, starting with a letter or underscore.
fn validate_channel_name(channel: &str) -> Result<(), Error> {
    if channel.is_empty() {
        return Err(Error::InvalidChannel(
            "channel name must not be empty".into(),
        ));
    }

    let mut chars = channel.chars();
    let first = chars.next().expect("checked non-empty");

    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(Error::InvalidChannel(format!(
            "channel name must start with a letter or underscore, got: {channel:?}"
        )));
    }

    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::InvalidChannel(format!(
            "channel name must contain only alphanumeric characters and underscores, got: {channel:?}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_channel_name;
    use crate::error::Error;

    #[test]
    fn valid_channel() {
        assert!(validate_channel_name("events").is_ok());
    }

    #[test]
    fn valid_channel_with_underscore() {
        assert!(validate_channel_name("_my_channel_2").is_ok());
    }

    #[test]
    fn reject_empty() {
        assert!(matches!(
            validate_channel_name(""),
            Err(Error::InvalidChannel(_))
        ));
    }

    #[test]
    fn reject_numeric_start() {
        assert!(matches!(
            validate_channel_name("9events"),
            Err(Error::InvalidChannel(_))
        ));
    }

    #[test]
    fn reject_semicolon() {
        assert!(matches!(
            validate_channel_name("ev;DROP TABLE"),
            Err(Error::InvalidChannel(_))
        ));
    }

    #[test]
    fn reject_hyphen() {
        assert!(matches!(
            validate_channel_name("my-channel"),
            Err(Error::InvalidChannel(_))
        ));
    }
}
