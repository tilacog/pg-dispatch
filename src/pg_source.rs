use std::future::poll_fn;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_postgres::AsyncMessage;
use tracing::{debug, warn};

use crate::error::Error;
use crate::traits::NotificationSource;

/// Production [`NotificationSource`] backed by a real `PostgreSQL` connection.
///
/// Connects to the database, issues `LISTEN <channel>`, and yields
/// notification payloads as they arrive over an async channel.
pub struct PgNotificationSource {
    rx: mpsc::Receiver<String>,
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
    pub async fn connect(db_url: &str, channel: &str) -> Result<Self, Error> {
        validate_channel_name(channel)?;

        let (client, mut connection) =
            tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;

        client.batch_execute(&format!("LISTEN {channel}")).await?;

        debug!(channel, "LISTEN issued");

        // The connection drives the async protocol. We poll it in a background
        // task and forward notification payloads to an mpsc channel.
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                let result = poll_fn(|cx| connection.poll_message(cx)).await;

                match result {
                    Some(Ok(AsyncMessage::Notification(n))) => {
                        let payload = n.payload().to_owned();
                        if tx.send(payload).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {} // Notice/NoticeResponse etc.
                    Some(Err(e)) => {
                        tracing::error!(error = %e, "postgres connection error");
                        break;
                    }
                    None => break, // Connection ended.
                }
            }
        });

        Ok(Self { rx })
    }
}

#[async_trait]
impl NotificationSource for PgNotificationSource {
    async fn next_payload(&mut self) -> Option<String> {
        self.rx.recv().await.map_or_else(
            || {
                warn!("notification channel closed");
                None
            },
            |payload| {
                debug!(payload = &payload, "notification received");
                Some(payload)
            },
        )
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
