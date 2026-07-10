use fallible_iterator::FallibleIterator;
use postgres::Client;

use crate::traits::NotificationSource;

/// Production [`NotificationSource`] backed by a real PostgreSQL connection.
pub struct PgNotificationSource {
    client: Client,
}

impl PgNotificationSource {
    /// Connect to the database, issue `LISTEN <channel>`, and return a
    /// notification source ready to yield payloads.
    pub fn connect(db_url: &str, channel: &str) -> Result<Self, ConnectError> {
        let mut client =
            Client::connect(db_url, postgres::NoTls).map_err(ConnectError::Connection)?;

        client
            .batch_execute(&format!("LISTEN {channel}"))
            .map_err(ConnectError::Listen)?;

        Ok(Self { client })
    }
}

/// Error connecting or issuing LISTEN.
#[derive(Debug)]
pub enum ConnectError {
    Connection(postgres::Error),
    Listen(postgres::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Connection(e) => write!(f, "failed to connect to database: {e}"),
            ConnectError::Listen(e) => write!(f, "failed to issue LISTEN: {e}"),
        }
    }
}

impl std::error::Error for ConnectError {}

impl NotificationSource for PgNotificationSource {
    fn next_payload(&mut self) -> Option<String> {
        let mut notifications = self.client.notifications();
        let mut iter = notifications.blocking_iter();
        match iter.next() {
            Ok(Some(notification)) => Some(notification.payload().to_owned()),
            Ok(None) => None,
            Err(_) => None,
        }
    }
}
