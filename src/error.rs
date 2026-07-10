use thiserror::Error;

/// All errors produced by this crate.
///
/// Each variant maps to a distinct failure mode in the dispatch lifecycle.
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to establish a database connection.
    #[error("failed to connect to database: {0}")]
    Connect(#[from] postgres::Error),

    /// Failed to issue the `LISTEN` command.
    #[error("failed to issue LISTEN: {0}")]
    Listen(postgres::Error),

    /// The channel name contains characters that are not valid in a
    /// `PostgreSQL` identifier when unquoted.
    #[error("invalid channel name: {0}")]
    InvalidChannel(String),

    /// The command spec is empty (no program specified).
    #[error("command spec is empty")]
    EmptyCommand,

    /// A dispatch failure from the command runner.
    #[error("{0}")]
    Run(#[from] crate::traits::RunError),
}
