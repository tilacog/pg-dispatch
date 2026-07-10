use std::ffi::OsString;

/// A source of PostgreSQL notifications.
///
/// Implementations are responsible for issuing `LISTEN <channel>` and
/// yielding payloads as they arrive. This abstraction decouples the
/// dispatcher from the concrete `postgres` crate, enabling test doubles.
pub trait NotificationSource {
    /// Returns the next notification payload, or `None` when the stream
    /// has ended.
    fn next_payload(&mut self) -> Option<String>;
}

/// Runs a command for a given payload.
///
/// The production implementation spawns a subprocess, writes the payload
/// to its stdin, and propagates stdout/stderr. Test implementations can
/// record invocations without touching the filesystem.
pub trait CommandRunner {
    /// Execute the configured command with `payload` on stdin.
    ///
    /// Returns `Ok(())` on success, or an error describing what went wrong.
    fn run(&self, payload: &str) -> Result<(), RunError>;
}

/// Error returned by [`CommandRunner::run`].
#[derive(Debug, Clone)]
pub enum RunError {
    /// The command could not be spawned.
    Spawn(String),
    /// The command exited with a non-zero status code (or was signalled).
    #[allow(dead_code)]
    Exit { code: Option<i32> },
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Spawn(msg) => write!(f, "failed to spawn command: {msg}"),
            RunError::Exit { code: Some(c) } => {
                write!(f, "command failed with status code {c}")
            }
            RunError::Exit { code: None } => {
                write!(f, "command terminated by a signal")
            }
        }
    }
}

impl std::error::Error for RunError {}

/// A parsed command (program + arguments), shared across runner impls.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub program: OsString,
    pub args: Vec<OsString>,
}
