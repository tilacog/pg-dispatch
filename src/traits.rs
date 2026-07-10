use std::ffi::OsString;

/// A source of `PostgreSQL` notifications.
///
/// Implementations are responsible for issuing `LISTEN <channel>` and
/// yielding payloads as they arrive. This abstraction decouples the
/// dispatcher from the concrete `postgres` crate, enabling test doubles.
///
/// See [`crate::PgNotificationSource`] for the production implementation.
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
///
/// See [`crate::ThreadPool`] for the production implementation.
pub trait CommandRunner {
    /// Execute the configured command with `payload` on stdin.
    ///
    /// Returns `Ok(())` on success, or an error describing what went wrong.
    fn run(&self, payload: &str) -> Result<(), RunError>;
}

/// Error returned by [`CommandRunner::run`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum RunError {
    /// The command could not be spawned.
    #[error("failed to spawn command: {0}")]
    Spawn(String),

    /// The command exited with a non-zero status code (or was signalled).
    #[error("command failed: {0}")]
    Exit(ExitError),
}

/// Describes a non-successful process exit.
#[derive(Debug, Clone)]
pub struct ExitError {
    /// The raw exit code, or `None` if terminated by a signal.
    pub code: Option<i32>,
}

impl std::fmt::Display for ExitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            Some(code) => write!(f, "status code {code}"),
            None => write!(f, "terminated by a signal"),
        }
    }
}

/// A parsed command (program + arguments), shared across runner impls.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    /// The program to execute.
    pub program: OsString,
    /// Arguments to pass to the program (excluding the program name itself).
    pub args: Vec<OsString>,
}

impl CommandSpec {
    /// Create a new command spec from a program and its arguments.
    #[must_use]
    pub fn new(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            args: vec![],
        }
    }

    /// Create a command spec from a program and a slice of arguments.
    #[must_use]
    pub fn with_args(program: impl Into<OsString>, args: Vec<OsString>) -> Self {
        Self {
            program: program.into(),
            args,
        }
    }

    /// Parse a whitespace-separated command string into a [`CommandSpec`].
    ///
    /// # Panics
    /// Panics if the string is empty or contains only whitespace.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        let mut parts = s.split_whitespace().map(OsString::from);
        let program = parts
            .next()
            .expect("command string must contain at least a program name");
        let args = parts.collect();
        Self { program, args }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_command_with_args() {
        let spec = CommandSpec::parse("sh script.sh --flag");
        assert_eq!(spec.program, OsString::from("sh"));
        assert_eq!(
            spec.args,
            vec![OsString::from("script.sh"), OsString::from("--flag"),]
        );
    }

    #[test]
    fn parse_command_no_args() {
        let spec = CommandSpec::parse("cat");
        assert_eq!(spec.program, OsString::from("cat"));
        assert!(spec.args.is_empty());
    }

    #[test]
    fn new_sets_empty_args() {
        let spec = CommandSpec::new("echo");
        assert_eq!(spec.program, OsString::from("echo"));
        assert!(spec.args.is_empty());
    }

    #[test]
    fn with_args_preserves_args() {
        let spec =
            CommandSpec::with_args("sh", vec![OsString::from("-c"), OsString::from("echo hi")]);
        assert_eq!(spec.program, OsString::from("sh"));
        assert_eq!(spec.args.len(), 2);
    }

    #[test]
    fn exit_error_display_code() {
        let e = ExitError { code: Some(42) };
        assert_eq!(e.to_string(), "status code 42");
    }

    #[test]
    fn exit_error_display_signal() {
        let e = ExitError { code: None };
        assert_eq!(e.to_string(), "terminated by a signal");
    }

    #[test]
    #[should_panic(expected = "command string must contain")]
    fn parse_empty_panics() {
        let _ = CommandSpec::parse("");
    }
}
