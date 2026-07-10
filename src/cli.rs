use std::ffi::OsString;

use clap::Parser;

use crate::error::Error;

/// Command-line interface for `pg-dispatcher`.
///
/// Listens to a `PostgreSQL` notification channel and executes a command for
/// each notification. The notification payload, if any, is sent to the
/// command's standard input.
#[derive(Debug, Clone, Parser)]
#[command(name = "pg-dispatcher", version, about)]
pub struct Cli {
    /// Database connection string (e.g. `postgres://user@host:port/dbname`).
    #[arg(long)]
    pub db_uri: String,

    /// `PostgreSQL` channel to `LISTEN` on.
    #[arg(long)]
    pub channel: String,

    /// Command to execute when a notification arrives. Arguments may be
    /// included, e.g. `sh script.sh`.
    #[arg(long)]
    pub exec: String,

    /// Maximum number of worker threads to spawn.
    #[arg(long, default_value = "4")]
    pub workers: usize,
}

/// Parsed configuration ready for the dispatcher.
#[derive(Debug, Clone)]
pub struct Config {
    /// Database connection string.
    pub db_url: String,
    /// `PostgreSQL` channel to LISTEN on.
    pub db_channel: String,
    /// Maximum number of worker threads.
    pub max_threads: usize,
    /// Parsed command (program + arguments).
    pub command: Vec<OsString>,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        let command = cli.exec.split_whitespace().map(OsString::from).collect();

        Self {
            db_url: cli.db_uri,
            db_channel: cli.channel,
            max_threads: cli.workers,
            command,
        }
    }
}

impl Config {
    /// Validate the configuration, returning an error if invalid.
    ///
    /// Currently checks:
    /// - Channel name contains only valid `PostgreSQL` identifier characters
    /// - Command is non-empty
    pub fn validate(&self) -> Result<(), Error> {
        validate_channel(&self.db_channel)?;

        if self.command.is_empty() {
            return Err(Error::EmptyCommand);
        }

        Ok(())
    }
}

/// Validate a `PostgreSQL` channel name for safe interpolation into `LISTEN`.
///
/// A valid unquoted identifier consists of alphanumeric characters and
/// underscores, starting with a letter or underscore. Anything else could
/// be a SQL injection vector when interpolated.
fn validate_channel(channel: &str) -> Result<(), Error> {
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
    use super::*;

    #[test]
    fn parse_required_args() {
        let cli = Cli::try_parse_from([
            "pg-dispatcher",
            "--db-uri",
            "foodb",
            "--channel",
            "foochan",
            "--exec",
            "sh test.sh",
            "--workers",
            "5",
        ])
        .unwrap();

        let config = Config::from(cli);

        assert_eq!(config.db_url, "foodb");
        assert_eq!(config.db_channel, "foochan");
        assert_eq!(config.max_threads, 5);
        assert_eq!(
            config.command,
            vec![OsString::from("sh"), OsString::from("test.sh")]
        );
    }

    #[test]
    fn default_workers_is_four() {
        let cli = Cli::try_parse_from([
            "pg-dispatcher",
            "--db-uri",
            "foodb",
            "--channel",
            "foochan",
            "--exec",
            "cat",
        ])
        .unwrap();

        let config = Config::from(cli);
        assert_eq!(config.max_threads, 4);
    }

    #[test]
    fn missing_required_arg_fails() {
        let result =
            Cli::try_parse_from(["pg-dispatcher", "--channel", "foochan", "--exec", "cat"]);

        assert!(result.is_err());
    }

    #[test]
    fn validate_accepts_simple_channel() {
        let config = Config {
            db_url: "postgres://localhost".into(),
            db_channel: "events".into(),
            max_threads: 4,
            command: vec![OsString::from("cat")],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_underscore_channel() {
        let config = Config {
            db_url: "postgres://localhost".into(),
            db_channel: "_my_channel_2".into(),
            max_threads: 4,
            command: vec![OsString::from("cat")],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_channel() {
        let config = Config {
            db_url: "postgres://localhost".into(),
            db_channel: "".into(),
            max_threads: 4,
            command: vec![OsString::from("cat")],
        };
        assert!(matches!(config.validate(), Err(Error::InvalidChannel(_))));
    }

    #[test]
    fn validate_rejects_numeric_start_channel() {
        let config = Config {
            db_url: "postgres://localhost".into(),
            db_channel: "9chan".into(),
            max_threads: 4,
            command: vec![OsString::from("cat")],
        };
        assert!(matches!(config.validate(), Err(Error::InvalidChannel(_))));
    }

    #[test]
    fn validate_rejects_special_chars_channel() {
        let config = Config {
            db_url: "postgres://localhost".into(),
            db_channel: "ev;DROP TABLE".into(),
            max_threads: 4,
            command: vec![OsString::from("cat")],
        };
        assert!(matches!(config.validate(), Err(Error::InvalidChannel(_))));
    }

    #[test]
    fn validate_rejects_empty_command() {
        let config = Config {
            db_url: "postgres://localhost".into(),
            db_channel: "events".into(),
            max_threads: 4,
            command: vec![],
        };
        assert!(matches!(config.validate(), Err(Error::EmptyCommand)));
    }
}
