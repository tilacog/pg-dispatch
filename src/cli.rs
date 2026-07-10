use std::ffi::OsString;

use clap::Parser;

/// Listens to a PostgreSQL notification channel and executes a command for each
/// notification. The notification payload, if any, is sent to the command's
/// standard input.
#[derive(Debug, Parser)]
#[command(name = "pg-dispatcher", version, about)]
pub struct Cli {
    /// Database connection string (e.g. postgres://user@host:port/dbname)
    #[arg(long)]
    pub db_uri: String,

    /// PostgreSQL channel to LISTEN on
    #[arg(long)]
    pub channel: String,

    /// Command to execute when a notification arrives. Arguments may be
    /// included, e.g. `sh script.sh`.
    #[arg(long)]
    pub exec: String,

    /// Maximum number of worker threads to spawn (default: 4)
    #[arg(long, default_value = "4")]
    pub workers: usize,
}

/// Parsed configuration ready for the dispatcher.
#[derive(Debug, Clone)]
pub struct Config {
    pub db_url: String,
    pub db_channel: String,
    pub max_threads: usize,
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
}
