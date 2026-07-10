//! An async `PostgreSQL` `LISTEN`/`NOTIFY` dispatcher.
//!
//! Listens to a `PostgreSQL` notification channel and executes a configurable
//! command for each notification received. The notification payload is sent
//! to the command's standard input.
//!
//! Built on `tokio` and `tokio-postgres` for fully async operation — no OS
//! threads are spawned per payload. Concurrency is bounded by a
//! [`tokio::sync::Semaphore`].
//!
//! # Architecture
//!
//! The crate is built around two async traits that enable dependency
//! inversion and full testability without a live database or subprocess:
//!
//! - [`NotificationSource`] — async stream of notification payloads
//!   (production impl: [`PgNotificationSource`])
//! - [`CommandRunner`] — async command execution (production impl:
//!   [`CommandExecutor`])
//!
//! [`Dispatcher`] orchestrates the loop: pull a notification, dispatch it,
//! repeat. It is generic over both traits, so you can plug in test doubles
//! for either.
//!
//! # Example (library usage)
//!
//! ```no_run
//! use pg_dispatcher::{
//!     CommandExecutor, CommandSpec, Dispatcher, PgNotificationSource,
//! };
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut source = PgNotificationSource::connect(
//!     "postgres://user@localhost/db",
//!     "events",
//! ).await?;
//!
//! let command = CommandSpec::new("cat");
//! let runner = CommandExecutor::new(4, command);
//! let dispatcher = Dispatcher::new(runner);
//!
//! // Shut down on Ctrl+C. Use `std::future::pending::<()>()` to never shut down.
//! let shutdown = async {
//!     let _ = tokio::signal::ctrl_c().await;
//! };
//! dispatcher.run(&mut source, shutdown).await;
//! # Ok(())
//! # }
//! ```

#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

pub mod cli;
pub mod dispatcher;
pub mod error;
pub mod pg_source;
pub mod runner;
pub mod traits;

#[cfg(test)]
mod mocks;

pub use cli::{Cli, Config};
pub use dispatcher::Dispatcher;
pub use error::Error;
pub use pg_source::PgNotificationSource;
pub use runner::{CommandExecutor, ThreadPool};
pub use traits::{CommandRunner, CommandSpec, NotificationSource, RunError};
