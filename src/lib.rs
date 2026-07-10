//! A `PostgreSQL` `LISTEN`/`NOTIFY` dispatcher.
//!
//! Listens to a `PostgreSQL` notification channel and executes a configurable
//! command for each notification received. The notification payload is sent
//! to the command's standard input.
//!
//! # Architecture
//!
//! The crate is built around two traits that enable dependency inversion and
//! full testability without a live database or subprocess:
//!
//! - [`NotificationSource`] — yields notification payloads (production impl:
//!   [`pg_source::PgNotificationSource`])
//! - [`CommandRunner`] — executes a command for a given payload (production
//!   impl: [`thread_pool::ThreadPool`])
//!
//! [`Dispatcher`] orchestrates the loop: pull a notification, dispatch it,
//! repeat. It is generic over both traits, so you can plug in test doubles
//! for either.
//!
//! # Example (library usage)
//!
//! ```no_run
//! use pg_dispatcher::{Dispatcher, PgNotificationSource, ThreadPool, CommandSpec};
//!
//! let source = PgNotificationSource::connect(
//!     "postgres://user@localhost/db",
//!     "events",
//! )?;
//!
//! let command = CommandSpec::new("cat");
//! let runner = ThreadPool::new(4, command);
//! let dispatcher = Dispatcher::new(runner);
//!
//! let mut source = source;
//! dispatcher.run(&mut source);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

pub mod cli;
pub mod dispatcher;
pub mod error;
pub mod pg_source;
pub mod thread_pool;
pub mod traits;

#[cfg(test)]
mod mocks;

pub use cli::{Cli, Config};
pub use dispatcher::Dispatcher;
pub use error::Error;
pub use pg_source::PgNotificationSource;
pub use thread_pool::ThreadPool;
pub use traits::{CommandRunner, CommandSpec, NotificationSource, RunError};
