use crate::cli::Config;
use crate::thread_pool::ThreadPool;
use crate::traits::{CommandRunner, NotificationSource};

/// Orchestrates pulling notifications from `source` and dispatching each
/// payload to `runner`. Generic over both to enable test doubles.
pub struct Dispatcher<R: CommandRunner> {
    runner: R,
}

impl<R: CommandRunner> Dispatcher<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    /// Pull notifications from `source` and dispatch each payload.
    /// Stops when the source is exhausted or the runner fails.
    pub fn run<S: NotificationSource>(&self, source: &mut S) {
        while let Some(payload) = source.next_payload() {
            if let Err(e) = self.runner.run(&payload) {
                eprintln!("[pg-dispatch] dispatch error: {e}");
                break;
            }
        }
    }
}

/// Convenience: build the production dispatcher from a [`Config`].
pub fn production(config: &Config) -> Dispatcher<ThreadPool> {
    let spec = crate::traits::CommandSpec {
        program: config
            .command
            .first()
            .cloned()
            .expect("command must contain at least the program name"),
        args: if config.command.len() > 1 {
            config.command[1..].to_vec()
        } else {
            vec![]
        },
    };
    let pool = ThreadPool::new(config.max_threads, spec);
    Dispatcher::new(pool)
}
