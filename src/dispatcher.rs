use tracing::{error, info};

use crate::traits::{CommandRunner, NotificationSource};

/// Orchestrates pulling notifications from a [`NotificationSource`] and
/// dispatching each payload to a [`CommandRunner`].
///
/// Generic over both traits to enable test doubles. The dispatch loop runs
/// until the source is exhausted or the runner returns an error.
///
/// # Example
///
/// ```
/// use pg_dispatcher::{Dispatcher, NotificationSource, CommandRunner, RunError};
/// use std::sync::{Arc, Mutex};
///
/// // A minimal mock source and runner for demonstration.
/// struct SeqSource { payloads: Vec<String> }
/// impl NotificationSource for SeqSource {
///     fn next_payload(&mut self) -> Option<String> {
///         self.payloads.pop()
///     }
/// }
/// struct EchoRunner { calls: Arc<Mutex<Vec<String>>> }
/// impl CommandRunner for EchoRunner {
///     fn run(&self, payload: &str) -> Result<(), RunError> {
///         self.calls.lock().unwrap().push(payload.into());
///         Ok(())
///     }
/// }
///
/// let source = SeqSource { payloads: vec!["world".into(), "hello".into()] };
/// let runner = EchoRunner { calls: Arc::new(Mutex::new(vec![])) };
/// let dispatcher = Dispatcher::new(runner);
///
/// let mut source = source;
/// dispatcher.run(&mut source);
/// ```
pub struct Dispatcher<R: CommandRunner> {
    runner: R,
}

impl<R: CommandRunner> Dispatcher<R> {
    /// Create a new dispatcher with the given command runner.
    #[must_use]
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }

    /// Pull notifications from `source` and dispatch each payload to the
    /// runner.
    ///
    /// The loop stops when:
    /// - The source returns `None` (exhausted)
    /// - The runner returns `Err` (command failure)
    pub fn run<S: NotificationSource>(&self, source: &mut S) {
        info!("dispatch loop started");

        while let Some(payload) = source.next_payload() {
            if let Err(e) = self.runner.run(&payload) {
                error!(error = %e, "dispatch error, stopping loop");
                break;
            }
        }

        info!("dispatch loop ended");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::{FailingCommandRunner, MockCommandRunner, MockNotificationSource};
    use std::sync::Arc;

    #[test]
    fn dispatches_all_payloads() {
        let source =
            MockNotificationSource::new(vec!["hello".into(), "world".into(), "third".into()]);
        let runner = MockCommandRunner::default();
        let invocations = runner.invocations.clone();
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert_eq!(
            invocations.lock().unwrap().clone(),
            vec!["hello", "world", "third"]
        );
    }

    #[test]
    fn empty_source_dispatches_nothing() {
        let source = MockNotificationSource::new(vec![]);
        let runner = MockCommandRunner::default();
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert!(invocations.lock().unwrap().is_empty());
    }

    #[test]
    fn stops_on_runner_error() {
        let source =
            MockNotificationSource::new(vec!["first".into(), "second".into(), "third".into()]);
        let runner = FailingCommandRunner::new(2);
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert_eq!(invocations.lock().unwrap().clone(), vec!["first", "second"]);
    }

    #[test]
    fn single_payload() {
        let source = MockNotificationSource::new(vec!["only".into()]);
        let runner = MockCommandRunner::default();
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert_eq!(invocations.lock().unwrap().clone(), vec!["only"]);
    }
}
