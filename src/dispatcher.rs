use std::future::Future;

use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::traits::{CommandRunner, NotificationSource};

/// Orchestrates pulling notifications from a [`NotificationSource`] and
/// dispatching each payload to a [`CommandRunner`].
///
/// Generic over both traits to enable test doubles. The dispatch loop runs
/// until the source is exhausted, the runner returns an error, or a shutdown
/// signal is received.
///
/// # Example
///
/// ```no_run
/// use pg_dispatcher::{
///     Dispatcher, NotificationSource, CommandRunner, RunError,
/// };
/// use std::sync::{Arc, Mutex};
///
/// struct SeqSource { payloads: Vec<String> }
/// impl NotificationSource for SeqSource {
///     async fn next_payload(&mut self) -> Option<String> {
///         self.payloads.pop()
///     }
/// }
///
/// #[derive(Clone)]
/// struct EchoRunner { calls: Arc<Mutex<Vec<String>>> }
/// impl CommandRunner for EchoRunner {
///     async fn run(&self, payload: String) -> Result<(), RunError> {
///         self.calls.lock().unwrap().push(payload);
///         Ok(())
///     }
/// }
///
/// # async fn example() {
/// let source = SeqSource { payloads: vec!["world".into(), "hello".into()] };
/// let runner = EchoRunner { calls: Arc::new(Mutex::new(vec![])) };
/// let dispatcher = Dispatcher::new(runner);
///
/// let mut source = source;
/// dispatcher.run(&mut source, std::future::pending::<()>()).await;
/// # }
/// ```
pub struct Dispatcher<R: CommandRunner> {
    runner: R,
}

impl<R: CommandRunner + Clone + 'static> Dispatcher<R> {
    /// Create a new dispatcher with the given command runner.
    #[must_use]
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }

    /// Pull notifications from `source` and dispatch each payload to the
    /// runner concurrently.
    ///
    /// The loop stops when:
    /// - The source returns `None` (exhausted)
    /// - The runner returns `Err` (command failure)
    /// - The `shutdown` future completes (graceful shutdown)
    ///
    /// On shutdown, in-flight commands are awaited to completion before
    /// returning — no command is left running.
    ///
    /// Pass `std::future::pending::<()>()` for `shutdown` to never shut down.
    pub async fn run<S, F>(&self, source: &mut S, shutdown: F)
    where
        S: NotificationSource,
        F: Future<Output = ()>,
    {
        info!("dispatch loop started");

        let mut tasks: JoinSet<Result<(), crate::traits::RunError>> = JoinSet::new();
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                biased;

                () = &mut shutdown => {
                    info!("shutdown signal received, stopping notification loop");
                    break;
                }

                result = source.next_payload() => {
                    if let Some(payload) = result {
                        let runner = self.runner.clone();
                        tasks.spawn(async move { runner.run(payload).await });
                    } else {
                        info!("notification source exhausted");
                        break;
                    }
                }

                Some(res) = tasks.join_next() => {
                    if let Err(e) = res {
                        error!(error = %e, "dispatch task panicked");
                    }
                }
            }
        }

        // Wait for all in-flight commands to finish.
        if !tasks.is_empty() {
            info!(in_flight = tasks.len(), "waiting for in-flight commands");
            while let Some(res) = tasks.join_next().await {
                if let Err(e) = res {
                    warn!(error = %e, "in-flight command task panicked during shutdown");
                }
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
    use std::time::Duration;

    #[tokio::test]
    async fn dispatches_all_payloads() {
        let source =
            MockNotificationSource::new(vec!["hello".into(), "world".into(), "third".into()]);
        let runner = MockCommandRunner::default();
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher
            .run(&mut source, std::future::pending::<()>())
            .await;

        // Give spawned tasks a chance to complete.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let inv = invocations.lock().unwrap().clone();
        assert_eq!(inv.len(), 3, "expected 3 invocations, got: {inv:?}");
        assert!(inv.contains(&"hello".to_string()));
        assert!(inv.contains(&"world".to_string()));
        assert!(inv.contains(&"third".to_string()));
    }

    #[tokio::test]
    async fn empty_source_dispatches_nothing() {
        let source = MockNotificationSource::new(vec![]);
        let runner = MockCommandRunner::default();
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher
            .run(&mut source, std::future::pending::<()>())
            .await;

        assert!(invocations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stops_on_runner_error() {
        let source =
            MockNotificationSource::new(vec!["first".into(), "second".into(), "third".into()]);
        let runner = FailingCommandRunner::new(2);
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher
            .run(&mut source, std::future::pending::<()>())
            .await;

        // Give spawned tasks a chance to complete.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let inv = invocations.lock().unwrap().clone();
        assert!(inv.contains(&"first".to_string()));
        assert!(inv.contains(&"second".to_string()));
    }

    #[tokio::test]
    async fn single_payload() {
        let source = MockNotificationSource::new(vec!["only".into()]);
        let runner = MockCommandRunner::default();
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher
            .run(&mut source, std::future::pending::<()>())
            .await;

        // Give spawned tasks a chance to complete.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let inv = invocations.lock().unwrap().clone();
        assert_eq!(inv.len(), 1);
        assert!(inv.contains(&"only".to_string()));
    }

    #[tokio::test]
    async fn graceful_shutdown_stops_loop() {
        // A source that blocks forever — only shutdown can stop the loop.
        struct HangingSource;
        impl NotificationSource for HangingSource {
            async fn next_payload(&mut self) -> Option<String> {
                std::future::pending::<()>().await;
                None
            }
        }

        let runner = MockCommandRunner::default();
        let invocations = Arc::clone(&runner.invocations);
        let dispatcher = Dispatcher::new(runner);

        let mut source = HangingSource;
        // Shut down immediately.
        dispatcher.run(&mut source, async {}).await;

        // No payloads should have been dispatched.
        assert!(invocations.lock().unwrap().is_empty());
    }
}
