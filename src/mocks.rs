use std::sync::{Arc, Mutex};

use crate::traits::{CommandRunner, ExitError, NotificationSource, RunError};

/// A mock [`NotificationSource`] that yields a pre-defined list of payloads.
pub struct MockNotificationSource {
    payloads: Vec<String>,
    index: usize,
}

impl MockNotificationSource {
    #[must_use]
    pub fn new(payloads: Vec<String>) -> Self {
        Self { payloads, index: 0 }
    }
}

impl NotificationSource for MockNotificationSource {
    async fn next_payload(&mut self) -> Option<String> {
        if self.index < self.payloads.len() {
            let payload = self.payloads[self.index].clone();
            self.index += 1;
            Some(payload)
        } else {
            None
        }
    }
}

/// A mock [`CommandRunner`] that records every payload it receives.
#[derive(Default, Clone)]
pub struct MockCommandRunner {
    pub invocations: Arc<Mutex<Vec<String>>>,
}

impl CommandRunner for MockCommandRunner {
    async fn run(&self, payload: String) -> Result<(), RunError> {
        self.invocations.lock().unwrap().push(payload);
        Ok(())
    }
}

/// A mock [`CommandRunner`] that always fails on the Nth invocation.
#[derive(Clone)]
pub struct FailingCommandRunner {
    pub invocations: Arc<Mutex<Vec<String>>>,
    pub fail_on: usize,
}

impl FailingCommandRunner {
    #[must_use]
    pub fn new(fail_on: usize) -> Self {
        Self {
            invocations: Arc::new(Mutex::new(vec![])),
            fail_on,
        }
    }
}

impl CommandRunner for FailingCommandRunner {
    async fn run(&self, payload: String) -> Result<(), RunError> {
        let count = {
            let mut inv = self.invocations.lock().unwrap();
            inv.push(payload);
            inv.len()
        };
        if count == self.fail_on {
            Err(RunError::Exit(ExitError { code: Some(1) }))
        } else {
            Ok(())
        }
    }
}
