use std::sync::{Arc, Mutex};

use crate::traits::{CommandRunner, NotificationSource, RunError};

/// A mock [`NotificationSource`] that yields a pre-defined list of payloads.
pub struct MockNotificationSource {
    payloads: Vec<String>,
    index: usize,
}

impl MockNotificationSource {
    pub fn new(payloads: Vec<String>) -> Self {
        Self { payloads, index: 0 }
    }
}

impl NotificationSource for MockNotificationSource {
    fn next_payload(&mut self) -> Option<String> {
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
#[derive(Default)]
pub struct MockCommandRunner {
    pub invocations: Arc<Mutex<Vec<String>>>,
}

impl CommandRunner for MockCommandRunner {
    fn run(&self, payload: &str) -> Result<(), RunError> {
        self.invocations.lock().unwrap().push(payload.to_owned());
        Ok(())
    }
}

/// A mock [`CommandRunner`] that always fails on the Nth invocation.
pub struct FailingCommandRunner {
    pub invocations: Arc<Mutex<Vec<String>>>,
    pub fail_on: usize,
}

impl FailingCommandRunner {
    pub fn new(fail_on: usize) -> Self {
        Self {
            invocations: Arc::new(Mutex::new(vec![])),
            fail_on,
        }
    }
}

impl CommandRunner for FailingCommandRunner {
    fn run(&self, payload: &str) -> Result<(), RunError> {
        let count = {
            let mut inv = self.invocations.lock().unwrap();
            inv.push(payload.to_owned());
            inv.len()
        };
        if count == self.fail_on {
            Err(RunError::Exit { code: Some(1) })
        } else {
            Ok(())
        }
    }
}
