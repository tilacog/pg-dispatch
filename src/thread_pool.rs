use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;

use tracing::{debug, error, info, warn};

use crate::traits::{CommandRunner, CommandSpec, RunError};

/// Messages exchanged on the job channel.
enum Message {
    Payload(String),
    Terminate,
}

/// A fixed-size thread pool that runs a configurable command for each
/// payload received via [`CommandRunner::run`].
///
/// Each worker spawns the configured command, writes the payload to its
/// stdin, and propagates stdout/stderr line-by-line. Workers run
/// concurrently, up to the configured pool size.
///
/// # Panics
/// [`ThreadPool::new`] panics if `size` is zero.
///
/// # Shutdown
/// On drop, all workers are sent a terminate signal and joined. This
/// ensures no worker is left running after the pool is destroyed.
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Message>>,
}

impl ThreadPool {
    /// Create a pool with `size` workers, each running `command` when a
    /// payload arrives.
    ///
    /// # Panics
    /// Panics if `size` is zero.
    #[must_use]
    pub fn new(size: usize, command: CommandSpec) -> Self {
        assert!(size > 0, "thread pool size must be greater than zero");

        let (sender, receiver) = mpsc::channel();
        let receiver = std::sync::Mutex::new(receiver);
        let receiver = std::sync::Arc::new(receiver);
        let command = std::sync::Arc::new(command);

        let workers = (0..size)
            .map(|id| Worker::new(id, receiver.clone(), command.clone()))
            .collect();

        Self {
            workers,
            sender: Some(sender),
        }
    }
}

impl CommandRunner for ThreadPool {
    fn run(&self, payload: &str) -> Result<(), RunError> {
        self.sender.as_ref().map_or_else(
            || Err(RunError::Spawn("worker channel closed".into())),
            |sender| {
                sender
                    .send(Message::Payload(payload.to_owned()))
                    .map_err(|_| RunError::Spawn("worker channel closed".into()))
            },
        )
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            for _ in &self.workers {
                let _ = sender.send(Message::Terminate);
            }
        }

        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

struct Worker {
    #[allow(dead_code)]
    id: usize,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        id: usize,
        receiver: std::sync::Arc<std::sync::Mutex<mpsc::Receiver<Message>>>,
        command: std::sync::Arc<CommandSpec>,
    ) -> Self {
        let handle = thread::spawn(move || loop {
            let message = {
                let guard = match receiver.lock() {
                    Ok(g) => g,
                    Err(e) => {
                        error!(worker = id, error = %e, "receiver lock poisoned");
                        break;
                    }
                };
                match guard.recv() {
                    Ok(msg) => msg,
                    Err(_) => break,
                }
            };

            match message {
                Message::Payload(payload) => {
                    run_command(id, &command, &payload);
                }
                Message::Terminate => break,
            }
        });

        Self {
            id,
            handle: Some(handle),
        }
    }
}

/// Spawn the configured command, write `payload` to its stdin, and propagate
/// stdout/stderr line-by-line.
fn run_command(id: usize, command: &CommandSpec, payload: &str) {
    let mut child = match Command::new(&command.program)
        .args(&command.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            error!(worker = id, error = %e, "failed to spawn command");
            return;
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        if let Err(e) = stdin.write_all(payload.as_bytes()) {
            warn!(worker = id, error = %e, "failed to write to stdin");
        }
        // stdin is dropped here, signaling EOF to the child.
    }

    let program_name = command.program.to_string_lossy().into_owned();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let stdout_handle = stdout.map(|stream| {
        let name = program_name.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                match line {
                    Ok(line) => info!(worker = id, command = %name, "{line}"),
                    Err(e) => warn!(worker = id, command = %name, error = %e, "stdout read error"),
                }
            }
        })
    });

    let stderr_handle = stderr.map(|stream| {
        let name = program_name.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                match line {
                    Ok(line) => warn!(worker = id, command = %name, "{line}"),
                    Err(e) => warn!(worker = id, command = %name, error = %e, "stderr read error"),
                }
            }
        })
    });

    let exit_status = match child.wait() {
        Ok(status) => status,
        Err(e) => {
            error!(worker = id, error = %e, "failed to wait for command");
            return;
        }
    };

    if let Some(h) = stdout_handle {
        let _ = h.join();
    }
    if let Some(h) = stderr_handle {
        let _ = h.join();
    }

    match exit_status.code() {
        Some(code) if exit_status.success() => {
            debug!(worker = id, code, "command succeeded");
        }
        Some(code) => {
            warn!(worker = id, command = %program_name, code, "command failed");
        }
        None => {
            warn!(worker = id, command = %program_name, "command terminated by signal");
        }
    }
}
