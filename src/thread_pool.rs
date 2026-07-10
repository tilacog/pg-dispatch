use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;

use crate::traits::{CommandRunner, CommandSpec, RunError};

/// Messages exchanged on the job channel.
enum Message {
    Payload(String),
    Terminate,
}

/// A fixed-size thread pool that runs a configurable command for each
/// payload received via [`ThreadPool::execute`].
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Message>>,
}

impl ThreadPool {
    /// Create a pool with `size` workers, each running `command` (program +
    /// arguments) when a payload arrives.
    ///
    /// # Panics
    /// Panics if `size` is zero.
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
        match &self.sender {
            Some(sender) => sender
                .send(Message::Payload(payload.to_owned()))
                .map_err(|_| RunError::Spawn("worker channel closed".into())),
            None => Err(RunError::Spawn("worker channel closed".into())),
        }
        .map(|_| ())
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Signal termination to all workers.
        if let Some(sender) = self.sender.take() {
            for _ in &self.workers {
                let _ = sender.send(Message::Terminate);
            }
        }

        // Wait for each worker to finish.
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
            // Lock, receive, and immediately drop the guard so other workers
            // can pick up the next message.
            let message = {
                let guard = match receiver.lock() {
                    Ok(g) => g,
                    Err(e) => {
                        eprintln!("[worker-{id}] receiver lock poisoned: {e}");
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
            eprintln!("[worker-{id}] failed to spawn command: {e}");
            return;
        }
    };

    // Write payload to stdin and close it.
    if let Some(mut stdin) = child.stdin.take() {
        if let Err(e) = stdin.write_all(payload.as_bytes()) {
            eprintln!("[worker-{id}] failed to write to stdin: {e}");
        }
        // stdin is dropped here, signaling EOF to the child.
    }

    // Read stdout and stderr concurrently while the process runs.
    // This avoids deadlocks when the child fills an OS pipe buffer.
    let program_name = command.program.to_string_lossy().into_owned();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let stdout_handle = stdout.map(|stream| {
        let name = program_name.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                match line {
                    Ok(line) => println!("[{name}-{id}] {line}"),
                    Err(e) => eprintln!("[{name}-{id}] stdout read error: {e}"),
                }
            }
        })
    });

    let stderr_handle = stderr.map(|stream| {
        let name = program_name.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                match line {
                    Ok(line) => eprintln!("[{name}-{id}] {line}"),
                    Err(e) => eprintln!("[{name}-{id}] stderr read error: {e}"),
                }
            }
        })
    });

    let exit_status = match child.wait() {
        Ok(status) => status,
        Err(e) => {
            eprintln!("[worker-{id}] failed to wait for command: {e}");
            return;
        }
    };

    // Join stream threads so all output is flushed before we log the result.
    if let Some(h) = stdout_handle {
        let _ = h.join();
    }
    if let Some(h) = stderr_handle {
        let _ = h.join();
    }

    match exit_status.code() {
        Some(code) if exit_status.success() => {
            println!("[worker-{id}] command succeeded with status code {code}.");
        }
        Some(code) => {
            eprintln!("[worker-{id}] {program_name} failed with status code {code}.");
        }
        None => {
            eprintln!("[worker-{id}] {program_name} terminated by a signal.");
        }
    }
}
