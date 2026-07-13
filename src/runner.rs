use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::traits::{CommandRunner, CommandSpec, ExitError, RunError};

/// An async command runner that limits concurrency with a [`Semaphore`].
///
/// Each call to [`CommandRunner::run`] acquires a permit (blocking if the
/// max concurrency has been reached), then spawns the configured command,
/// writes the payload to its stdin, and awaits completion.
///
/// This replaces the previous thread-pool design with a lighter-weight
/// async approach — no OS threads are spawned per payload.
#[derive(Clone)]
pub struct CommandExecutor {
    command: Arc<CommandSpec>,
    semaphore: Arc<Semaphore>,
}

impl CommandExecutor {
    /// Create a new executor with `max_concurrency` concurrent command
    /// executions.
    ///
    /// # Panics
    /// Panics if `max_concurrency` is zero.
    #[must_use]
    pub fn new(max_concurrency: usize, command: CommandSpec) -> Self {
        assert!(
            max_concurrency > 0,
            "max concurrency must be greater than zero"
        );
        Self {
            command: Arc::new(command),
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
        }
    }
}

impl CommandRunner for CommandExecutor {
    async fn run(&self, payload: String) -> Result<(), RunError> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| RunError::Spawn("semaphore closed".into()))?;

        let program_name = self.command.program.to_string_lossy().into_owned();

        let mut child = spawn_command(&self.command, &payload).await?;

        let stdout_task = child.stdout.take().map(spawn_stdout_reader);
        let stderr_task = child.stderr.take().map(spawn_stderr_reader);

        let exit_status = child
            .wait()
            .await
            .map_err(|e| RunError::Spawn(e.to_string()))?;

        join_stream_tasks(stdout_task, stderr_task).await;

        log_exit_status(&program_name, exit_status)
    }
}

/// Spawn the configured command, write `payload` to stdin, and return the
/// child process.
async fn spawn_command(command: &CommandSpec, payload: &str) -> Result<Child, RunError> {
    let mut child = Command::new(&command.program)
        .args(&command.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| RunError::Spawn(e.to_string()))?;

    if let Some(mut stdin) = child.stdin.take() {
        if let Err(e) = stdin.write_all(payload.as_bytes()).await {
            warn!(error = %e, "failed to write to stdin");
        }
        // stdin is dropped here, signaling EOF to the child.
    }

    Ok(child)
}

/// Spawn a task that reads stdout line-by-line and logs each line.
fn spawn_stdout_reader(stream: tokio::process::ChildStdout) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => info!(command = "stdout", "{}", line.trim_end()),
                Err(e) => {
                    warn!(command = "stdout", error = %e, "read error");
                    break;
                }
            }
        }
    })
}

/// Spawn a task that reads stderr line-by-line and logs each line.
fn spawn_stderr_reader(stream: tokio::process::ChildStderr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => warn!(command = "stderr", "{}", line.trim_end()),
                Err(e) => {
                    warn!(command = "stderr", error = %e, "read error");
                    break;
                }
            }
        }
    })
}

/// Await both stream-reading tasks so all output is flushed.
async fn join_stream_tasks(stdout: Option<JoinHandle<()>>, stderr: Option<JoinHandle<()>>) {
    if let Some(task) = stdout {
        let _ = task.await;
    }
    if let Some(task) = stderr {
        let _ = task.await;
    }
}

/// Log the exit status and return `Ok` for success, `Err` for failure.
fn log_exit_status(
    program_name: &str,
    exit_status: std::process::ExitStatus,
) -> Result<(), RunError> {
    match exit_status.code() {
        Some(code) if exit_status.success() => {
            debug!(code, "command succeeded");
            Ok(())
        }
        Some(code) => {
            warn!(command = program_name, code, "command failed");
            Err(RunError::Exit(ExitError { code: Some(code) }))
        }
        None => {
            warn!(command = program_name, "command terminated by signal");
            Err(RunError::Exit(ExitError { code: None }))
        }
    }
}

// Keep the old name as an alias for backward compatibility in the public API.
pub type ThreadPool = CommandExecutor;
