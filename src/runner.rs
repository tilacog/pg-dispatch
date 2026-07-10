use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Semaphore;
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

        let mut child = Command::new(&self.command.program)
            .args(&self.command.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| RunError::Spawn(e.to_string()))?;

        // Write payload to stdin and close it.
        if let Some(mut stdin) = child.stdin.take() {
            if let Err(e) = stdin.write_all(payload.as_bytes()).await {
                warn!(error = %e, "failed to write to stdin");
            }
            // stdin is dropped here, signaling EOF to the child.
        }

        // Read stdout and stderr concurrently.
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let stdout_task = stdout.map(|stream| {
            let name = program_name.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim_end();
                            info!(command = %name, "{trimmed}");
                        }
                        Err(e) => {
                            warn!(command = %name, error = %e, "stdout read error");
                            break;
                        }
                    }
                }
            })
        });

        let stderr_task = stderr.map(|stream| {
            let name = program_name.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim_end();
                            warn!(command = %name, "{trimmed}");
                        }
                        Err(e) => {
                            warn!(command = %name, error = %e, "stderr read error");
                            break;
                        }
                    }
                }
            })
        });

        let exit_status = child
            .wait()
            .await
            .map_err(|e| RunError::Spawn(e.to_string()))?;

        if let Some(task) = stdout_task {
            let _ = task.await;
        }
        if let Some(task) = stderr_task {
            let _ = task.await;
        }

        match exit_status.code() {
            Some(code) if exit_status.success() => {
                debug!(code, "command succeeded");
                Ok(())
            }
            Some(code) => {
                warn!(command = %program_name, code, "command failed");
                Err(RunError::Exit(ExitError { code: Some(code) }))
            }
            None => {
                warn!(command = %program_name, "command terminated by signal");
                Err(RunError::Exit(ExitError { code: None }))
            }
        }
    }
}

// Keep the old name as an alias for backward compatibility in the public API.
pub type ThreadPool = CommandExecutor;
