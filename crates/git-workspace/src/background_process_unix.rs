//! Cancellation for native Git processes owned by background prefetch.
use std::collections::HashSet;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, ensure};

#[derive(Default)]
struct State {
    cancelled: bool,
    groups: HashSet<u32>,
}

/// Stops only this worker's Git process groups and prevents subsequent starts.
#[derive(Clone, Default)]
pub struct BackgroundCancellation(Arc<Mutex<State>>);

impl BackgroundCancellation {
    pub fn cancel(&self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.cancelled = true;
        for pid in &state.groups {
            // Registered children are unreaped, so their IDs cannot be reused.
            unsafe { libc::kill(-(*pid as i32), libc::SIGKILL) };
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancelled
    }

    pub(crate) fn check(&self) -> Result<()> {
        ensure!(
            !self
                .0
                .lock()
                .map_err(|_| anyhow::anyhow!("background cancellation lock poisoned"))?
                .cancelled,
            "background prefetch cancelled"
        );
        Ok(())
    }
}

pub(crate) struct NativeChild {
    pub(crate) child: Child,
    cancellation: Option<BackgroundCancellation>,
    reaped: bool,
}

impl NativeChild {
    pub(crate) fn spawn(
        command: &mut Command,
        cancellation: Option<&BackgroundCancellation>,
    ) -> Result<Self> {
        command.process_group(0);
        // Serialize spawn/register with cancellation: cancellation cannot miss a
        // newly started child, and a cancelled worker cannot start another one.
        let mut state = cancellation.map(|c| c.0.lock().unwrap_or_else(|e| e.into_inner()));
        ensure!(
            !state.as_ref().is_some_and(|s| s.cancelled),
            "background prefetch cancelled"
        );
        let child = command.spawn().context("start Git subprocess")?;
        if let Some(state) = state.as_mut() {
            state.groups.insert(child.id());
        }
        Ok(Self {
            child,
            cancellation: cancellation.cloned(),
            reaped: false,
        })
    }

    pub(crate) fn finish(&mut self) -> Result<ExitStatus> {
        // Observe exit without reaping. Pipe EOF alone is insufficient: Git may
        // still be finishing object-store writes after closing its output.
        loop {
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id(),
                    &mut info,
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error).context("observe Git subprocess exit");
            }
            if unsafe { info.si_pid() } != 0 {
                break;
            }
        }
        self.reap()
    }

    fn reap(&mut self) -> Result<ExitStatus> {
        // Hold the registry lock across kill/reap/remove, preventing cancellation
        // from signalling a PID after wait has released it for reuse.
        let mut state = self
            .cancellation
            .as_ref()
            .map(|c| c.0.lock().unwrap_or_else(|e| e.into_inner()));
        unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };
        let status = self.child.wait().context("wait for Git subprocess");
        if let Some(state) = state.as_mut() {
            state.groups.remove(&self.child.id());
        }
        self.reaped = true;
        status
    }
}

impl Drop for NativeChild {
    fn drop(&mut self) {
        if !self.reaped {
            // Drop also covers an interrupted protocol or a persistent reader.
            // Kill before waiting so abandoned native operations cannot block it.
            let _ = self.reap();
        }
    }
}
