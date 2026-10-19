//! Windows job objects contain Git and every descendant before it starts running.
use std::collections::HashMap;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, Command, ExitStatus};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, ensure};
use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, WAIT_OBJECT_0};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, INFINITE, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
    WaitForSingleObject,
};

#[derive(Default)]
struct State {
    cancelled: bool,
    jobs: HashMap<u32, Arc<OwnedHandle>>,
}

/// Cancels this worker's complete Git process trees, including transport helpers.
#[derive(Clone, Default)]
pub struct BackgroundCancellation(Arc<Mutex<State>>);
impl BackgroundCancellation {
    pub fn cancel(&self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.cancelled = true;
        for job in state.jobs.values() {
            // Handles remain owned by the registry, so cannot be recycled here.
            unsafe { TerminateJobObject(job.as_raw_handle(), 1) };
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancelled
    }
    pub(crate) fn check(&self) -> Result<()> {
        ensure!(!self.is_cancelled(), "background prefetch cancelled");
        Ok(())
    }
}

pub(crate) struct NativeChild {
    pub(crate) child: Child,
    job: Arc<OwnedHandle>,
    cancellation: Option<BackgroundCancellation>,
    reaped: bool,
}
impl NativeChild {
    pub(crate) fn spawn(
        command: &mut Command,
        cancellation: Option<&BackgroundCancellation>,
    ) -> Result<Self> {
        let mut state = cancellation.map(|c| c.0.lock().unwrap_or_else(|e| e.into_inner()));
        ensure!(
            !state.as_ref().is_some_and(|s| s.cancelled),
            "background prefetch cancelled"
        );
        let raw_job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        ensure!(
            !raw_job.is_null(),
            "create Git process job: {}",
            std::io::Error::last_os_error()
        );
        let job = Arc::new(unsafe { OwnedHandle::from_raw_handle(raw_job) });
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        ensure!(
            unsafe {
                SetInformationJobObject(
                    raw_job,
                    JobObjectExtendedLimitInformation,
                    &limits as *const _ as *const _,
                    std::mem::size_of_val(&limits) as u32,
                )
            } != 0,
            "configure Git process job: {}",
            std::io::Error::last_os_error()
        );
        // The child cannot execute user code (or start escaping descendants)
        // until it has been attached to our non-breakaway kill-on-close job.
        command.creation_flags(CREATE_SUSPENDED);
        let mut child = command.spawn().context("start suspended Git subprocess")?;
        let started = (|| {
            ensure!(
                unsafe { AssignProcessToJobObject(raw_job, child.as_raw_handle()) } != 0,
                "assign Git process job: {}",
                std::io::Error::last_os_error()
            );
            resume_child(child.id())
        })();
        if let Err(error) = started {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        if let Some(state) = state.as_mut() {
            state.jobs.insert(child.id(), job.clone());
        }
        Ok(Self {
            child,
            job,
            cancellation: cancellation.cloned(),
            reaped: false,
        })
    }
    pub(crate) fn finish(&mut self) -> Result<ExitStatus> {
        ensure!(
            unsafe { WaitForSingleObject(self.child.as_raw_handle(), INFINITE) } == WAIT_OBJECT_0,
            "observe Git subprocess exit: {}",
            std::io::Error::last_os_error()
        );
        self.reap()
    }
    fn reap(&mut self) -> Result<ExitStatus> {
        let mut state = self
            .cancellation
            .as_ref()
            .map(|c| c.0.lock().unwrap_or_else(|e| e.into_inner()));
        // Even after Git exits, a transport helper might still be alive.
        unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) };
        let status = self.child.wait().context("wait for Git subprocess");
        if let Some(state) = state.as_mut() {
            state.jobs.remove(&self.child.id());
        }
        self.reaped = true;
        status
    }
}
impl Drop for NativeChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.reap();
        }
    }
}

fn resume_child(pid: u32) -> Result<()> {
    let raw_snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    ensure!(
        raw_snapshot != INVALID_HANDLE_VALUE,
        "enumerate suspended Git thread: {}",
        std::io::Error::last_os_error()
    );
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw_snapshot) };
    let mut entry = THREADENTRY32 {
        dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
        ..Default::default()
    };
    let mut present = unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) };
    while present != 0 {
        if entry.th32OwnerProcessID == pid {
            let raw_thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            ensure!(
                !raw_thread.is_null(),
                "open suspended Git thread: {}",
                std::io::Error::last_os_error()
            );
            let thread = unsafe { OwnedHandle::from_raw_handle(raw_thread) };
            ensure!(
                unsafe { ResumeThread(thread.as_raw_handle()) } != u32::MAX,
                "resume Git thread: {}",
                std::io::Error::last_os_error()
            );
            return Ok(());
        }
        present = unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) };
    }
    anyhow::bail!("suspended Git process has no primary thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;
    use std::time::{Duration, Instant};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_SYNCHRONIZE};

    #[test]
    fn cancellation_terminates_descendants_and_prevents_later_spawns() {
        let cancel = BackgroundCancellation::default();
        let mut command = Command::new("powershell.exe");
        command.args(["-NoProfile", "-NonInteractive", "-Command",
            "$p = Start-Process -FilePath $env:ComSpec -ArgumentList '/c ping -n 60 127.0.0.1 >nul' -PassThru -WindowStyle Hidden; [Console]::WriteLine($p.Id); Start-Sleep -Seconds 60"])
            .stdout(Stdio::piped()).stderr(Stdio::null());
        let mut native = NativeChild::spawn(&mut command, Some(&cancel)).unwrap();
        let stdout = native.child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = tx.send(result);
        });
        let line = rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();
        let descendant: u32 = line.trim().parse().unwrap();
        let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, descendant) };
        assert!(
            !raw.is_null(),
            "descendant must be running before cancellation"
        );
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        let started = Instant::now();
        cancel.cancel();
        assert_eq!(
            unsafe { WaitForSingleObject(native.child.as_raw_handle(), 5_000) },
            WAIT_OBJECT_0
        );
        assert!(!native.finish().unwrap().success());
        assert_eq!(
            unsafe { WaitForSingleObject(handle.as_raw_handle(), 5_000) },
            WAIT_OBJECT_0
        );
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(NativeChild::spawn(&mut Command::new("cmd.exe"), Some(&cancel)).is_err());
    }

    #[test]
    fn successful_exit_is_preserved_and_drop_reaps_unfinished_child() {
        let mut command = Command::new("cmd.exe");
        command.args(["/c", "exit", "0"]);
        let mut native = NativeChild::spawn(&mut command, None).unwrap();
        assert!(native.finish().unwrap().success());
        let mut command = Command::new("cmd.exe");
        command.args(["/c", "ping -n 60 127.0.0.1 >nul"]);
        let native = NativeChild::spawn(&mut command, None).unwrap();
        let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, native.child.id()) };
        assert!(!raw.is_null());
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        drop(native);
        assert_eq!(
            unsafe { WaitForSingleObject(handle.as_raw_handle(), 5_000) },
            WAIT_OBJECT_0
        );
    }
}
