//! Process-local startup headroom, separate from per-resource admission bounds.
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LimitWarning {
    QueryFailed,
    RaiseFailed,
    HardLimitBelowTarget,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct StartupLimits {
    pub requested_target: u64,
    pub initial_soft: Option<u64>,
    pub hard: Option<u64>,
    pub effective_soft: Option<u64>,
    pub attempted_raise: bool,
    pub warning: Option<LimitWarning>,
    pub raw_os_error: Option<i32>,
}

fn desired_soft(initial: u64, hard: u64, target: u64) -> u64 {
    if target == 0 {
        initial
    } else {
        initial.max(target.min(hard))
    }
}

impl StartupLimits {
    /// Call once after acquiring daemon ownership, before opening service stores.
    /// This changes only this process's soft limit, never its hard limit or a
    /// different process. Existing higher soft limits are preserved.
    pub(crate) fn apply(target: u64) -> Self {
        let mut snapshot = Self {
            requested_target: target,
            initial_soft: None,
            hard: None,
            effective_soft: None,
            attempted_raise: false,
            warning: None,
            raw_os_error: None,
        };
        let mut limits = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // getrlimit writes the initialized, correctly sized rlimit structure.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } != 0 {
            snapshot.warning = Some(LimitWarning::QueryFailed);
            snapshot.raw_os_error = std::io::Error::last_os_error().raw_os_error();
            snapshot.warn();
            return snapshot;
        }
        let initial = limits.rlim_cur as u64;
        let hard = limits.rlim_max as u64;
        snapshot.initial_soft = Some(initial);
        snapshot.hard = Some(hard);
        snapshot.effective_soft = Some(initial);
        if target > hard {
            snapshot.warning = Some(LimitWarning::HardLimitBelowTarget);
        }
        let desired = desired_soft(initial, hard, target);
        if desired > initial {
            snapshot.attempted_raise = true;
            let requested = libc::rlimit {
                // Config validation bounds target; desired is no larger than
                // the inherited hard limit and never lowers the current soft.
                rlim_cur: desired as libc::rlim_t,
                rlim_max: limits.rlim_max,
            };
            // setrlimit receives a valid structure; the hard limit is unchanged.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &requested) } != 0 {
                snapshot.warning = Some(LimitWarning::RaiseFailed);
                snapshot.raw_os_error = std::io::Error::last_os_error().raw_os_error();
            } else if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } == 0 {
                snapshot.effective_soft = Some(limits.rlim_cur as u64);
            } else {
                snapshot.effective_soft = None;
                snapshot.warning = Some(LimitWarning::QueryFailed);
                snapshot.raw_os_error = std::io::Error::last_os_error().raw_os_error();
            }
        }
        if snapshot.warning.is_some() {
            snapshot.warn();
        }
        snapshot
    }

    fn warn(&self) {
        // Fixed fields only: no error chains, paths, credentials or endpoints.
        eprintln!(
            "daemon open-file headroom warning: {:?}; target={}, effective={:?}, hard={:?}, errno={:?}",
            self.warning, self.requested_target, self.effective_soft, self.hard, self.raw_os_error
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_headroom_never_lowers_soft_or_changes_hard_capacity() {
        for (name, initial, hard, target, expected) in [
            ("inherit", 1024, 1_048_576, 0, 1024),
            ("raise to target", 1024, 1_048_576, 16_384, 16_384),
            (
                "preserve higher inherited soft",
                65_536,
                1_048_576,
                16_384,
                65_536,
            ),
            ("clamp to inherited hard", 1024, 4096, 16_384, 4096),
            ("already at hard", 4096, 4096, 16_384, 4096),
        ] {
            assert_eq!(desired_soft(initial, hard, target), expected, "{name}");
        }
    }
}
