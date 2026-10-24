use crate::{Evidence, Mode, Profile};
use anyhow::{Result, ensure};
use serde_json::Value;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read},
    path::Path,
};

pub(crate) struct Completion {
    pub evidence: Evidence,
    pub success: bool,
}
pub(crate) fn digest(path: &Path, limit: u64) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() <= limit,
        "evidence file exceeds limit"
    );
    let mut hash = blake3::Hasher::new();
    let mut total = 0u64;
    let mut bytes = [0; 65536];
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        ensure!(total <= limit, "evidence file grew beyond limit");
        hash.update(&bytes[..n]);
    }
    Ok(hash.finalize().to_hex().to_string())
}
pub(crate) fn verify(
    path: &Path,
    compact: &Path,
    profile: &Profile,
    exit: i32,
    max_bep: u64,
    max_compact: u64,
) -> Result<Completion> {
    let bep_blake3 = digest(path, max_bep)?;

    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let mut started = None;
    let mut finished = None;
    let mut targets: HashMap<&str, bool> = HashMap::new();
    let mut tests: HashMap<&str, bool> = HashMap::new();
    let mut line = Vec::new();
    let mut total = 0u64;
    let mut events = 0;
    loop {
        line.clear();
        let n = reader
            .by_ref()
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        events += 1;
        ensure!(
            n <= 1024 * 1024 && total <= max_bep && events <= 100_000,
            "BEP event budget exceeded"
        );
        let value: Value = serde_json::from_slice(&line)?;
        let id = &value["id"];
        if id.get("started").is_some() {
            ensure!(started.is_none(), "duplicate build start");
            let uuid = value["started"]["uuid"].as_str().unwrap_or_default();
            ensure!(
                !uuid.is_empty()
                    && uuid.len() <= 128
                    && uuid.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'),
                "missing invocation identity"
            );
            started = Some(uuid.to_owned());
            ensure!(
                value["started"]["command"].as_str() == Some(profile.mode.command()),
                "BEP command differs from profile"
            );
        }
        if id.get("buildFinished").is_some() {
            ensure!(finished.is_none(), "duplicate build completion");
            let code = value["finished"]["exitCode"]["code"].as_i64().unwrap_or(0);
            ensure!(
                code == i64::from(exit),
                "BEP exit differs from process status"
            );
            finished = Some(
                value["finished"]["overallSuccess"]
                    .as_bool()
                    .unwrap_or(false),
            );
        }
        if let Some(label) = id["targetCompleted"]["label"].as_str() {
            if !id["targetCompleted"]["aspect"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
            {
                continue;
            }
            if let Some(target) = profile
                .targets
                .iter()
                .find(|target| target.as_str() == label)
            {
                ensure!(
                    targets
                        .insert(
                            target,
                            value["completed"]["success"].as_bool().unwrap_or(false)
                        )
                        .is_none(),
                    "duplicate configured target completion"
                );
            }
        }
        if let Some(label) = id["testSummary"]["label"].as_str() {
            if let Some(target) = profile
                .targets
                .iter()
                .find(|target| target.as_str() == label)
            {
                ensure!(
                    tests
                        .insert(
                            target,
                            value["testSummary"]["overallStatus"].as_str() == Some("PASSED")
                                && value["testSummary"]["totalRunCount"].as_u64().unwrap_or(0) > 0
                        )
                        .is_none(),
                    "duplicate configured test summary"
                );
            }
        }
    }
    let finished = finished.ok_or_else(|| anyhow::anyhow!("build completion is missing"))?;
    let invocation_id = started.ok_or_else(|| anyhow::anyhow!("build start is missing"))?;
    let tests_verified = profile.mode == Mode::Test
        && profile
            .targets
            .iter()
            .all(|t| tests.get(t.as_str()) == Some(&true));
    let all_targets = profile
        .targets
        .iter()
        .all(|t| targets.get(t.as_str()) == Some(&true));
    let success =
        exit == 0 && finished && all_targets && (profile.mode == Mode::Build || tests_verified);
    // A success process with incomplete/contradictory evidence is infrastructure
    // failure, never a negative verdict attributed to the source candidate.
    ensure!(
        exit != 0 || success,
        "successful process has incomplete check evidence"
    );
    ensure!(
        exit == 0 || (!finished && matches!(exit, 1 | 3)),
        "non-check process failure"
    );
    let execution_log_blake3 = match std::fs::metadata(compact) {
        Ok(_) => Some(digest(compact, max_compact)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !success && exit != 0 => None,
        Err(e) => return Err(e.into()),
    };
    Ok(Completion {
        evidence: Evidence {
            invocation_id,
            bep_blake3,
            execution_log_blake3,
            requested_targets: profile.targets.clone(),
            tests_verified,
            bazel_exit_code: exit,
        },
        success,
    })
}
