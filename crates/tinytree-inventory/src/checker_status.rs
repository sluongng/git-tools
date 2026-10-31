//! Operator diagnostics are read-only by default. Explicit recover-once admits
//! one server-owned recovery quantum; it never supplies a verdict.
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tinytree_client::{Config, LandingPhase, LandingStart, NativeClient, Transport};
use tinytree_model::CommitId;

struct Args {
    config: PathBuf,
    job: [u8; 32],
    intent: LandingStart,
    timeout: u64,
    recover_once: bool,
    inspect_recovery: bool,
    expected_attempts: Option<u32>,
    expected_recoveries: Option<u32>,
}
fn id(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid_id".into());
    }
    let mut out = [0; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16).map_err(|_| "invalid_id")?;
    }
    Ok(out)
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|b| format!("{b:02x}")).collect()
}
fn args() -> Result<Args, String> {
    let mut fields = BTreeMap::new();
    let mut input = std::env::args().skip(1);
    let mut recover_once = false;
    let mut inspect_recovery = false;
    while let Some(key) = input.next() {
        if key == "--inspect-recovery" {
            if inspect_recovery {
                return Err("invalid_or_duplicate_argument".into());
            }
            inspect_recovery = true;
            continue;
        }
        if key == "--recover-once" {
            if recover_once {
                return Err("invalid_or_duplicate_argument".into());
            }
            recover_once = true;
            continue;
        }
        if !matches!(
            key.as_str(),
            "--config"
                | "--job"
                | "--request-id"
                | "--source-base"
                | "--source-head"
                | "--destination"
                | "--expected-generation"
                | "--expected-head"
                | "--profile"
                | "--profile-version"
                | "--timeout-seconds"
                | "--expected-attempts"
                | "--expected-recoveries"
        ) {
            return Err("unknown_argument".into());
        }
        let value = input.next().ok_or("missing_argument_value")?;
        if value.is_empty() || value.len() > 4096 || fields.insert(key, value).is_some() {
            return Err("invalid_or_duplicate_argument".into());
        }
    }
    let mut take = |key: &str| {
        fields
            .remove(key)
            .ok_or_else(|| "missing_argument".to_string())
    };
    let config = PathBuf::from(take("--config")?);
    let job = id(&take("--job")?)?;
    let intent = LandingStart {
        idempotency_key: take("--request-id")?,
        source_base: CommitId(id(&take("--source-base")?)?),
        source_head: CommitId(id(&take("--source-head")?)?),
        destination: take("--destination")?,
        expected_generation: take("--expected-generation")?
            .parse()
            .map_err(|_| "invalid_generation")?,
        expected_head: CommitId(id(&take("--expected-head")?)?),
        profile_name: take("--profile")?,
        expected_profile_version: take("--profile-version")?,
    };
    let timeout = fields
        .remove("--timeout-seconds")
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|_| "invalid_timeout")?
        .unwrap_or(10);
    if !(1..=60).contains(&timeout) || job == [0; 32] {
        return Err("invalid_argument".into());
    }
    let expected_attempts = fields
        .remove("--expected-attempts")
        .map(|v| v.parse::<u32>())
        .transpose()
        .map_err(|_| "invalid_attempts")?;
    let expected_recoveries = fields
        .remove("--expected-recoveries")
        .map(|v| v.parse::<u32>())
        .transpose()
        .map_err(|_| "invalid_recoveries")?;
    if (recover_once && inspect_recovery)
        || recover_once != (expected_attempts.is_some() && expected_recoveries.is_some())
        || (!recover_once && (expected_attempts.is_some() || expected_recoveries.is_some()))
    {
        return Err("invalid_recover_once_arguments".into());
    }
    Ok(Args {
        config,
        job,
        intent,
        timeout,
        recover_once,
        inspect_recovery,
        expected_attempts,
        expected_recoveries,
    })
}
fn stage(value: i32) -> Result<&'static str, String> {
    Ok(match value {
        0 => "unspecified",
        1 => "reservation",
        2 => "reservation_directory",
        3 => "authority_refresh",
        4 => "candidate_pin",
        5 => "reservation_persist",
        6 => "queued",
        7 => "preparation",
        8 => "binary_verify",
        9 => "configuration_verify",
        10 => "environment_binding",
        11 => "execution",
        12 => "receipt_verify",
        13 => "verdict_apply",
        _ => return Err("invalid_checker_progress".into()),
    })
}
fn error_code(value: i32) -> Result<&'static str, String> {
    Ok(match value {
        0 => "none",
        1 => "reservation_failed",
        2 => "process_failed",
        3 => "execution_uncertain",
        4 => "receipt_invalid",
        _ => return Err("invalid_checker_progress".into()),
    })
}
fn phase(value: LandingPhase) -> &'static str {
    match value {
        LandingPhase::Intent => "intent",
        LandingPhase::PreparedPendingChecks => "prepared_pending_checks",
        LandingPhase::Checked => "checked",
        LandingPhase::Committed => "committed",
        LandingPhase::Conflict => "conflict",
        LandingPhase::Unsupported => "unsupported",
        LandingPhase::CheckFailed => "check_failed",
        LandingPhase::CheckUnavailable => "check_unavailable",
        LandingPhase::StaleTarget => "stale_target",
        LandingPhase::Expired => "expired",
    }
}
async fn run(args: Args) -> Result<Value, String> {
    let config = Config::load(&args.config).map_err(|_| "configuration_load_failed")?;
    if config.transport != Transport::Grpc {
        return Err("unsupported_transport".into());
    }
    let client = NativeClient::connect(config)
        .await
        .map_err(|s| format!("rpc_{:?}", s.code()).to_ascii_lowercase())?;
    let job = if args.recover_once {
        client
            .recover_landing_once(
                &args.intent,
                args.job,
                args.expected_attempts.unwrap(),
                args.expected_recoveries.unwrap(),
            )
            .await
    } else {
        client
            .lookup_landing(&args.intent, Some(args.job), args.inspect_recovery)
            .await
    }
    .map_err(|s| format!("rpc_{:?}", s.code()).to_ascii_lowercase())?;
    let progress = job.checker_progress().map(|p| -> Result<Value,String> {
        if p.retry_at_seconds < 0 { return Err("invalid_checker_progress".into()); }
        Ok(json!({"stage":stage(p.stage)?,"active_task":p.active_task,"attempts":p.attempts,
            "recoveries":p.recoveries,"retry_at_seconds":p.retry_at_seconds,
            "saved_phase":match p.saved_phase {0=>"unspecified",1=>"reserved",2=>"dispatch_intent",3=>"uncertain",4=>"verified",5=>"retry_ready",6=>"terminal",7=>"unavailable_ready",_=>return Err("invalid_checker_progress".into())},
            "automatic_recovery_blocked":p.automatic_recovery_blocked,"error_code":error_code(p.error_code)?,"error_stage":stage(p.error_stage)?}))
    }).transpose()?;
    let inspection = job.recovery_inspection().map(|p| -> Result<Value,String> {
        let transport = ["unspecified","missing","invalid","start_unconfirmed","quiescent_eligible","quiescent_ineligible"];
        let stdout = ["unspecified","missing","empty","receipt_valid","invalid","oversized"];
        let runner = ["not_inspected","config_invalid","scratch_missing","attempt_missing","journal_missing","journal_invalid","binding_mismatch","environment_missing","tools_incomplete","receipt_saved","readiness_present","unavailable"];
        Ok(json!({"transport":transport.get(p.transport as usize).ok_or("invalid_recovery_inspection")?,"stdout":stdout.get(p.stdout as usize).ok_or("invalid_recovery_inspection")?,"stderr_present":p.stderr_present,"runner":runner.get(p.runner as usize).ok_or("invalid_recovery_inspection")?}))
    }).transpose()?;
    let candidate = job
        .candidate()
        .map(|c| json!({"head":hex(&c.head.0),"root_tree":hex(&c.root_tree.0)}));
    Ok(
        json!({"schema_version":1,"mode":if args.recover_once {"recover_once_admission"} else {"read_only"},"job_id":hex(&job.handle().id()),"phase":phase(job.phase()),"candidate":candidate,"checker_progress":progress,"recovery_inspection":inspection}),
    )
}
#[tokio::main]
async fn main() {
    let result = match args() {
        Ok(args) => {
            match tokio::time::timeout(Duration::from_secs(args.timeout), run(args)).await {
                Ok(result) => result,
                Err(_) => Err("deadline_exceeded".into()),
            }
        }
        Err(error) => Err(error),
    };
    let failed = result.is_err();
    let value = result.unwrap_or_else(|code| json!({"schema_version":1,"error":{"code":code}}));
    println!("{}", value);
    if failed {
        std::process::exit(1);
    }
}
