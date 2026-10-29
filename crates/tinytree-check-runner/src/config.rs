use crate::{hex_id, name};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
    path::{Component, Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binary {
    pub path: PathBuf,
    pub sha256: String,
}
impl Binary {
    pub fn verify(&self) -> Result<()> {
        absolute(&self.path)?;
        ensure!(hex_id(&self.sha256), "invalid executable checksum");
        let file = std::fs::File::open(&self.path)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file() && metadata.len() <= 1024 * 1024 * 1024,
            "invalid executable size/type"
        );
        let mut hash = Sha256::new();
        let mut reader = file.take(1024 * 1024 * 1024 + 1);
        let mut bytes = [0; 65536];
        loop {
            let n = reader.read(&mut bytes)?;
            if n == 0 {
                break;
            }
            hash.update(&bytes[..n]);
        }
        ensure!(
            format!("{:x}", hash.finalize()) == self.sha256,
            "executable checksum differs"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Build,
    Test,
}
impl Mode {
    pub(crate) fn command(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Test => "test",
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub name: String,
    pub repositories: Vec<String>,
    /// Permit authenticated service-selected provisioned Forge account repositories.
    #[serde(default, skip_serializing_if = "is_false")]
    pub forge_accounts: bool,
    pub store: String,
    pub mode: Mode,
    /// Exact labels, not patterns. Test mode requires a testSummary for each.
    pub targets: Vec<String>,
    #[serde(default)]
    pub flags: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub force_remote_execution: bool,
}
fn default_timeout() -> u64 {
    1800
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub scratch: PathBuf,
    pub daemon_config: PathBuf,
    pub rbe_socket: PathBuf,
    pub runner_sha256: String,
    #[serde(default)]
    pub remote_instance_name: String,
    pub workspace: Binary,
    pub bazel: Binary,
    #[serde(default)]
    pub tools: BTreeMap<String, Binary>,
    pub profiles: Vec<Profile>,
    /// Only workspace management receives these names. Values are never in a
    /// receipt or fingerprint. The Bazel process gets a fresh environment.
    #[serde(default)]
    pub workspace_environment: Vec<String>,
    #[serde(default = "cleanup_timeout")]
    pub cleanup_timeout_seconds: u64,
    #[serde(default = "remote_timeout")]
    pub remote_timeout_seconds: u64,
    #[serde(default = "max_log")]
    pub max_log_bytes: u64,
    #[serde(default = "max_bep")]
    pub max_bep_bytes: u64,
    #[serde(default = "max_execution")]
    pub max_execution_log_bytes: u64,
    #[serde(default = "max_attempts")]
    pub max_attempts: usize,
}
fn cleanup_timeout() -> u64 {
    60
}
fn remote_timeout() -> u64 {
    600
}
fn max_log() -> u64 {
    16 * 1024 * 1024
}
fn max_bep() -> u64 {
    64 * 1024 * 1024
}
fn max_execution() -> u64 {
    128 * 1024 * 1024
}
fn max_attempts() -> usize {
    1024
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = read_config(path)?;
        let config: Self = toml::from_str(std::str::from_utf8(&bytes)?)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        for path in [
            &self.scratch,
            &self.daemon_config,
            &self.rbe_socket,
            &self.workspace.path,
            &self.bazel.path,
        ] {
            absolute(path)?;
        }
        ensure!(
            hex_id(&self.workspace.sha256)
                && hex_id(&self.bazel.sha256)
                && hex_id(&self.runner_sha256),
            "invalid binary pin"
        );
        ensure!(
            (1..=128).contains(&self.profiles.len())
                && (1..=300).contains(&self.cleanup_timeout_seconds)
                && (1..=3600).contains(&self.remote_timeout_seconds)
                && (1..=4096).contains(&self.max_attempts),
            "invalid checker limits"
        );
        ensure!(
            (1024..=64 * 1024 * 1024).contains(&self.max_log_bytes)
                && (1024..=256 * 1024 * 1024).contains(&self.max_bep_bytes)
                && (1024..=512 * 1024 * 1024).contains(&self.max_execution_log_bytes),
            "invalid checker byte limits"
        );
        ensure!(
            self.workspace_environment.len() <= 32
                && self.workspace_environment.iter().all(|v| name(v)
                    && !v.as_bytes()[0].is_ascii_digit()
                    && !v.contains(['-', '.'])),
            "invalid environment allowlist"
        );
        ensure!(
            self.workspace_environment
                .iter()
                .all(|v| !v.starts_with("LD_")
                    && !v.starts_with("DYLD_")
                    && !matches!(
                        v.as_str(),
                        "PATH" | "HOME" | "TMPDIR" | "XDG_CONFIG_HOME" | "BASH_ENV" | "ENV"
                    )),
            "management environment may not override execution isolation"
        );
        ensure!(
            self.remote_instance_name.len() <= 512
                && !self
                    .remote_instance_name
                    .bytes()
                    .any(|b| b.is_ascii_control()),
            "invalid remote instance name"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= 65536,
            "check configuration exceeds serialized budget"
        );
        ensure!(self.tools.len() <= 16, "too many profile tools");
        for (name, binary) in &self.tools {
            ensure!(
                crate::name(name)
                    && !matches!(name.as_str(), "workspace" | "bazel")
                    && hex_id(&binary.sha256),
                "invalid profile tool"
            );
            absolute(&binary.path)?;
        }
        let mut names = HashSet::new();
        for p in &self.profiles {
            ensure!(
                name(&p.name) && name(&p.store) && names.insert(&p.name),
                "invalid checker profile"
            );
            ensure!(
                (!p.repositories.is_empty() || p.forge_accounts)
                    && p.repositories.len() <= 128
                    && p.repositories.iter().all(|r| name(r)),
                "invalid profile repository scope"
            );
            ensure!(
                (1..=128).contains(&p.targets.len())
                    && p.targets.iter().all(|t| target(t))
                    && p.targets.iter().map(String::len).sum::<usize>() <= 8192,
                "profile requires exact target labels"
            );
            ensure!(
                p.targets.iter().collect::<HashSet<_>>().len() == p.targets.len(),
                "duplicate check target"
            );
            for flag in &p.flags {
                if let Some((prefix, rest)) = flag.split_once("@runner_tool(") {
                    let tool = rest
                        .strip_suffix(')')
                        .ok_or_else(|| anyhow::anyhow!("invalid tool placeholder"))?;
                    ensure!(
                        prefix.ends_with('=') && self.tools.contains_key(tool),
                        "unknown profile tool"
                    );
                }
            }
            ensure!(
                (1..=86400).contains(&p.timeout_seconds)
                    && p.flags.len() <= 128
                    && p.flags.iter().all(|v| allowed_flag(v)),
                "invalid check command policy"
            );
        }
        Ok(())
    }
    pub fn profile(&self, name: &str) -> Result<&Profile> {
        self.profiles
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| anyhow::anyhow!("check profile unavailable"))
    }
    /// Changes to binary pins, management configuration, scope, targets or
    /// command policy require a new frozen server profile version.
    pub fn profile_version(&self, name: &str) -> Result<String> {
        self.validate()?;
        let daemon = read_config(&self.daemon_config)?;
        self.profile_version_for_daemon(name, &daemon)
    }
    pub(crate) fn profile_version_for_daemon(&self, name: &str, daemon: &[u8]) -> Result<String> {
        let profile = self.profile(name)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-check-profile-v1\0");
        hash.update(&serde_json::to_vec(&(self, profile))?);
        hash.update(blake3::hash(daemon).as_bytes());
        Ok(hash.finalize().to_hex().to_string())
    }
}
pub(crate) fn read_config(path: &Path) -> Result<Vec<u8>> {
    absolute(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration parent is missing"))?;
    let directory = private_fs::PrivateDir::open_existing(parent)?;
    Ok(directory.read(
        path.file_name()
            .ok_or_else(|| anyhow::anyhow!("configuration name is missing"))?,
        65536,
    )?)
}
pub(crate) fn absolute(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute()
            && path.as_os_str().len() <= 4096
            && !path
                .components()
                .any(|c| matches!(c, Component::ParentDir | Component::CurDir)),
        "checker paths must be absolute and normalized"
    );
    Ok(())
}
pub(crate) fn target(value: &str) -> bool {
    value.starts_with("//")
        && value.contains(':')
        && value.len() <= 1024
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/_-.:+".contains(&b))
        && !value.contains("..")
        && !value.ends_with(":all")
        && !value.ends_with(":all-targets")
        && !value.ends_with(":*")
}
fn allowed_flag(value: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "--extra_toolchains=",
        "--extra_execution_platforms=",
        "--host_platform=",
        "--platforms=",
        "--check_direct_dependencies=",
        "--repo_env=",
        "--test_env=",
        "--copt=",
        "--cxxopt=",
        "--linkopt=",
        "--define=",
        "--features=",
        "--jobs=",
    ];
    value.len() <= 4096
        && !value.bytes().any(|b| b == 0 || b == b'\n' || b == b'\r')
        && (PREFIXES.iter().any(|p| value.starts_with(p))
            || matches!(
                value,
                "--compilation_mode=opt"
                    | "--compilation_mode=dbg"
                    | "--compilation_mode=fastbuild"
            )
            || setting_assignment(value))
}
fn setting_assignment(value: &str) -> bool {
    let Some((label, assigned)) = value.strip_prefix("--").and_then(|v| v.split_once('=')) else {
        return false;
    };
    if assigned.is_empty() {
        return false;
    }
    let label = if let Some(external) = label.strip_prefix('@') {
        let Some(end) = external.find("//") else {
            return false;
        };
        let repository = &external[..end];
        if repository.is_empty()
            || repository.len() > 128
            || !repository
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b))
        {
            return false;
        }
        &external[end..]
    } else {
        label
    };
    if !target(label) {
        return false;
    }
    let Some((package, setting)) = label[2..].split_once(':') else {
        return false;
    };
    let components = |path: &str| path.split('/').all(|part| !part.is_empty() && part != ".");
    (package.is_empty() || components(package))
        && !setting.is_empty()
        && !setting.contains(':')
        && components(setting)
}

fn is_false(value: &bool) -> bool {
    !*value
}
