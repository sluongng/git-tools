//! Explicit noninteractive helper policy. Credentials are never configuration.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

/// One trusted Git credential protocol executable. Arguments must be public
/// constants, never credentials. Environment values are acquired only from the
/// explicitly named role environment and are never serialized or policy-hashed.
/// Keep the helper and its dependencies immutable while the watcher is running;
/// pinning is not a defense against hostile same-UID replacement after checking.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialHelper {
    pub program: PathBuf,
    pub program_blake3: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    #[serde(default)]
    pub environment_names: Vec<String>,
}
impl CredentialHelper {
    /// Read a bounded, trusted, nonsecret JSON policy file. Never put token
    /// values in this file; use a private auth store or an explicit role variable.
    pub fn load(path: &Path) -> Result<Self> {
        let file = trusted_file(path, 65536, false)?;
        let mut data = Vec::new();
        file.take(65537).read_to_end(&mut data)?;
        ensure!(data.len() <= 65536, "credential helper policy too large");
        let helper: Self = serde_json::from_slice(&data)
            .map_err(|_| anyhow::anyhow!("invalid credential helper policy"))?;
        helper.validate()?;
        Ok(helper)
    }
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.arguments.len() <= 32 && self.environment_names.len() <= 32,
            "credential helper policy bound exceeded"
        );
        ensure!(
            self.arguments
                .iter()
                .all(|v| v.len() <= 4096 && !v.bytes().any(|b| b < 32 || b == 127)),
            "invalid public helper argument"
        );
        let mut names = std::collections::BTreeSet::new();
        for name in &self.environment_names {
            ensure!(
                valid_environment_name(name) && names.insert(name),
                "invalid or reserved helper environment name"
            );
        }
        ensure!(
            self.arguments.iter().map(String::len).sum::<usize>() <= 16384,
            "helper public argument byte bound exceeded"
        );
        self.verify_program()
    }
    pub(super) fn verify_program(&self) -> Result<()> {
        self.verify_program_checked(|| Ok(()))
    }
    fn verify_program_checked(&self, mut checkpoint: impl FnMut() -> Result<()>) -> Result<()> {
        checkpoint()?;
        ensure!(
            self.program_blake3.len() == 64
                && self
                    .program_blake3
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid helper program pin"
        );
        let mut file = trusted_file(&self.program, 256 * 1024 * 1024, true)?;
        let before = file.metadata()?;
        let mut hash = blake3::Hasher::new();
        let mut size = 0;
        let mut buffer = [0u8; 65536];
        loop {
            checkpoint()?;
            let n = file.read(&mut buffer)?;
            checkpoint()?;
            if n == 0 {
                break;
            }
            size += n;
            ensure!(
                size <= 256 * 1024 * 1024,
                "helper program exceeds byte bound"
            );
            hash.update(&buffer[..n]);
        }
        let after = file.metadata()?;
        let named = std::fs::symlink_metadata(&self.program)
            .map_err(|_| anyhow::anyhow!("helper program path changed"))?;
        ensure!(
            (after.dev(), after.ino(), after.mode(), after.size())
                == (named.dev(), named.ino(), named.mode(), named.size()),
            "helper program path changed"
        );
        ensure!(
            (
                before.dev(),
                before.ino(),
                before.size(),
                before.mtime(),
                before.mtime_nsec()
            ) == (
                after.dev(),
                after.ino(),
                after.size(),
                after.mtime(),
                after.mtime_nsec()
            ) && hash.finalize().to_hex().as_str() == self.program_blake3,
            "credential helper program pin mismatch"
        );
        Ok(())
    }
    pub(super) fn policy_bytes(&self) -> Result<Vec<u8>> {
        // Only paths, public arguments, a public program hash and variable NAMES.
        Ok(serde_json::to_vec(self)?)
    }
    pub(super) fn capture(&self) -> Result<CapturedHelper> {
        self.validate()?;
        let mut environment = BTreeMap::new();
        let mut total = 0;
        for name in &self.environment_names {
            let value = std::env::var_os(name)
                .ok_or_else(|| anyhow::anyhow!("explicit helper environment unavailable"))?;
            total += value.as_encoded_bytes().len();
            ensure!(
                total <= 65536 && !value.as_encoded_bytes().contains(&0),
                "helper environment byte bound exceeded"
            );
            environment.insert(name.clone(), value);
        }
        Ok(CapturedHelper {
            policy: self.clone(),
            environment,
        })
    }
}
// Deliberately no Debug/Serialize: values may contain credentials.
pub(super) struct CapturedHelper {
    pub policy: CredentialHelper,
    pub environment: BTreeMap<String, OsString>,
}
impl CredentialHelper {
    pub(super) fn command_until(
        &self,
        deadline: tokio::time::Instant,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<String> {
        self.verify_program_checked(|| {
            ensure!(!*cancel.borrow(), "push cancelled before admission");
            ensure!(
                tokio::time::Instant::now() < deadline,
                "push operation deadline exceeded"
            );
            Ok(())
        })?;
        let program = self
            .program
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("helper program path must be UTF-8"))?;
        // Git's ! form avoids implicit "git credential-" command rewriting.
        // Every word is literal POSIX single-quoted data, not configured shell.
        let mut command = format!("!{}", shell_word(program));
        for argument in &self.arguments {
            command.push(' ');
            command.push_str(&shell_word(argument));
        }
        Ok(command)
    }
}
fn shell_word(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
fn valid_environment_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        && !name.as_bytes()[0].is_ascii_digit()
        && ![
            "GIT_", "LD_", "DYLD_", "PYTHON", "BASH_", "SSH_", "CURL_", "SSL_", "PERL", "RUBY",
            "NODE_", "LUA_", "TCL", "PHP", "JAVA_", "JDK_", "DOTNET_", "MONO_", "ERL_", "RUST",
            "CARGO_", "LC_",
        ]
        .iter()
        .any(|p| name.starts_with(p))
        && ![
            "PATH",
            "HOME",
            "TMPDIR",
            "TMP",
            "TEMP",
            "ENV",
            "SHELLOPTS",
            "BASHOPTS",
            "ZDOTDIR",
            "GCONV_PATH",
            "LOCPATH",
            "NLSPATH",
            "GLIBC_TUNABLES",
            "LANG",
            "LANGUAGE",
            "CLASSPATH",
            "_JAVA_OPTIONS",
            "CDPATH",
            "IFS",
            "ALL_PROXY",
            "HTTPS_PROXY",
            "HTTP_PROXY",
            "NO_PROXY",
            "XDG_CONFIG_HOME",
            "XDG_CONFIG_DIRS",
            "LC_ALL",
            "TZ",
        ]
        .contains(&name)
}
fn trusted_file(path: &Path, maximum: u64, executable: bool) -> Result<std::fs::File> {
    ensure!(
        path.is_absolute()
            && path.as_os_str().as_encoded_bytes().len() <= 4096
            && !path
                .as_os_str()
                .as_encoded_bytes()
                .iter()
                .any(|b| *b < 32 || *b == 127)
            && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
            && path
                .canonicalize()
                .map_err(|_| anyhow::anyhow!("helper path unavailable"))?
                == path,
        "helper path must be canonical and absolute"
    );
    for parent in path.ancestors().skip(1) {
        let m = std::fs::symlink_metadata(parent)
            .map_err(|_| anyhow::anyhow!("helper parent unavailable"))?;
        ensure!(
            m.is_dir()
                && (m.uid() == 0 || m.uid() == unsafe { libc::geteuid() })
                && (m.mode() & 0o022 == 0 || (m.uid() == 0 && m.mode() & 0o1000 != 0)),
            "untrusted helper parent metadata"
        );
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| anyhow::anyhow!("helper file unavailable"))?;
    let m = file.metadata()?;
    ensure!(
        m.is_file()
            && m.nlink() == 1
            && m.size() <= maximum
            && (m.uid() == 0 || m.uid() == unsafe { libc::geteuid() })
            && m.mode() & 0o022 == 0
            && (!executable || m.mode() & 0o111 != 0),
        "untrusted helper file metadata"
    );
    Ok(file)
}
