//! Native Git administrative files independent of Unix overlay machinery.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsStr,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Serialize, Deserialize)]
struct WorktreeState {
    version: u32,
    root: PathBuf,
    repository: PathBuf,
    commit: String,
}
fn command(git: &Path, admin: Option<&Path>) -> Result<Command> {
    let mut command = Command::new(git);
    // Explicit admin/worktree paths make setup independent of inherited Git
    // environment. Never let an agent's ambient index redirect these writes.
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_SHALLOW_FILE",
        "GIT_PREFIX",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_GRAFT_FILE",
    ] {
        command.env_remove(name);
    }
    if let Some(admin) = admin {
        command.arg("--git-dir").arg(path_text(admin)?);
    }
    Ok(command)
}
fn run(git: &Path, admin: Option<&Path>, args: &[&OsStr]) -> Result<Vec<u8>> {
    let output = command(git, admin)?
        .args(args)
        .output()
        .context("run native Git")?;
    ensure!(
        output.status.success(),
        "native Git setup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}
fn path_text(path: &Path) -> Result<String> {
    let text = path
        .to_str()
        .context("Git administrative paths must be UTF-8")?;
    ensure!(
        !text.contains(['\n', '\r', '\0']),
        "newline or NUL in administrative path"
    );
    #[cfg(windows)]
    {
        if let Some(unc) = text.strip_prefix("\\\\?\\UNC\\") {
            Ok(format!("//{}", unc.replace('\\', "/")))
        } else {
            Ok(text
                .strip_prefix("\\\\?\\")
                .unwrap_or(text)
                .replace('\\', "/"))
        }
    }
    #[cfg(not(windows))]
    {
        Ok(text.to_owned())
    }
}

/// Create an isolated native Git index/ref namespace without checking out any
/// files. Reopening validates identity and preserves staged edits and new commits.
/// The caller holds the provider's exclusive state lock.
pub fn prepare(
    repository: &Path,
    root: &Path,
    state: &Path,
    commit: &str,
    git: &Path,
) -> Result<()> {
    ensure!(
        commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "expected full SHA1 commit"
    );
    let repository = repository.canonicalize()?;
    let root = root.canonicalize()?;
    let state = state.canonicalize()?;
    ensure!(
        !state.starts_with(&root) && !repository.starts_with(&root),
        "administrative state and source must be outside root"
    );
    let marker = state.join("worktree.json");
    let admin = state.join("git");
    let pointer = format!("gitdir: {}\n", path_text(&admin)?);
    let control = root.join(".git");
    if control.exists() {
        ensure!(
            std::fs::symlink_metadata(&control)?.file_type().is_file(),
            "refusing to replace existing .git"
        );
        ensure!(
            std::fs::read(&control)? == pointer.as_bytes(),
            "existing .git belongs to another workspace"
        );
        ensure!(
            marker.exists(),
            "existing Git control file lacks trusted administrative state"
        );
    }
    if marker.exists() {
        let saved: WorktreeState = serde_json::from_slice(&std::fs::read(&marker)?)?;
        ensure!(
            saved.version == 1
                && saved.repository == repository
                && saved.root == root
                && saved.commit == commit,
            "native Git state belongs to another projection"
        );
        ensure!(
            admin.join("HEAD").is_file() && admin.join("index").is_file(),
            "native Git administrative state is incomplete; refusing to reset it"
        );
    } else {
        ensure!(
            !admin.exists(),
            "incomplete Git setup exists; inspect it before retrying (no automatic index reset)"
        );
        let format = run(
            git,
            Some(&repository),
            &[
                OsStr::new("rev-parse"),
                OsStr::new("--is-bare-repository"),
                OsStr::new("--show-object-format"),
            ],
        )?;
        ensure!(
            format == b"true\nsha1\n",
            "source must be a bare SHA1 repository"
        );
        run(
            git,
            None,
            &[
                OsStr::new("init"),
                OsStr::new("--bare"),
                OsStr::new(&path_text(&admin)?),
            ],
        )?;
        let objects = path_text(&repository.join("objects"))?;
        std::fs::write(
            admin.join("objects/info/alternates"),
            format!("{objects}\n"),
        )?;
        // Alternates share objects, not promisor configuration. Preserve lazy
        // native Git fetch for history commands that need an old blob.
        let origin = command(git, Some(&repository))?
            .args(["config", "--local", "--get", "remote.origin.url"])
            .output()
            .context("read source origin configuration")?;
        ensure!(
            origin.status.success() || origin.status.code() == Some(1),
            "cannot read source origin configuration: {}",
            String::from_utf8_lossy(&origin.stderr)
        );
        if origin.status.success() {
            let url = String::from_utf8(origin.stdout)?
                .trim_end_matches(['\r', '\n'])
                .to_owned();
            ensure!(!url.contains(['\r', '\n', '\0']), "invalid origin URL");
            for (key, value) in [
                ("remote.origin.url", url.as_str()),
                ("remote.origin.promisor", "true"),
                ("remote.origin.partialclonefilter", "blob:none"),
                ("extensions.partialclone", "origin"),
                ("core.repositoryformatversion", "1"),
            ] {
                run(
                    git,
                    Some(&admin),
                    &[OsStr::new("config"), OsStr::new(key), OsStr::new(value)],
                )?;
            }
        }
        for (key, value) in [
            ("core.bare", "false".to_owned()),
            ("core.worktree", path_text(&root)?),
            ("core.autocrlf", "false".to_owned()),
            ("core.longpaths", "true".to_owned()),
        ] {
            run(
                git,
                Some(&admin),
                &[OsStr::new("config"), OsStr::new(key), OsStr::new(&value)],
            )?;
        }
        // read-tree populates index metadata only: it must not hydrate blobs.
        run(
            git,
            Some(&admin),
            &[OsStr::new("read-tree"), OsStr::new(commit)],
        )?;
        run(
            git,
            Some(&admin),
            &[
                OsStr::new("update-ref"),
                OsStr::new("refs/heads/vfs"),
                OsStr::new(commit),
            ],
        )?;
        run(
            git,
            Some(&admin),
            &[
                OsStr::new("symbolic-ref"),
                OsStr::new("HEAD"),
                OsStr::new("refs/heads/vfs"),
            ],
        )?;
        let saved = WorktreeState {
            version: 1,
            root: root.clone(),
            repository,
            commit: commit.to_owned(),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(marker)?;
        file.write_all(&serde_json::to_vec_pretty(&saved)?)?;
        file.sync_all()?;
    }
    if !control.exists() {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(control)?;
        file.write_all(pointer.as_bytes())?;
        file.sync_all()?;
    }
    Ok(())
}
