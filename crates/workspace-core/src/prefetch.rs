//! Bounded Bazel metadata, symlink, and literal repository-loading input prefetch.
//! This is not Starlark evaluation: inactive literal calls may be overfetched, while
//! computed attributes, unsupported escapes, external repositories, arbitrary file
//! attributes, and symlink referents remain lazy. RC paths are workspace-relative.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::Serialize;

use crate::backend::{BlobPrefetchReport, GitRepo};
use crate::filesystem::{PublicEntry, Snapshot, display_git_path};

#[derive(Debug, Serialize)]
pub struct BazelPrefetchReport {
    pub selected_paths: Vec<String>,
    pub blobs: BlobPrefetchReport,
    pub path_reasons: BTreeMap<String, Vec<String>>,
    pub discovery_waves: usize,
    pub skipped_discovery: Vec<String>,
}

/// Selection policy independent of Git transport. Empty roots mean the whole
/// snapshot; roots restrict metadata and symlinks, not all file contents.
#[derive(Debug)]
pub struct BazelPrefetchPolicy {
    roots: Vec<PathBuf>,
    includes: Vec<PathBuf>,
    discovery: bool,
}
impl Default for BazelPrefetchPolicy {
    fn default() -> Self {
        Self {
            roots: vec![],
            includes: vec![],
            discovery: true,
        }
    }
}
fn normalize(path: &Path) -> Result<PathBuf> {
    ensure!(
        !path.as_os_str().is_empty(),
        "prefetch path must not be empty"
    );
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => normalized.push(name),
            Component::CurDir => {}
            _ => anyhow::bail!("prefetch paths must be repository-relative without '..' traversal"),
        }
    }
    Ok(normalized)
}

impl BazelPrefetchPolicy {
    pub fn new(roots: &[PathBuf]) -> Result<Self> {
        let mut normalized = Vec::new();
        for root in roots {
            let path = normalize(root)?;
            if !normalized.contains(&path) {
                normalized.push(path);
            }
        }
        Ok(Self {
            roots: normalized,
            ..Self::default()
        })
    }

    /// Missing includes are rejected against the pinned snapshot before fetching.
    pub fn with_includes(mut self, includes: &[PathBuf]) -> Result<Self> {
        for include in includes {
            let path = normalize(include)?;
            if !self.includes.contains(&path) {
                self.includes.push(path);
            }
        }
        Ok(self)
    }
    pub fn with_discovery(mut self, enabled: bool) -> Self {
        self.discovery = enabled;
        self
    }

    /// Reject invalid includes before starting a mount or background worker.
    pub fn validate(&self, snapshot: &Snapshot) -> Result<()> {
        for include in &self.includes {
            ensure!(
                include.as_os_str().is_empty()
                    || snapshot.nodes.iter().any(|node| node.path == *include),
                "prefetch include is absent from the pinned snapshot: {}",
                include.display()
            );
        }
        Ok(())
    }
    fn included(&self, path: &Path) -> bool {
        self.includes
            .iter()
            .any(|include| path.starts_with(include))
    }
    fn in_scope(&self, path: &Path) -> bool {
        self.roots.is_empty()
            || self.roots.iter().any(|root| path.starts_with(root))
            || self.included(path)
    }
    fn matches(&self, path: &Path, kind: &str) -> bool {
        (kind == "file" || kind == "symlink")
            && (self.included(path)
                || (self.in_scope(path) && (kind == "symlink" || is_bazel_metadata_path(path))))
    }

    /// Match native snapshot paths before projecting display-only strings.
    pub fn select(&self, snapshot: &Snapshot) -> Vec<PublicEntry> {
        let mut selected: Vec<_> = snapshot
            .nodes
            .iter()
            .skip(1)
            .filter(|entry| self.matches(&entry.path, entry.kind.name()))
            .map(|entry| PublicEntry {
                path: display_git_path(&entry.path),
                inode: entry.inode,
                mode: entry.mode,
                kind: entry.kind.name().to_owned(),
                oid: entry.oid.clone(),
            })
            .collect();
        selected.sort_by(|left, right| left.path.cmp(&right.path));
        selected
    }
}

pub fn is_bazel_metadata_path(path: &Path) -> bool {
    let Some(name) = path.file_name() else {
        return false;
    };
    let name = name.as_encoded_bytes();
    matches!(
        name,
        b"BUILD"
            | b"BUILD.bazel"
            | b"WORKSPACE"
            | b"WORKSPACE.bazel"
            | b"MODULE.bazel"
            | b"MODULE.bazel.lock"
            | b"REPO.bazel"
            | b".bazelrc"
            | b".bazelversion"
    ) || name.ends_with(b".bzl")
        || name.ends_with(b".MODULE.bazel")
}

pub fn select_bazel_metadata(entries: &[PublicEntry]) -> Vec<&PublicEntry> {
    let mut selected: Vec<_> = entries
        .iter()
        .filter(|entry| {
            entry.kind == "symlink"
                || (entry.kind == "file" && is_bazel_metadata_path(Path::new(&entry.path)))
        })
        .collect();
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    selected
}

pub fn prefetch_bazel_metadata(
    repo: &mut GitRepo,
    snapshot: &Snapshot,
) -> Result<BazelPrefetchReport> {
    prefetch_bazel_with_policy(repo, snapshot, &BazelPrefetchPolicy::default())
}

pub fn prefetch_bazel_with_policy(
    repo: &mut GitRepo,
    snapshot: &Snapshot,
    policy: &BazelPrefetchPolicy,
) -> Result<BazelPrefetchReport> {
    policy.validate(snapshot)?;
    let entries: HashMap<_, _> = snapshot
        .nodes
        .iter()
        .skip(1)
        .filter(|node| matches!(node.kind.name(), "file" | "symlink"))
        .map(|node| (node.path.clone(), node))
        .collect();
    let mut selected = HashSet::new();
    let mut requested = HashSet::new();
    let mut reasons: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut seeds = Vec::new();
    let mut oids = Vec::new();
    for (path, node) in &entries {
        if !policy.matches(path, node.kind.name()) {
            continue;
        }
        selected.insert(path.clone());
        let reason = if policy.included(path) {
            "explicit include"
        } else if node.kind.name() == "symlink" {
            "symlink target"
        } else {
            "Bazel metadata filename"
        };
        reasons
            .entry(display_git_path(path))
            .or_default()
            .push(reason.into());
        let oid = node
            .oid
            .as_ref()
            .context("selected metadata entry has no blob ID")?;
        if requested.insert(oid.clone()) {
            oids.push(oid.clone());
        }
        if node.kind.name() == "file"
            && let Some(kind) = seed_kind(path)
        {
            seeds.push((path.clone(), kind));
        }
    }
    oids.sort();
    seeds.sort_by(|left, right| left.0.cmp(&right.0));
    let mut blobs = repo.prefetch_blobs(&oids)?;
    let initially_selected = selected.len();
    let mut read = HashSet::new();
    let mut total_bytes = 0u64;
    let mut waves = 0;
    let mut skipped = Vec::new();
    while policy.discovery && !seeds.is_empty() {
        if waves >= 8 {
            skipped.push("remaining discovery omitted: eight-wave limit reached".into());
            break;
        }
        waves += 1;
        let mut next = Vec::new();
        let mut pending = Vec::new();
        let mut stop = false;
        'seed_loop: for (path, kind) in seeds {
            if !read.insert(path.clone()) {
                continue;
            }
            let node = entries.get(&path).context("discovery seed is absent")?;
            let oid = node.oid.as_ref().context("discovery seed has no blob ID")?;
            let size = *repo
                .blob_sizes(std::slice::from_ref(oid))?
                .get(oid)
                .context("seed size unavailable")?;
            if size > 1024 * 1024 {
                if skipped.len() < 64 {
                    skipped.push(format!(
                        "{}: discovery omitted because seed exceeds 1 MiB",
                        path.display()
                    ));
                }
                continue;
            }
            let Some(next_total) = total_bytes.checked_add(size) else {
                skipped.push("remaining discovery omitted: seed byte counter overflow".into());
                stop = true;
                break;
            };
            if next_total > 8 * 1024 * 1024 {
                skipped.push("remaining discovery omitted: 8 MiB total seed limit reached".into());
                stop = true;
                break;
            }
            total_bytes = next_total;
            let bytes = repo.git_output(&["cat-file", "blob", oid], false)?;
            let Ok(text) = std::str::from_utf8(&bytes) else {
                if skipped.len() < 64 {
                    skipped.push(format!("{}: non-UTF8 discovery seed", path.display()));
                }
                continue;
            };
            let refs = match kind {
                Seed::Module => starlark_references(text),
                Seed::Rc => rc_references(text),
            };
            for reference in refs {
                // Included MODULE fragments share Bazel's root-module package.
                let Some(target) = resolve_reference(Path::new(""), &reference.value) else {
                    continue;
                };
                if !policy.in_scope(&target) {
                    continue;
                }
                let Some(target_node) = entries.get(&target) else {
                    continue;
                };
                if !selected.contains(&target) && selected.len() - initially_selected >= 10_000 {
                    skipped.push(
                        "remaining discovery omitted: 10,000 additional path limit reached".into(),
                    );
                    stop = true;
                    break 'seed_loop;
                }
                let display = display_git_path(&target);
                let reason = format!("{} in {}", reference.reason, path.display());
                let target_reasons = reasons.entry(display).or_default();
                if !target_reasons.contains(&reason) {
                    target_reasons.push(reason);
                }
                if selected.insert(target.clone()) {
                    let oid = target_node
                        .oid
                        .as_ref()
                        .context("discovered file has no blob ID")?;
                    if requested.insert(oid.clone()) {
                        pending.push(oid.clone());
                    }
                }
                if reference.rc_import
                    && target_node.kind.name() == "file"
                    && !read.contains(&target)
                {
                    next.push((target, Seed::Rc));
                }
            }
        }
        if !pending.is_empty() {
            let batch = repo.prefetch_blobs(&pending)?;
            blobs.requested_blobs += batch.requested_blobs;
            blobs.already_local_blobs += batch.already_local_blobs;
            blobs.fetched_blobs += batch.fetched_blobs;
            blobs.fetch_batches += batch.fetch_batches;
        }
        if stop {
            break;
        }
        seeds = next;
    }
    let mut selected_paths: Vec<_> = selected.into_iter().map(|p| display_git_path(&p)).collect();
    selected_paths.sort();
    Ok(BazelPrefetchReport {
        selected_paths,
        blobs,
        path_reasons: reasons,
        discovery_waves: waves,
        skipped_discovery: skipped,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Seed {
    Module,
    Rc,
}
fn seed_kind(path: &Path) -> Option<Seed> {
    let name = path.file_name()?.as_encoded_bytes();
    if name == b".bazelrc" {
        Some(Seed::Rc)
    } else if name == b"MODULE.bazel" || name.ends_with(b".MODULE.bazel") {
        Some(Seed::Module)
    } else {
        None
    }
}
struct Reference {
    value: String,
    reason: &'static str,
    rc_import: bool,
}
fn resolve_reference(base: &Path, value: &str) -> Option<PathBuf> {
    let workspace_relative = value.starts_with("%workspace%/");
    let value = value.strip_prefix("%workspace%/").unwrap_or(value);
    let base = if workspace_relative {
        Path::new("")
    } else {
        base
    };
    if value.is_empty() || value.starts_with('@') || value.contains(['$', '%', '\\', '\0']) {
        return None;
    }
    let path = if let Some(label) = value.strip_prefix("//") {
        let (package, file) = label.split_once(':')?;
        if file.is_empty() || file.contains(':') {
            return None;
        }
        PathBuf::from(package).join(file)
    } else if let Some(file) = value.strip_prefix(':') {
        if file.is_empty() || file.contains(':') {
            return None;
        }
        base.join(file)
    } else {
        if value.contains(':') {
            return None;
        }
        base.join(value)
    };
    normalize(&path).ok().filter(|p| !p.as_os_str().is_empty())
}

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Ident(String),
    String(String),
    Symbol(char),
}
/// Tokens inside comments and strings cannot masquerade as attribute syntax.
fn lex(text: &str) -> Vec<Token> {
    let chars: Vec<_> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '#' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '\'' || c == '"' {
            let triple = chars.get(i + 1) == Some(&c) && chars.get(i + 2) == Some(&c);
            i += if triple { 3 } else { 1 };
            let mut value = String::new();
            let mut closed = false;
            let mut valid = true;
            while i < chars.len() {
                if chars[i] == c
                    && (!triple || (chars.get(i + 1) == Some(&c) && chars.get(i + 2) == Some(&c)))
                {
                    i += if triple { 3 } else { 1 };
                    closed = true;
                    break;
                }
                if chars[i] == '\\' {
                    i += 1;
                    let Some(&escaped) = chars.get(i) else {
                        break;
                    };
                    match escaped {
                        '\\' | '\'' | '"' => value.push(escaped),
                        'n' => value.push('\n'),
                        'r' => value.push('\r'),
                        't' => value.push('\t'),
                        _ => {
                            valid = false;
                        }
                    }
                } else {
                    value.push(chars[i]);
                }
                i += 1;
            }
            if closed && valid {
                tokens.push(Token::String(value));
            } else {
                tokens.push(Token::Symbol('?'));
            }
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            i += 1;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            tokens.push(Token::Ident(chars[start..i].iter().collect()));
        } else {
            if !c.is_whitespace() {
                tokens.push(Token::Symbol(c));
            }
            i += 1;
        }
    }
    tokens
}
fn starlark_references(text: &str) -> Vec<Reference> {
    let tokens = lex(text);
    let mut refs = Vec::new();
    for i in 0..tokens.len() {
        let Token::Ident(name) = &tokens[i] else {
            continue;
        };
        if i == 0 || !matches!(tokens.get(i - 1), Some(Token::Symbol('(' | ','))) {
            continue;
        }
        if !matches!(name.as_str(), "patches" | "requirements_lock")
            || tokens.get(i + 1) != Some(&Token::Symbol('='))
        {
            continue;
        }
        let mut end = i + 2;
        let mut values = Vec::new();
        if name == "requirements_lock" {
            if let Some(Token::String(value)) = tokens.get(end) {
                values.push(value.clone());
                end += 1;
            }
        } else if tokens.get(end) == Some(&Token::Symbol('[')) {
            end += 1;
            let mut valid = true;
            while end < tokens.len() && tokens.get(end) != Some(&Token::Symbol(']')) {
                if let Some(Token::String(value)) = tokens.get(end) {
                    values.push(value.clone());
                    end += 1;
                } else {
                    valid = false;
                    break;
                }
                if tokens.get(end) == Some(&Token::Symbol(',')) {
                    end += 1;
                } else if tokens.get(end) != Some(&Token::Symbol(']')) {
                    valid = false;
                    break;
                }
            }
            if !valid || tokens.get(end) != Some(&Token::Symbol(']')) {
                values.clear();
            } else {
                end += 1;
            }
        }
        if !matches!(tokens.get(end), Some(Token::Symbol(',' | ')')) | None) {
            continue;
        }
        for value in values {
            refs.push(Reference {
                value,
                reason: if name == "patches" {
                    "literal patches"
                } else {
                    "literal requirements_lock"
                },
                rc_import: false,
            });
        }
    }
    refs
}
fn rc_words(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    for c in line.chars() {
        if escaped {
            word.push(c);
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if let Some(q) = quote {
            if c == q {
                quote = None;
            } else {
                word.push(c);
            }
        } else if c == '\'' || c == '"' {
            quote = Some(c);
        } else if c == '#' {
            break;
        } else if c.is_whitespace() {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
        } else {
            word.push(c);
        }
    }
    if quote.is_some() || escaped {
        return vec![];
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}
fn rc_references(text: &str) -> Vec<Reference> {
    let mut refs = Vec::new();
    for line in text.lines() {
        let words = rc_words(line);
        if matches!(
            words.first().map(String::as_str),
            Some("import" | "try-import")
        ) {
            if words.len() == 2 {
                refs.push(Reference {
                    value: words[1].clone(),
                    reason: "rc import",
                    rc_import: true,
                });
            }
            continue;
        }
        for (i, word) in words.iter().enumerate() {
            let value = word.strip_prefix("--downloader_config=").or_else(|| {
                if word == "--downloader_config" {
                    words.get(i + 1).map(String::as_str)
                } else {
                    None
                }
            });
            if let Some(value) = value {
                refs.push(Reference {
                    value: value.into(),
                    reason: "literal downloader_config",
                    rc_import: false,
                });
            }
        }
    }
    refs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SizeFallback;
    use std::process::Command;

    #[test]
    fn starlark_discovery_only_uses_literal_file_attributes() {
        let source = r#"
# f(patches = ["//:comment.patch"])
""" f(patches = ["//:doc.patch"]) """
patches = ["//:variable.patch"]
f(
    patches = ["//pkg:first.patch", ':second.patch',],
    requirements_lock = "//:requirements.txt",
    lock_file = "//:maven_install.json",
    other = "patches = [\"//:quoted.patch\"]",
)
f(patches = ["//:prefix.patch"] + variable)
f(requirements_lock = "//:prefix.txt" + suffix)
f(patches = dynamic)
f(patches = ["//:literal.patch", dynamic])
f(patches = [x for x in values])
"#;
        let refs = starlark_references(source);
        let values: Vec<_> = refs
            .iter()
            .map(|reference| reference.value.as_str())
            .collect();
        assert_eq!(
            values,
            ["//pkg:first.patch", ":second.patch", "//:requirements.txt"]
        );
    }

    #[test]
    fn repository_reference_resolution_rejects_external_and_unsafe_paths() {
        let base = Path::new("module");
        for (value, expected) in [
            ("//pkg:file.patch", "pkg/file.patch"),
            (":local.patch", "module/local.patch"),
            ("local.patch", "module/local.patch"),
            ("//:root.txt", "root.txt"),
            ("%workspace%/config/file", "config/file"),
        ] {
            assert_eq!(resolve_reference(base, value).unwrap(), Path::new(expected));
        }
        for value in [
            "@external//:file",
            "@@external//:file",
            "../outside",
            "//pkg:../outside",
            "/absolute",
            "//pkg:/absolute",
            "$HOME/file",
            "%other%/file",
            "C:/file",
            "\\windows",
            ":",
        ] {
            assert!(resolve_reference(base, value).is_none(), "{value}");
        }
    }

    #[test]
    fn rc_discovery_handles_quotes_comments_imports_and_downloader_flags() {
        let refs = rc_references(
            r#"
# import bad.rc
import %workspace%/config/common.rc
try-import "config/optional.rc"
common --downloader_config=downloader.cfg # --downloader_config=bad.cfg
build:ci --downloader_config "config/with space.cfg"
common --other="--downloader_config=not-a-flag.cfg"
"#,
        );
        let values: Vec<_> = refs
            .iter()
            .map(|reference| (reference.value.as_str(), reference.rc_import))
            .collect();
        assert_eq!(
            values,
            [
                ("%workspace%/config/common.rc", true),
                ("config/optional.rc", true),
                ("downloader.cfg", false),
                ("config/with space.cfg", false)
            ]
        );
    }

    fn fixture() -> (tempfile::TempDir, GitRepo, Snapshot) {
        let dir = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success());
        };
        git(&["init", "-q"]);
        git(&["config", "user.name", "Fixture"]);
        git(&["config", "user.email", "fixture@example.invalid"]);
        for (path, content) in [
            (
                "MODULE.bazel",
                "f(patches = ['//patches:change.patch'], requirements_lock = '//:requirements.txt')",
            ),
            (
                ".bazelrc",
                "import %workspace%/config/common.rc\ncommon --downloader_config=downloader.cfg",
            ),
            ("config/common.rc", "import config/nested.rc\n"),
            (
                "config/nested.rc",
                "common --downloader_config=config/nested.cfg\n",
            ),
            ("config/nested.cfg", "nested rules"),
            ("patches/change.patch", "patch content"),
            ("requirements.txt", "dependency==1"),
            ("downloader.cfg", "rules"),
            ("unused.txt", "never selected"),
        ] {
            let path = dir.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        git(&["add", "."]);
        git(&["commit", "-qm", "fixture"]);
        git(&["clone", "-q", "--bare", ".", "bare.git"]);
        let repo = GitRepo::open(
            dir.path().join("bare.git"),
            "git",
            "origin",
            SizeFallback::Error,
        )
        .unwrap();
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        (dir, repo, snapshot)
    }

    #[test]
    fn public_prefetch_discovers_recursive_rc_and_module_inputs_without_unrelated_files() {
        let (_dir, mut repo, snapshot) = fixture();
        let report = prefetch_bazel_metadata(&mut repo, &snapshot).unwrap();
        assert_eq!(
            report.selected_paths,
            [
                ".bazelrc",
                "MODULE.bazel",
                "config/common.rc",
                "config/nested.cfg",
                "config/nested.rc",
                "downloader.cfg",
                "patches/change.patch",
                "requirements.txt"
            ]
        );
        assert_eq!(report.blobs.requested_blobs, 8);
        assert_eq!(report.blobs.already_local_blobs, 8);
        assert_eq!(report.blobs.fetched_blobs, 0);
        assert_eq!(report.discovery_waves, 3);
        assert!(
            report.path_reasons["requirements.txt"]
                .iter()
                .any(|reason| reason.contains("requirements_lock"))
        );
    }

    #[test]
    fn discovery_obeys_scope_and_explicit_includes_can_add_outside_scope() {
        let (_dir, mut repo, snapshot) = fixture();
        let scoped = BazelPrefetchPolicy::new(&["MODULE.bazel".into()]).unwrap();
        let report = prefetch_bazel_with_policy(&mut repo, &snapshot, &scoped).unwrap();
        assert_eq!(report.selected_paths, ["MODULE.bazel"]);
        let included = scoped
            .with_includes(&["patches".into()])
            .unwrap()
            .with_discovery(false);
        let report = prefetch_bazel_with_policy(&mut repo, &snapshot, &included).unwrap();
        assert_eq!(
            report.selected_paths,
            ["MODULE.bazel", "patches/change.patch"]
        );
        assert_eq!(report.discovery_waves, 0);
        let missing = included.with_includes(&["missing.txt".into()]).unwrap();
        assert!(prefetch_bazel_with_policy(&mut repo, &snapshot, &missing).is_err());
        assert!(
            BazelPrefetchPolicy::default()
                .with_includes(&["../escape".into()])
                .is_err()
        );
    }

    fn updated_snapshot(dir: &Path, repo: &GitRepo, files: &[(&str, String)]) -> Snapshot {
        for (path, content) in files {
            let path = dir.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        for args in [&["add", "."][..], &["commit", "-qm", "updated fixture"][..]] {
            // Keep the nested bare clone out of the fixture worktree's index.
            let output = Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .args(if args[0] == "add" {
                    &[":!bare.git"][..]
                } else {
                    &[][..]
                })
                .output()
                .unwrap();
            assert!(output.status.success());
        }
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(dir.join("bare.git"))
            .arg("fetch")
            .arg("-q")
            .arg(dir)
            .arg("HEAD")
            .output()
            .unwrap();
        assert!(output.status.success());
        Snapshot::load(repo, "FETCH_HEAD").unwrap()
    }

    #[test]
    fn oversized_module_keeps_base_metadata_and_explicit_includes_without_discovery_failure() {
        let (dir, mut repo, _) = fixture();
        let text = "# large valid module comment\n".repeat(40_000);
        let snapshot = updated_snapshot(dir.path(), &repo, &[("MODULE.bazel", text)]);
        let policy = BazelPrefetchPolicy::new(&["MODULE.bazel".into()])
            .unwrap()
            .with_includes(&["unused.txt".into()])
            .unwrap();
        let report = prefetch_bazel_with_policy(&mut repo, &snapshot, &policy).unwrap();
        assert_eq!(report.selected_paths, ["MODULE.bazel", "unused.txt"]);
        assert_eq!(report.blobs.requested_blobs, 2);
        assert!(
            report
                .skipped_discovery
                .iter()
                .any(|reason| reason.contains("1 MiB"))
        );
    }

    #[test]
    fn recursive_rc_cycles_and_deep_chains_are_bounded_and_nonfatal() {
        for (name, count, closes_cycle, expected_limit) in
            [("cycle", 2, true, false), ("deep chain", 12, false, true)]
        {
            let (dir, mut repo, _) = fixture();
            let mut files = vec![(".bazelrc".to_owned(), "import chain/0.rc\n".to_owned())];
            for index in 0..count {
                let content = if index + 1 < count {
                    format!("import chain/{}.rc\n", index + 1)
                } else if closes_cycle {
                    "import chain/0.rc\n".into()
                } else {
                    "# end\n".into()
                };
                files.push((format!("chain/{index}.rc"), content));
            }
            let borrowed: Vec<_> = files
                .iter()
                .map(|(path, text)| (path.as_str(), text.clone()))
                .collect();
            let snapshot = updated_snapshot(dir.path(), &repo, &borrowed);
            let report = prefetch_bazel_metadata(&mut repo, &snapshot).unwrap();
            assert!(report.discovery_waves <= 8, "{name}");
            assert_eq!(
                report
                    .skipped_discovery
                    .iter()
                    .any(|reason| reason.contains("eight-wave")),
                expected_limit,
                "{name}"
            );
            if expected_limit {
                assert!(
                    !report
                        .selected_paths
                        .iter()
                        .any(|path| path == "chain/8.rc")
                );
            }
        }
    }

    #[test]
    fn included_module_patch_labels_resolve_from_root_module_package() {
        let (dir, mut repo, _) = fixture();
        let snapshot = updated_snapshot(
            dir.path(),
            &repo,
            &[
                (
                    "subdir/child.MODULE.bazel",
                    "f(patches = [':fix.patch'])".into(),
                ),
                ("fix.patch", "root patch".into()),
                ("subdir/fix.patch", "not the referenced patch".into()),
            ],
        );
        let report = prefetch_bazel_metadata(&mut repo, &snapshot).unwrap();
        assert!(report.selected_paths.iter().any(|path| path == "fix.patch"));
        assert!(
            !report
                .selected_paths
                .iter()
                .any(|path| path == "subdir/fix.patch")
        );
    }

    #[test]
    fn aggregate_seed_budget_reports_omission_without_failing_base_prefetch() {
        let (dir, mut repo, _) = fixture();
        let contents = format!("#{}\n", "x".repeat(1024 * 1024 - 2));
        let files: Vec<_> = (0..9)
            .map(|index| (format!("part{index}.MODULE.bazel"), contents.clone()))
            .collect();
        let borrowed: Vec<_> = files
            .iter()
            .map(|(path, text)| (path.as_str(), text.clone()))
            .collect();
        let snapshot = updated_snapshot(dir.path(), &repo, &borrowed);
        let report = prefetch_bazel_metadata(&mut repo, &snapshot).unwrap();
        assert!(
            report
                .selected_paths
                .iter()
                .any(|path| path == "part8.MODULE.bazel")
        );
        assert!(
            report
                .skipped_discovery
                .iter()
                .any(|reason| reason.contains("8 MiB total"))
        );
    }
}
