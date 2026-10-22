//! Deterministic native objects for loopback tests and measured repository seeds.
//! No Git conversion, repository server, network access or native storage policy.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tinytree_model::{
    ChangeId, Commit, CommitId, EncodedObject, FileId, Limits, Object, ObjectKey, Signature,
    Timestamp, Tree, TreeEntry, TreeId, TreeMerge, TreeValue,
};

pub const MAX_UNUSED_FILES: usize = 10_000;
pub const MAX_BYTES_PER_FILE: usize = 16 * 1024 * 1024;
pub const MAX_TOTAL_ASSET_BYTES: usize = 128 * 1024 * 1024;

const MODULE: &[u8] =
    b"module(name = \"tinytree_fixture\")\nbazel_dep(name = \"rules_cc\", version = \"0.2.25\")\n";
const BUILD: &[u8] = b"load(\"@rules_cc//cc:cc_binary.bzl\", \"cc_binary\")\n\ncc_binary(\n    name = \"hello\",\n    srcs = [\"hello.cc\"],\n)\n";
const HELLO: &[u8] =
    b"#include <cstdio>\nint main() { std::puts(\"hello from tinytree\"); return 0; }\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureConfig {
    /// Unused files in assets/, independent of the three Bazel workload files.
    pub unused_files: usize,
    pub bytes_per_file: usize,
}

impl Default for FixtureConfig {
    fn default() -> Self {
        Self {
            unused_files: 64,
            bytes_per_file: 4096,
        }
    }
}

impl FixtureConfig {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.unused_files <= MAX_UNUSED_FILES,
            "too many unused files"
        );
        ensure!(
            self.bytes_per_file <= MAX_BYTES_PER_FILE,
            "asset file exceeds byte limit"
        );
        let total = self
            .unused_files
            .checked_mul(self.bytes_per_file)
            .context("asset byte count overflow")?;
        ensure!(
            total <= MAX_TOTAL_ASSET_BYTES,
            "total asset bytes exceed limit"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub id: String,
    pub size_bytes: u64,
    pub executable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestObject {
    pub kind: u8,
    pub id: String,
    pub size_bytes: u64,
}

/// Stable logical manifest. Machine paths and timing intentionally live elsewhere.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u16,
    pub config: FixtureConfig,
    pub root_commit: String,
    pub files: Vec<ManifestFile>,
    pub objects: Vec<ManifestObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fixture {
    pub manifest: Manifest,
    objects: Vec<EncodedObject>,
}

/// Observations about one seed operation; not part of object or manifest identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedReport {
    pub root_commit: String,
    pub file_count: usize,
    pub object_count: usize,
    pub object_bytes: u64,
    pub elapsed_seconds: f64,
}

type Objects = BTreeMap<(u8, [u8; 32]), EncodedObject>;

fn hex(id: [u8; 32]) -> String {
    blake3::Hash::from(id).to_hex().to_string()
}

fn add_object(objects: &mut Objects, object: EncodedObject) {
    objects
        .entry((object.descriptor.key.kind as u8, object.descriptor.key.id.0))
        .or_insert(object);
}

fn add_file(
    objects: &mut Objects,
    files: &mut Vec<ManifestFile>,
    path: &str,
    bytes: Vec<u8>,
) -> TreeValue {
    let descriptor = tinytree_model::describe_file(&bytes);
    files.push(ManifestFile {
        path: path.to_owned(),
        id: hex(descriptor.key.id.0),
        size_bytes: descriptor.size_bytes,
        executable: false,
    });
    let value = TreeValue::File {
        id: FileId(descriptor.key.id.0),
        executable: false,
        size_bytes: descriptor.size_bytes,
        copy_id: None,
    };
    add_object(objects, EncodedObject { descriptor, bytes });
    value
}

fn add_tree(objects: &mut Objects, entries: Vec<TreeEntry>) -> Result<TreeId> {
    let encoded = tinytree_model::encode(&Object::Tree(Tree { entries }), Limits::default())?;
    let id = TreeId(encoded.descriptor.key.id.0);
    add_object(objects, encoded);
    Ok(id)
}

/// Build one native commit plus its complete object closure and intrinsic empty
/// tree. Asset contents depend only on their index and requested length, not the
/// number of neighboring assets, host state, clock, random source or Git state.
pub fn build_fixture(config: FixtureConfig) -> Result<Fixture> {
    config.validate()?;
    let mut objects = Objects::new();
    let mut files = Vec::with_capacity(config.unused_files + 3);
    add_object(&mut objects, tinytree_model::empty_tree());
    let mut assets = Vec::with_capacity(config.unused_files);
    for index in 0..config.unused_files {
        let name = format!("unused-{index:08}.bin");
        let mut bytes = vec![0; config.bytes_per_file];
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"tinytree-fixture-asset-v1\0");
        hasher.update(&(index as u64).to_be_bytes());
        hasher.finalize_xof().fill(&mut bytes);
        assets.push(TreeEntry {
            name: name.clone(),
            value: add_file(&mut objects, &mut files, &format!("assets/{name}"), bytes),
        });
    }
    let asset_tree = add_tree(&mut objects, assets)?;
    let mut entries = Vec::with_capacity(4);
    for (name, bytes) in [("BUILD.bazel", BUILD), ("MODULE.bazel", MODULE)] {
        entries.push(TreeEntry {
            name: name.into(),
            value: add_file(&mut objects, &mut files, name, bytes.to_vec()),
        });
    }
    entries.push(TreeEntry {
        name: "assets".into(),
        value: TreeValue::Tree(asset_tree),
    });
    entries.push(TreeEntry {
        name: "hello.cc".into(),
        value: add_file(&mut objects, &mut files, "hello.cc", HELLO.to_vec()),
    });
    let root_tree = add_tree(&mut objects, entries)?;
    let mut change = blake3::Hasher::new();
    change.update(b"tinytree-fixture-change-v1\0");
    change.update(root_tree.as_bytes());
    let mut change_id = [0; 16];
    change_id.copy_from_slice(&change.finalize().as_bytes()[..16]);
    let author = Signature {
        name: "Fixture".into(),
        email: "fixture@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let commit = tinytree_model::encode(
        &Object::Commit(Commit {
            parents: vec![CommitId::ROOT],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![root_tree],
                labels: vec![String::new()],
            },
            change_id: ChangeId(change_id),
            description: "Deterministic Tinytree fixture\n".into(),
            author: author.clone(),
            committer: author,
            secure_signature: None,
        }),
        Limits::default(),
    )?;
    let root_commit = hex(commit.descriptor.key.id.0);
    add_object(&mut objects, commit);
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let objects: Vec<_> = objects.into_values().collect();
    let manifest = Manifest {
        format_version: tinytree_model::FORMAT_VERSION,
        config,
        root_commit,
        files,
        objects: objects
            .iter()
            .map(|object| ManifestObject {
                kind: object.descriptor.key.kind as u8,
                id: hex(object.descriptor.key.id.0),
                size_bytes: object.descriptor.size_bytes,
            })
            .collect(),
    };
    Ok(Fixture { manifest, objects })
}

/// Relative bundle location, derived only from a validated typed object identity.
pub fn object_relative_path(key: ObjectKey) -> PathBuf {
    PathBuf::from("objects")
        .join((key.kind as u8).to_string())
        .join(hex(key.id.0))
}

impl Fixture {
    pub fn objects(&self) -> &[EncodedObject] {
        &self.objects
    }

    pub fn manifest_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(&self.manifest)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Write only into a new or empty selected directory. Files are create-new,
    /// the manifest is written last, and failures never remove existing contents.
    pub fn write_bundle(&self, out: &Path) -> Result<SeedReport> {
        let started = Instant::now();
        match fs::symlink_metadata(out) {
            Ok(meta) => {
                ensure!(
                    meta.file_type().is_dir() && !meta.file_type().is_symlink(),
                    "output must be a real directory"
                );
                ensure!(
                    fs::read_dir(out)?.next().is_none(),
                    "output directory is not empty"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(out)?,
            Err(error) => return Err(error.into()),
        }
        fs::create_dir(out.join("objects"))?;
        for object in &self.objects {
            let relative = object_relative_path(object.descriptor.key);
            let path = out.join(relative);
            let parent = path.parent().expect("fixed object path has parent");
            if !parent.exists() {
                fs::create_dir(parent)?;
            }
            write_new(&path, &object.bytes)?;
        }
        write_new(&out.join("manifest.json"), &self.manifest_bytes()?)?;
        Ok(SeedReport {
            root_commit: self.manifest.root_commit.clone(),
            file_count: self.manifest.files.len(),
            object_count: self.objects.len(),
            object_bytes: self
                .objects
                .iter()
                .map(|object| object.descriptor.size_bytes)
                .sum(),
            elapsed_seconds: started.elapsed().as_secs_f64(),
        })
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file: File = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests;
