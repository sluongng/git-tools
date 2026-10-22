use std::fs;
use std::path::Path;
use std::process::Command;

use tinytree_fixture::{FixtureConfig, Manifest, SeedReport, build_fixture, object_relative_path};

fn run(out: &Path) -> SeedReport {
    let output = Command::new(env!("CARGO_BIN_EXE_tinytree-fixture"))
        .args(["--files", "5", "--bytes-per-file", "127", "--out"])
        .arg(out)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn separate_cli_processes_produce_identical_native_bundles() {
    let temporary = tempfile::tempdir().unwrap();
    let a = temporary.path().join("a");
    let b = temporary.path().join("b");
    let report_a = run(&a);
    let report_b = run(&b);
    let expected = build_fixture(FixtureConfig {
        unused_files: 5,
        bytes_per_file: 127,
    })
    .unwrap();
    assert_eq!(report_a.root_commit, expected.manifest.root_commit);
    assert_eq!(report_a.root_commit, report_b.root_commit);
    assert_eq!(report_a.object_count, report_b.object_count);
    assert_eq!(report_a.object_bytes, report_b.object_bytes);
    assert!(report_a.elapsed_seconds.is_finite() && report_a.elapsed_seconds >= 0.0);
    assert!(report_b.elapsed_seconds.is_finite() && report_b.elapsed_seconds >= 0.0);
    let manifest_a = fs::read(a.join("manifest.json")).unwrap();
    assert_eq!(manifest_a, fs::read(b.join("manifest.json")).unwrap());
    assert_eq!(manifest_a, expected.manifest_bytes().unwrap());
    let manifest: Manifest = serde_json::from_slice(&manifest_a).unwrap();
    assert_eq!(manifest.files.len(), 8);
    let text = String::from_utf8(manifest_a).unwrap();
    assert!(!text.contains("elapsed_seconds"));
    assert!(!text.contains(&temporary.path().to_string_lossy().to_string()));
    for object in expected.objects() {
        let relative = object_relative_path(object.descriptor.key);
        let bytes_a = fs::read(a.join(&relative)).unwrap();
        assert_eq!(bytes_a, fs::read(b.join(relative)).unwrap());
        assert_eq!(bytes_a, object.bytes);
    }
}

#[test]
fn cli_rejects_invalid_budgets_without_creating_output() {
    let temporary = tempfile::tempdir().unwrap();
    let out = temporary.path().join("invalid");
    let result = Command::new(env!("CARGO_BIN_EXE_tinytree-fixture"))
        .args(["--files", "10001", "--out"])
        .arg(&out)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!out.exists());
}
