use super::*;
use std::collections::HashMap;
use tinytree_model::Kind;

#[test]
fn independent_builds_have_identical_complete_verified_native_closures() {
    let config = FixtureConfig {
        unused_files: 8,
        bytes_per_file: 73,
    };
    let a = build_fixture(config).unwrap();
    let b = build_fixture(config).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.manifest_bytes().unwrap(), b.manifest_bytes().unwrap());
    assert_eq!(a.manifest.files.len(), 11);

    let by_key: HashMap<_, _> = a
        .objects()
        .iter()
        .map(|object| {
            (
                (object.descriptor.key.kind as u8, object.descriptor.key.id.0),
                object,
            )
        })
        .collect();
    assert_eq!(by_key.len(), a.objects().len());
    assert!(
        a.objects()
            .iter()
            .any(|object| object.descriptor.key.kind == Kind::Commit
                && hex(object.descriptor.key.id.0) == a.manifest.root_commit)
    );
    assert!(!by_key.contains_key(&(Kind::Commit as u8, CommitId::ROOT.0)));
    let empty = tinytree_model::empty_tree();
    assert!(by_key.contains_key(&(Kind::Tree as u8, empty.descriptor.key.id.0)));

    for object in a.objects() {
        tinytree_model::verify(&object.descriptor, &object.bytes, Limits::default()).unwrap();
        if object.descriptor.key.kind == Kind::File {
            continue;
        }
        let decoded = tinytree_model::decode(&object.bytes, Limits::default()).unwrap();
        for key in decoded.references() {
            assert!(
                by_key.contains_key(&(key.kind as u8, key.id.0)),
                "fixture closure must be complete"
            );
        }
        if let Object::Tree(tree) = decoded {
            for entry in tree.entries {
                if let TreeValue::File { id, size_bytes, .. } = entry.value {
                    let file = by_key[&(Kind::File as u8, id.0)];
                    assert_eq!(size_bytes, file.bytes.len() as u64);
                }
            }
        }
    }
    for file in &a.manifest.files {
        let object = a
            .objects()
            .iter()
            .find(|object| {
                object.descriptor.key.kind == Kind::File
                    && hex(object.descriptor.key.id.0) == file.id
            })
            .unwrap();
        assert_eq!(object.bytes.len() as u64, file.size_bytes);
        assert!(!file.executable);
    }
}

#[test]
fn adding_unused_files_preserves_existing_content_identities() {
    let a = build_fixture(FixtureConfig {
        unused_files: 2,
        bytes_per_file: 128,
    })
    .unwrap();
    let b = build_fixture(FixtureConfig {
        unused_files: 3,
        bytes_per_file: 128,
    })
    .unwrap();
    let files_b: HashMap<_, _> = b
        .manifest
        .files
        .iter()
        .map(|file| (&file.path, file))
        .collect();
    for file in &a.manifest.files {
        assert_eq!(files_b[&file.path], file);
    }
    assert_ne!(a.manifest.root_commit, b.manifest.root_commit);
    let empty = build_fixture(FixtureConfig {
        unused_files: 0,
        bytes_per_file: 0,
    })
    .unwrap();
    assert_eq!(empty.manifest.files.len(), 3);
}

#[test]
fn invalid_allocation_budgets_are_rejected_before_generation() {
    for config in [
        FixtureConfig {
            unused_files: MAX_UNUSED_FILES + 1,
            bytes_per_file: 0,
        },
        FixtureConfig {
            unused_files: 1,
            bytes_per_file: MAX_BYTES_PER_FILE + 1,
        },
        FixtureConfig {
            unused_files: MAX_UNUSED_FILES,
            bytes_per_file: MAX_BYTES_PER_FILE,
        },
        FixtureConfig {
            unused_files: usize::MAX,
            bytes_per_file: usize::MAX,
        },
    ] {
        assert!(build_fixture(config).is_err(), "invalid config must fail");
    }
    FixtureConfig {
        unused_files: MAX_UNUSED_FILES,
        bytes_per_file: 0,
    }
    .validate()
    .unwrap();
}

#[test]
fn bundle_writer_refuses_nonempty_destinations_without_mutation() {
    let temporary = tempfile::tempdir().unwrap();
    let fixture = build_fixture(FixtureConfig {
        unused_files: 2,
        bytes_per_file: 64,
    })
    .unwrap();
    let out = temporary.path().join("bundle");
    let report = fixture.write_bundle(&out).unwrap();
    assert_eq!(report.object_count, fixture.objects().len());
    assert_eq!(report.file_count, fixture.manifest.files.len());
    assert_eq!(
        fs::read(out.join("manifest.json")).unwrap(),
        fixture.manifest_bytes().unwrap()
    );
    assert!(fixture.write_bundle(&out).is_err());
    for object in fixture.objects() {
        assert_eq!(
            fs::read(out.join(object_relative_path(object.descriptor.key))).unwrap(),
            object.bytes
        );
    }

    let nonempty = temporary.path().join("unrelated");
    fs::create_dir(&nonempty).unwrap();
    fs::write(nonempty.join("keep"), b"do not alter").unwrap();
    assert!(fixture.write_bundle(&nonempty).is_err());
    assert_eq!(fs::read(nonempty.join("keep")).unwrap(), b"do not alter");
    assert_eq!(fs::read_dir(&nonempty).unwrap().count(), 1);
    let empty = temporary.path().join("empty");
    fs::create_dir(&empty).unwrap();
    fixture.write_bundle(&empty).unwrap();
}

#[cfg(unix)]
#[test]
fn bundle_writer_refuses_a_symlink_destination() {
    let temporary = tempfile::tempdir().unwrap();
    let real = temporary.path().join("real");
    let alias = temporary.path().join("alias");
    fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let fixture = build_fixture(FixtureConfig {
        unused_files: 0,
        bytes_per_file: 0,
    })
    .unwrap();
    assert!(fixture.write_bundle(&alias).is_err());
    assert_eq!(fs::read_dir(real).unwrap().count(), 0);
}
