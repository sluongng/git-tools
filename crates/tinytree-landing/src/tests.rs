use super::*;
use std::collections::HashMap;

#[derive(Default)]
struct Reader {
    objects: HashMap<m::ObjectKey, Vec<u8>>,
    reads: Vec<m::ObjectKey>,
    allowances: Vec<usize>,
}
impl MetadataReader for Reader {
    fn read(&mut self, key: m::ObjectKey, max_bytes: usize) -> Result<Vec<u8>, ReadError> {
        assert!(
            !matches!(key.kind, m::Kind::File | m::Kind::Symlink),
            "candidate opened content"
        );
        self.reads.push(key);
        self.allowances.push(max_bytes);
        let bytes = self.objects.get(&key).ok_or(ReadError::Missing)?;
        if bytes.len() > max_bytes {
            return Err(ReadError::Limit);
        }
        Ok(bytes.clone())
    }
}
impl Reader {
    fn put(&mut self, object: m::Object) -> [u8; 32] {
        let e = m::encode(&object, m::Limits::default()).unwrap();
        self.objects.insert(e.descriptor.key, e.bytes);
        e.descriptor.key.id.0
    }
    fn tree(&mut self, entries: Vec<(&str, m::TreeValue)>) -> m::TreeId {
        m::TreeId(
            self.put(m::Object::Tree(m::Tree {
                entries: entries
                    .into_iter()
                    .map(|(name, value)| m::TreeEntry {
                        name: name.into(),
                        value,
                    })
                    .collect(),
            })),
        )
    }
    fn commit(&mut self, parent: m::CommitId, tree: m::TreeId, change: u8) -> m::CommitId {
        m::CommitId(self.put(m::Object::Commit(commit(parent, tree, change))))
    }
}
fn signature(name: &str) -> m::Signature {
    m::Signature {
        name: name.into(),
        email: "fixture@example.invalid".into(),
        timestamp: m::Timestamp {
            millis_since_epoch: 12,
            tz_offset_minutes: 0,
        },
    }
}
fn commit(parent: m::CommitId, tree: m::TreeId, change: u8) -> m::Commit {
    m::Commit {
        parents: vec![parent],
        predecessors: vec![],
        root_tree: m::TreeMerge {
            terms: vec![tree],
            labels: vec![String::new()],
        },
        change_id: m::ChangeId([change; 16]),
        description: format!("change {change}"),
        author: signature("Author"),
        committer: signature("Old committer"),
        secure_signature: None,
    }
}
fn file(bytes: &[u8], executable: bool) -> m::TreeValue {
    m::TreeValue::File {
        id: m::file_id(bytes),
        executable,
        size_bytes: bytes.len() as u64,
        copy_id: None,
    }
}
fn request(base: m::CommitId, head: m::CommitId, dest: m::CommitId) -> Request {
    Request {
        source_base: base,
        source_head: head,
        destination_head: dest,
        committer: signature("Landing"),
    }
}
fn decoded(candidate: &Candidate, id: [u8; 32]) -> m::Object {
    let object = candidate
        .objects
        .iter()
        .find(|o| o.descriptor.key.id.0 == id)
        .expect("generated object");
    m::decode(&object.bytes, m::Limits::default()).unwrap()
}

#[test]
fn explicit_mapped_parent_rebases_a_dependent_patch_without_losing_destination_changes() {
    let mut r = Reader::default();
    let empty = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, empty, 1);
    let parent_tree = r.tree(vec![("a", file(b"a", false))]);
    let parent = r.commit(base, parent_tree, 2);
    let destination_tree = r.tree(vec![("d", file(b"destination", false))]);
    let destination = r.commit(base, destination_tree, 3);
    let rebased_parent = build(
        &mut r,
        request(base, parent, destination),
        Limits::default(),
    )
    .unwrap();
    for object in &rebased_parent.objects {
        r.objects
            .insert(object.descriptor.key, object.bytes.clone());
    }
    let child_tree = r.tree(vec![("a", file(b"a", false)), ("b", file(b"child", false))]);
    let child = r.commit(parent, child_tree, 4);
    let input = request(parent, child, rebased_parent.head);
    assert!(
        build(&mut r, input.clone(), Limits::default()).is_err(),
        "ordinary build must retain its common-ancestor contract"
    );
    let candidate =
        build_on_mapped_base(&mut r, input, rebased_parent.head, Limits::default()).unwrap();
    assert_eq!(
        entries(&candidate, candidate.root_tree)
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "d"]
    );
    let m::Object::Commit(commit) = decoded(&candidate, candidate.head.0) else {
        panic!()
    };
    assert_eq!(commit.parents, [rebased_parent.head]);
    assert_eq!(commit.change_id, m::ChangeId([4; 16]));
    assert_eq!(
        candidate.rewrites.iter().map(|p| p.old).collect::<Vec<_>>(),
        [child]
    );
    assert!(
        build_on_mapped_base(
            &mut r,
            request(parent, child, destination),
            destination,
            Limits::default()
        )
        .is_err(),
        "unrelated logical base must be rejected"
    );
}
fn entries(candidate: &Candidate, id: m::TreeId) -> Vec<m::TreeEntry> {
    let m::Object::Tree(t) = decoded(candidate, id.0) else {
        panic!()
    };
    t.entries
}

#[test]
fn disjoint_stack_is_deterministic_preserves_identity_and_empty_changes() {
    let mut r = Reader::default();
    let empty = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, empty, 1);
    let a = r.tree(vec![("a", file(b"a", false))]);
    let first = r.commit(base, a, 2);
    let mut second_c = commit(first, a, 3);
    second_c.predecessors = vec![base];
    let second = m::CommitId(r.put(m::Object::Commit(second_c.clone())));
    let d = r.tree(vec![("d", file(b"d", true))]);
    let dest = r.commit(base, d, 4);
    let candidate = build(&mut r, request(base, second, dest), Limits::default()).unwrap();
    assert_eq!(
        candidate.rewrites.iter().map(|x| x.old).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(
        candidate
            .rewrites
            .iter()
            .map(|x| x.change_id)
            .collect::<Vec<_>>(),
        [m::ChangeId([2; 16]), m::ChangeId([3; 16])]
    );
    let m::Object::Commit(c1) = decoded(&candidate, candidate.rewrites[0].new.0) else {
        panic!()
    };
    let m::Object::Commit(c2) = decoded(&candidate, candidate.head.0) else {
        panic!()
    };
    assert_eq!(c1.parents, [dest]);
    assert_eq!(c2.parents, [candidate.rewrites[0].new]);
    assert_eq!(c1.root_tree, c2.root_tree);
    assert_eq!(c2.author, second_c.author);
    assert_eq!(c2.description, second_c.description);
    assert_eq!(c2.predecessors, [base]);
    assert_eq!(c2.committer, signature("Landing"));
    assert_eq!(
        entries(&candidate, candidate.root_tree)
            .iter()
            .map(|x| x.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "d"]
    );
    assert_eq!(
        build(&mut r, request(base, second, dest), Limits::default()).unwrap(),
        candidate
    );
}

#[test]
fn nested_disjoint_entries_preserve_deletions_modes_symlinks_and_copy_metadata() {
    let mut r = Reader::default();
    let unchanged = r.tree(vec![("not-opened", file(b"unrelated", false))]);
    let base_child = r.tree(vec![
        ("delete", file(b"gone", false)),
        ("mode", file(b"same", false)),
    ]);
    let base_tree = r.tree(vec![
        ("dir", m::TreeValue::Tree(base_child)),
        ("unchanged", m::TreeValue::Tree(unchanged)),
    ]);
    let base = r.commit(m::CommitId::ROOT, base_tree, 1);
    let copy = m::CopyId(r.put(m::Object::CopyHistory(m::CopyHistory {
        current_path: "dir/copied".into(),
        parents: vec![],
        salt: vec![7],
    })));
    let copied = m::TreeValue::File {
        id: m::file_id(b"copy"),
        size_bytes: 4,
        executable: false,
        copy_id: Some(copy),
    };
    let source_child = r.tree(vec![
        ("copied", copied.clone()),
        ("mode", file(b"same", true)),
    ]);
    let source_tree = r.tree(vec![
        ("dir", m::TreeValue::Tree(source_child)),
        ("unchanged", m::TreeValue::Tree(unchanged)),
    ]);
    let source = r.commit(base, source_tree, 2);
    let link = m::TreeValue::Symlink {
        id: m::SymlinkId([8; 32]),
        target_bytes: 3,
    };
    let dest_child = r.tree(vec![
        ("delete", file(b"gone", false)),
        ("link", link.clone()),
        ("mode", file(b"same", false)),
    ]);
    let dest_tree = r.tree(vec![
        ("dir", m::TreeValue::Tree(dest_child)),
        ("unchanged", m::TreeValue::Tree(unchanged)),
    ]);
    let dest = r.commit(base, dest_tree, 3);
    let candidate = build(&mut r, request(base, source, dest), Limits::default()).unwrap();
    let root = entries(&candidate, candidate.root_tree);
    let m::TreeValue::Tree(child) = root[0].value else {
        panic!()
    };
    let got = entries(&candidate, child);
    assert_eq!(
        got,
        vec![
            m::TreeEntry {
                name: "copied".into(),
                value: copied
            },
            m::TreeEntry {
                name: "link".into(),
                value: link
            },
            m::TreeEntry {
                name: "mode".into(),
                value: file(b"same", true)
            }
        ]
    );
    assert!(!r.reads.contains(&key(m::Kind::Tree, unchanged.0)));
}

#[test]
fn same_path_divergence_and_type_replacements_are_typed_conflicts() {
    for dest_value in [file(b"dest", false), m::TreeValue::Tree(m::TreeId([9; 32]))] {
        let mut r = Reader::default();
        let b = r.tree(vec![("f", file(b"base", false))]);
        let s = r.tree(vec![("f", file(b"source", false))]);
        let d = r.tree(vec![("f", dest_value)]);
        let base = r.commit(m::CommitId::ROOT, b, 1);
        let source = r.commit(base, s, 2);
        let dest = r.commit(base, d, 3);
        assert_eq!(
            build(&mut r, request(base, source, dest), Limits::default()),
            Err(Error::Conflict { path: "f".into() })
        );
    }
}

#[test]
fn equal_tree_fast_paths_do_not_open_tree_payloads() {
    for (base_tree, source_tree, dest_tree, expected) in [(1, 1, 3, 3), (1, 2, 1, 2), (1, 2, 2, 2)]
    {
        let mut r = Reader::default();
        let b = m::TreeId([base_tree; 32]);
        let s = m::TreeId([source_tree; 32]);
        let d = m::TreeId([dest_tree; 32]);
        let base = r.commit(m::CommitId::ROOT, b, 1);
        let source = r.commit(base, s, 2);
        let dest = r.commit(base, d, 3);
        let candidate = build(&mut r, request(base, source, dest), Limits::default()).unwrap();
        assert_eq!(candidate.root_tree, m::TreeId([expected; 32]));
        assert!(r.reads.iter().all(|k| k.kind == m::Kind::Commit));
    }
}

#[test]
fn source_rejections_and_destination_ancestry_are_explicit() {
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let a = r.commit(base, t, 2);
    let duplicate = r.commit(a, t, 2);
    assert_eq!(
        build(&mut r, request(base, duplicate, base), Limits::default()),
        Err(Error::Invalid("duplicate source ChangeId"))
    );
    assert_eq!(
        build(&mut r, request(base, base, base), Limits::default()),
        Err(Error::Invalid("empty source stack"))
    );
    let unrelated = r.commit(m::CommitId::ROOT, t, 3);
    assert_eq!(
        build(&mut r, request(base, a, unrelated), Limits::default()),
        Err(Error::Invalid("base is not on destination ancestry"))
    );
    let mut merge = commit(base, t, 4);
    merge.parents.push(unrelated);
    let merge = m::CommitId(r.put(m::Object::Commit(merge)));
    assert_eq!(
        build(&mut r, request(base, merge, base), Limits::default()),
        Err(Error::UnsupportedHistory("source merge"))
    );
    assert_eq!(
        build(&mut r, request(base, a, merge), Limits::default()),
        Err(Error::UnsupportedHistory("destination merge"))
    );
    let mut unresolved = commit(base, t, 5);
    unresolved.root_tree = m::TreeMerge {
        terms: vec![t, t, t],
        labels: vec![String::new(); 3],
    };
    let unresolved = m::CommitId(r.put(m::Object::Commit(unresolved)));
    assert_eq!(
        build(&mut r, request(base, unresolved, base), Limits::default()),
        Err(Error::UnsupportedHistory("unresolved tree"))
    );
    let mut signed = commit(base, t, 6);
    signed.secure_signature = Some(m::SecureSignature {
        data: m::unsigned_commit_bytes(&signed, m::Limits::default()).unwrap(),
        signature: vec![1],
    });
    let signed = m::CommitId(r.put(m::Object::Commit(signed)));
    assert_eq!(
        build(&mut r, request(base, signed, base), Limits::default()),
        Err(Error::UnsupportedHistory("signed source commit"))
    );
}

#[test]
fn corrupted_metadata_and_mismatched_copy_path_are_rejected() {
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let source = r.commit(base, t, 2);
    r.objects
        .get_mut(&key(m::Kind::Commit, source.0))
        .unwrap()
        .push(0);
    assert_eq!(
        build(&mut r, request(base, source, base), Limits::default()),
        Err(Error::Object(m::Error::HashMismatch))
    );
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let copy = m::CopyId(r.put(m::Object::CopyHistory(m::CopyHistory {
        current_path: "wrong".into(),
        parents: vec![],
        salt: vec![],
    })));
    let s = r.tree(vec![(
        "actual",
        m::TreeValue::File {
            id: m::file_id(b"a"),
            size_bytes: 1,
            executable: false,
            copy_id: Some(copy),
        },
    )]);
    let source = r.commit(base, s, 2);
    let d = r.tree(vec![("other", file(b"d", false))]);
    let dest = r.commit(base, d, 3);
    assert_eq!(
        build(&mut r, request(base, source, dest), Limits::default()),
        Err(Error::Invalid("copy history current path"))
    );
}

#[test]
fn resource_limits_fail_before_unbounded_reads_or_output() {
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let a = r.commit(base, t, 2);
    let b = r.commit(a, t, 3);
    let cases = [
        (
            Limits {
                max_stack: 1,
                ..Limits::default()
            },
            Error::Limit(Budget::Stack),
        ),
        (
            Limits {
                max_reads: 1,
                ..Limits::default()
            },
            Error::Limit(Budget::Reads),
        ),
        (
            Limits {
                max_work: 1,
                ..Limits::default()
            },
            Error::Limit(Budget::Work),
        ),
        (
            Limits {
                max_output_objects: 1,
                ..Limits::default()
            },
            Error::Limit(Budget::OutputObjects),
        ),
    ];
    for (limits, expected) in cases {
        assert_eq!(build(&mut r, request(base, b, base), limits), Err(expected));
    }
    let limit = Limits {
        max_metadata_bytes: 32,
        ..Limits::default()
    };
    assert!(matches!(
        build(&mut r, request(base, a, base), limit),
        Err(Error::Read {
            error: ReadError::Limit,
            ..
        })
    ));
    assert_eq!(*r.allowances.last().unwrap(), 32);
    assert!(
        build(
            &mut r,
            request(base, a, base),
            Limits {
                max_output_bytes: 1,
                ..Limits::default()
            }
        )
        .is_err()
    );
}

#[test]
fn subsequent_changed_commit_reads_previously_constructed_tree_without_reader_upload() {
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let a = r.tree(vec![("a", file(b"first", false))]);
    let first = r.commit(base, a, 2);
    let b = r.tree(vec![("a", file(b"second", false))]);
    let second = r.commit(first, b, 3);
    let d = r.tree(vec![("d", file(b"dest", false))]);
    let dest = r.commit(base, d, 4);
    let candidate = build(&mut r, request(base, second, dest), Limits::default()).unwrap();
    assert_eq!(
        entries(&candidate, candidate.root_tree),
        vec![
            m::TreeEntry {
                name: "a".into(),
                value: file(b"second", false)
            },
            m::TreeEntry {
                name: "d".into(),
                value: file(b"dest", false)
            }
        ]
    );
    assert_eq!(candidate.rewrites.len(), 2);
    for object in &candidate.objects {
        assert!(!r.reads.contains(&object.descriptor.key));
    }
}

#[test]
fn already_present_source_identity_or_change_is_not_duplicated_as_an_empty_rewrite() {
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let source = r.commit(base, t, 2);
    let descendant = r.commit(source, t, 3);
    let mut rewritten = commit(base, t, 2);
    rewritten.description = "same change, independently rewritten".into();
    let rewritten = m::CommitId(r.put(m::Object::Commit(rewritten)));
    assert_ne!(rewritten, source);
    for dest in [source, descendant, rewritten] {
        assert_eq!(
            build(&mut r, request(base, source, dest), Limits::default()),
            Err(Error::UnsupportedHistory(
                "source change already on destination"
            ))
        );
    }
    let duplicate_base = r.commit(base, t, 1);
    assert_eq!(
        build(
            &mut r,
            request(base, duplicate_base, base),
            Limits::default()
        ),
        Err(Error::Invalid("source ChangeId equals base ChangeId"))
    );
}

#[test]
fn nested_path_depth_and_ancestry_limits_stop_the_rebase() {
    let mut r = Reader::default();
    let b = r.tree(vec![("base", file(b"b", false))]);
    let s = r.tree(vec![
        ("base", file(b"b", false)),
        ("source", file(b"s", false)),
    ]);
    let d = r.tree(vec![
        ("base", file(b"b", false)),
        ("dest", file(b"d", false)),
    ]);
    let b = r.tree(vec![("directory", m::TreeValue::Tree(b))]);
    let s = r.tree(vec![("directory", m::TreeValue::Tree(s))]);
    let d = r.tree(vec![("directory", m::TreeValue::Tree(d))]);
    let base = r.commit(m::CommitId::ROOT, b, 1);
    let source = r.commit(base, s, 2);
    let dest = r.commit(base, d, 3);
    assert_eq!(
        build(
            &mut r,
            request(base, source, dest),
            Limits {
                max_depth: 1,
                ..Limits::default()
            }
        ),
        Err(Error::Limit(Budget::Depth))
    );
    assert_eq!(
        build(
            &mut r,
            request(base, source, dest),
            Limits {
                max_path_bytes: 12,
                ..Limits::default()
            }
        ),
        Err(Error::Limit(Budget::Path))
    );
    let next = r.commit(dest, d, 4);
    assert_eq!(
        build(
            &mut r,
            request(base, source, next),
            Limits {
                max_ancestry: 1,
                ..Limits::default()
            }
        ),
        Err(Error::Limit(Budget::Ancestry))
    );
    assert_eq!(
        build(
            &mut r,
            request(base, source, dest),
            Limits {
                max_output_bytes: 200,
                ..Limits::default()
            }
        ),
        Err(Error::Limit(Budget::OutputBytes))
    );
}

#[test]
fn full_output_followed_by_deletion_fits_tree_entry_limit() {
    let mut r = Reader::default();
    let b = r.tree(vec![
        ("b", file(b"base", false)),
        ("c", file(b"delete", false)),
    ]);
    let s = r.tree(vec![
        ("a", file(b"add", false)),
        ("b", file(b"base", false)),
    ]);
    let d = r.tree(vec![
        ("b", file(b"dest", false)),
        ("c", file(b"delete", false)),
    ]);
    let base = r.commit(m::CommitId::ROOT, b, 1);
    let source = r.commit(base, s, 2);
    let dest = r.commit(base, d, 3);
    let candidate = build(
        &mut r,
        request(base, source, dest),
        Limits {
            max_entries_per_tree: 2,
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(
        entries(&candidate, candidate.root_tree)
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
}

#[test]
fn exact_fast_forward_can_preserve_commit_identity_without_new_evolution() {
    let mut r = Reader::default();
    let t = r.tree(vec![]);
    let base = r.commit(m::CommitId::ROOT, t, 1);
    let source = r.commit(base, t, 2);
    let mut request = request(base, source, base);
    request.committer = signature("Old committer");
    let candidate = build(&mut r, request, Limits::default()).unwrap();
    assert_eq!(candidate.head, source);
    assert_eq!(
        candidate.rewrites,
        [Rewrite {
            old: source,
            new: source,
            change_id: m::ChangeId([2; 16])
        }]
    );
}

#[test]
fn projected_directory_deletion_preserves_excluded_descendants_and_canonical_copy_identity() {
    let mut r = Reader::default();
    let canonical_directory = r.tree(vec![
        ("private", file(b"excluded", false)),
        ("visible", file(b"visible", false)),
    ]);
    let canonical_tree = r.tree(vec![("folder", m::TreeValue::Tree(canonical_directory))]);
    let canonical = r.commit(m::CommitId::ROOT, canonical_tree, 1);
    let copy = m::CopyId(r.put(m::Object::CopyHistory(m::CopyHistory {
        current_path: "folder/visible".into(),
        parents: vec![],
        salt: vec![1],
    })));
    let mut visible = file(b"visible", false);
    let m::TreeValue::File { copy_id, .. } = &mut visible else {
        panic!()
    };
    *copy_id = Some(copy);
    let projected_directory = r.tree(vec![("visible", visible)]);
    let projected_tree = r.tree(vec![("folder", m::TreeValue::Tree(projected_directory))]);
    let projected = r.commit(m::CommitId::ROOT, projected_tree, 8);
    let empty = r.tree(vec![]);
    let source = r.commit(projected, empty, 9);
    let candidate = build_on_projected_base(
        &mut r,
        request(projected, source, canonical),
        canonical,
        Limits::default(),
    )
    .unwrap();
    let root = entries(&candidate, candidate.root_tree);
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].name, "folder");
    let m::TreeValue::Tree(folder) = root[0].value else {
        panic!()
    };
    let retained = entries(&candidate, folder);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].name, "private");
    assert_eq!(retained[0].value, file(b"excluded", false));
    let m::Object::Commit(head) = decoded(&candidate, candidate.head.0) else {
        panic!()
    };
    assert_eq!(head.parents, [canonical]);
}
