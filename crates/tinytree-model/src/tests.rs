use super::*;
fn commit() -> Commit {
    let person = |name: &str, millis, offset| Signature {
        name: name.into(),
        email: "author@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: millis,
            tz_offset_minutes: offset,
        },
    };
    Commit {
        parents: vec![CommitId::ROOT, CommitId([1; 32])],
        predecessors: vec![CommitId([2; 32]), CommitId([1; 32])],
        root_tree: TreeMerge {
            terms: vec![
                TreeId([3; 32]),
                TreeId([4; 32]),
                TreeId([3; 32]),
                TreeId([5; 32]),
                TreeId([6; 32]),
            ],
            labels: ["left", "base one", "left again", "base two", "right"]
                .map(str::to_owned)
                .to_vec(),
        },
        change_id: ChangeId([7; 16]),
        description: "native merge\nUnicode: λ\n".into(),
        author: person("Author", -1234567, 90),
        committer: person("Committer", 1234567, -90),
        secure_signature: None,
    }
}
#[test]
fn file_hash_and_empty_tree_have_fixed_public_vectors() {
    let file = describe_file(b"abc");
    assert_eq!(file.key.kind, Kind::File);
    assert_eq!(file.size_bytes, 3);
    assert_eq!(
        blake3::Hash::from(file.key.id.0).to_hex().as_str(),
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
    );
    let empty = empty_tree();
    assert_eq!(empty.bytes, b"TTREE\0\0\0\0\x01\x02\0\0\0\0");
    assert_eq!(
        decode(&empty.bytes, Limits::default()).unwrap(),
        Object::Tree(Tree::default())
    );
    assert_ne!(empty.descriptor.key.id.0, CommitId::ROOT.0);
    let root = synthetic_root();
    assert!(root.parents.is_empty());
    assert_eq!(root.change_id, ChangeId::ROOT);
    assert_eq!(
        root.root_tree.terms,
        vec![TreeId(empty.descriptor.key.id.0)]
    );
    assert!(encode(&Object::Commit(root), Limits::default()).is_err());
}
#[test]
fn all_commit_fields_merge_order_and_signature_binding_round_trip() {
    let limits = Limits::default();
    let mut value = commit();
    let unsigned = unsigned_commit_bytes(&value, limits).unwrap();
    // Fixed-width reference bytes were independently assembled with Python's
    // struct module and hashed with b3sum, rather than this codec.
    assert_eq!(unsigned.len(), 515);
    assert_eq!(
        blake3::hash(&unsigned).to_hex().as_str(),
        "9b192321709c72f7e6350b551eaa9dceb13db5b1da487a3708a790f97db7de32"
    );
    value.secure_signature = Some(SecureSignature {
        data: unsigned.clone(),
        signature: vec![0, 255, 1, 128],
    });
    let encoded = encode(&Object::Commit(value.clone()), limits).unwrap();
    assert_eq!(encoded.bytes.len(), 1042);
    assert_eq!(
        blake3::Hash::from(encoded.descriptor.key.id.0)
            .to_hex()
            .as_str(),
        "95668e6e51cf08fde4b5a19ba8c4632364d4c69acfa54aae4bd93f4a20181022"
    );
    assert_eq!(unsigned_commit_bytes(&value, limits).unwrap(), unsigned);
    assert_eq!(
        decode(&encoded.bytes, limits).unwrap(),
        Object::Commit(value.clone())
    );
    verify(&encoded.descriptor, &encoded.bytes, limits).unwrap();
    let again = encode(&Object::Commit(value.clone()), limits).unwrap();
    assert_eq!(again, encoded);
    assert_ne!(
        encoded.descriptor.key.id.0,
        *blake3::hash(&unsigned).as_bytes()
    );
    value.description.push('!');
    assert!(matches!(
        encode(&Object::Commit(value), limits),
        Err(Error::Invalid("signature preimage"))
    ));
}
#[test]
fn tree_modes_lengths_copy_ids_symlinks_and_copy_ancestry_round_trip() {
    let objects = vec![
        (
            Object::Tree(Tree {
                entries: vec![
                    TreeEntry {
                        name: "a".into(),
                        value: TreeValue::File {
                            id: file_id(b""),
                            executable: false,
                            size_bytes: 0,
                            copy_id: None,
                        },
                    },
                    TreeEntry {
                        name: "b".into(),
                        value: TreeValue::File {
                            id: file_id(b"exec"),
                            executable: true,
                            size_bytes: 4,
                            copy_id: Some(CopyId([9; 32])),
                        },
                    },
                    TreeEntry {
                        name: "link".into(),
                        value: TreeValue::Symlink {
                            id: SymlinkId([1; 32]),
                            target_bytes: 5,
                        },
                    },
                    TreeEntry {
                        name: "sub".into(),
                        value: TreeValue::Tree(TreeId([2; 32])),
                    },
                ],
            }),
            "64aa6c9dc48fefd5f87308f335a61619cea80554f2dc7772b062c2828eacda9c",
            232,
        ),
        (
            Object::Symlink("../λ".into()),
            "7c090f94eb098e1f9124d43c39217e9866d481b89da6eeb0d99587a7f0f2cbb7",
            20,
        ),
        (
            Object::CopyHistory(CopyHistory {
                current_path: "src/λ".into(),
                parents: vec![CopyId([2; 32]), CopyId([1; 32]), CopyId([2; 32])],
                salt: vec![0, 255, 0],
            }),
            "986023008a86c2579e6034036fe662ae741c20e6e8072f595c9a0c895c28a809",
            128,
        ),
    ];
    for (object, expected_hash, expected_size) in objects {
        let encoded = encode(&object, Limits::default()).unwrap();
        assert_eq!(encoded.bytes.len(), expected_size);
        assert_eq!(
            blake3::Hash::from(encoded.descriptor.key.id.0)
                .to_hex()
                .as_str(),
            expected_hash
        );
        assert_eq!(decode(&encoded.bytes, Limits::default()).unwrap(), object);
        verify(&encoded.descriptor, &encoded.bytes, Limits::default()).unwrap();
    }
}
#[test]
fn closure_keeps_negative_merge_terms_predecessors_and_copy_history() {
    let object = Object::Commit(commit());
    let refs = object.references();
    assert!(refs.contains(&ObjectKey {
        kind: Kind::Tree,
        id: ObjectId([4; 32])
    }));
    assert!(refs.contains(&ObjectKey {
        kind: Kind::Tree,
        id: ObjectId([5; 32])
    }));
    assert!(refs.contains(&ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([2; 32])
    }));
    assert!(!refs.contains(&ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32])
    }));
    let copies = Object::CopyHistory(CopyHistory {
        current_path: "a".into(),
        parents: vec![CopyId([7; 32])],
        salt: vec![],
    });
    assert_eq!(
        copies.references(),
        vec![ObjectKey {
            kind: Kind::CopyHistory,
            id: ObjectId([7; 32])
        }]
    );
}
#[test]
fn malformed_names_order_merge_labels_and_symlinks_are_rejected() {
    for name in ["", ".", "..", "a/b", "a\0b"] {
        let object = Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: name.into(),
                value: TreeValue::Tree(TreeId([1; 32])),
            }],
        });
        assert!(
            encode(&object, Limits::default()).is_err(),
            "name must be rejected"
        );
    }
    for names in [["b", "a"], ["a", "a"]] {
        let tree = Tree {
            entries: names
                .map(|name| TreeEntry {
                    name: name.into(),
                    value: TreeValue::Tree(TreeId([1; 32])),
                })
                .to_vec(),
        };
        assert!(encode(&Object::Tree(tree), Limits::default()).is_err());
    }
    for invalid in [
        TreeMerge {
            terms: vec![],
            labels: vec![],
        },
        TreeMerge {
            terms: vec![TreeId([1; 32]); 2],
            labels: vec![String::new(); 2],
        },
        TreeMerge {
            terms: vec![TreeId([1; 32])],
            labels: vec!["not resolved".into()],
        },
        TreeMerge {
            terms: vec![TreeId([1; 32]); 3],
            labels: vec![String::new()],
        },
    ] {
        let mut value = commit();
        value.root_tree = invalid;
        assert!(encode(&Object::Commit(value), Limits::default()).is_err());
    }
    assert!(encode(&Object::Symlink("a\0b".into()), Limits::default()).is_err());
    for path in ["", "/a", "a/", "a//b", "a/../b"] {
        assert!(
            encode(
                &Object::CopyHistory(CopyHistory {
                    current_path: path.into(),
                    parents: vec![],
                    salt: vec![]
                }),
                Limits::default()
            )
            .is_err()
        );
    }
}
#[test]
fn decode_rejects_every_truncation_unknown_domains_and_trailing_bytes() {
    let encoded = encode(&Object::Commit(commit()), Limits::default()).unwrap();
    for end in 0..encoded.bytes.len() {
        assert!(decode(&encoded.bytes[..end], Limits::default()).is_err());
    }
    let mut changed = empty_tree().bytes;
    changed.push(0);
    assert!(matches!(
        decode(&changed, Limits::default()),
        Err(Error::Invalid("trailing bytes"))
    ));
    let mut unknown = empty_tree().bytes;
    unknown[9] = 2;
    assert_eq!(
        decode(&unknown, Limits::default()),
        Err(Error::UnsupportedVersion)
    );
    unknown[9] = 1;
    unknown[10] = 255;
    assert_eq!(
        decode(&unknown, Limits::default()),
        Err(Error::UnsupportedKind)
    );
    unknown[10] = Kind::File as u8;
    assert_eq!(
        decode(&unknown, Limits::default()),
        Err(Error::UnsupportedKind)
    );
    unknown[10] = Kind::Tree as u8;
    unknown[11..15].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(decode(&unknown, Limits::default()), Err(Error::Limit));
}
#[test]
fn descriptors_validate_size_hash_kind_and_decode_limits() {
    let limits = Limits::default();
    let encoded = encode(&Object::Symlink("target".into()), limits).unwrap();
    let mut desc = encoded.descriptor;
    desc.size_bytes += 1;
    assert_eq!(
        verify(&desc, &encoded.bytes, limits),
        Err(Error::SizeMismatch)
    );
    desc = encoded.descriptor;
    desc.key.id.0[0] ^= 1;
    assert_eq!(
        verify(&desc, &encoded.bytes, limits),
        Err(Error::HashMismatch)
    );
    desc = encoded.descriptor;
    desc.key.kind = Kind::Tree;
    assert!(verify(&desc, &encoded.bytes, limits).is_err());
    assert_eq!(
        decode(
            &encoded.bytes,
            Limits {
                max_object_bytes: 1,
                ..limits
            }
        ),
        Err(Error::Limit)
    );
    assert!(
        encode(
            &Object::Commit(commit()),
            Limits {
                max_items: 1,
                ..limits
            }
        )
        .is_err()
    );
    assert!(
        encode(
            &Object::Symlink("long".into()),
            Limits {
                max_field_bytes: 1,
                ..limits
            }
        )
        .is_err()
    );
    assert!(FileId::from_slice(&[0; 31]).is_err());
    assert!(ChangeId::from_slice(&[0; 32]).is_err());
}
#[test]
fn malformed_utf8_and_noncanonical_booleans_are_not_normalized() {
    let encoded = encode(&Object::Symlink("x".into()), Limits::default()).unwrap();
    let mut bytes = encoded.bytes;
    *bytes.last_mut().unwrap() = 255;
    assert!(matches!(
        decode(&bytes, Limits::default()),
        Err(Error::Invalid("UTF-8"))
    ));
    let object = Object::Tree(Tree {
        entries: vec![TreeEntry {
            name: "a".into(),
            value: TreeValue::File {
                id: FileId([1; 32]),
                executable: false,
                size_bytes: 0,
                copy_id: None,
            },
        }],
    });
    let mut bytes = encode(&object, Limits::default()).unwrap().bytes;
    // Header11 + count4 + name length4 + name1 + variant1 + file ID32.
    bytes[53] = 2;
    assert!(matches!(
        decode(&bytes, Limits::default()),
        Err(Error::Invalid("boolean"))
    ));
}
