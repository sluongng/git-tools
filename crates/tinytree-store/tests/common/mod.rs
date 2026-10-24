//! Deterministic native fixtures shared by the independent driver and verifier.
use tinytree_model::{
    ChangeId, Commit, CommitId, Descriptor, EncodedObject, FileId, Limits, Object, Signature,
    Timestamp, Tree, TreeEntry, TreeId, TreeMerge, TreeValue,
};
use tinytree_store::{PublishRequest, Store};

pub const REPOSITORY: &str = "demo";
pub const REFERENCE: &str = "main";
pub const UNACKNOWLEDGED_BYTES: &[u8] = b"unacknowledged native file";

pub struct Fixture {
    pub file: Descriptor,
    pub file_bytes: Vec<u8>,
    pub metadata: Vec<EncodedObject>,
    pub head: Descriptor,
}

fn fixture(label: &str, parent: CommitId, change: u8) -> Fixture {
    let file_bytes = format!("native {label}\n").into_bytes();
    let file = tinytree_model::describe_file(&file_bytes);
    let tree = tinytree_model::encode(
        &Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "source.txt".into(),
                value: TreeValue::File {
                    id: FileId(file.key.id.0),
                    executable: false,
                    size_bytes: file.size_bytes,
                    copy_id: None,
                },
            }],
        }),
        Limits::default(),
    )
    .unwrap();
    let person = Signature {
        name: "Fixture".into(),
        email: "fixture@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let commit = tinytree_model::encode(
        &Object::Commit(Commit {
            parents: vec![parent],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![TreeId(tree.descriptor.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: ChangeId([change; 16]),
            description: label.into(),
            author: person.clone(),
            committer: person,
            secure_signature: None,
        }),
        Limits::default(),
    )
    .unwrap();
    let head = commit.descriptor;
    Fixture {
        file,
        file_bytes,
        metadata: vec![tree, commit],
        head,
    }
}
pub fn initial() -> Fixture {
    fixture("initial", CommitId::ROOT, 1)
}
pub fn replacement() -> Fixture {
    fixture("replacement", CommitId(initial().head.key.id.0), 2)
}
pub fn upload(store: &Store, fixture: &Fixture) -> tinytree_store::Result<()> {
    store.put(REPOSITORY, fixture.file, &fixture.file_bytes, 100)?;
    for object in &fixture.metadata {
        store.put(REPOSITORY, object.descriptor, &object.bytes, 100)?;
    }
    Ok(())
}
pub fn publication(key: &str, generation: u64, head: Descriptor) -> PublishRequest {
    PublishRequest {
        idempotency_key: key.into(),
        reference_name: REFERENCE.into(),
        expected_generation: generation,
        new_commit: Some(CommitId(head.key.id.0)),
        staging_leases: vec![],
    }
}

pub fn retention_publication(
    key: &str,
    generation: u64,
    head: Descriptor,
    acquire: bool,
) -> PublishRequest {
    PublishRequest {
        reference_name: tinytree_store::retention_reference(
            tinytree_store::RetentionOwner {
                principal: [1; 32],
                client: [2; 16],
            },
            tinytree_model::CommitId(head.key.id.0),
        )
        .unwrap(),
        idempotency_key: key.to_owned(),
        expected_generation: generation,
        new_commit: acquire.then_some(tinytree_model::CommitId(head.key.id.0)),
        staging_leases: vec![],
    }
}

pub const LANDING_OWNER: tinytree_store::LandingOwner = tinytree_store::LandingOwner([7; 32]);
pub fn landing_request() -> tinytree_store::LandingRequest {
    let person = Signature {
        name: "Landing fixture".into(),
        email: "landing@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 100,
            tz_offset_minutes: 0,
        },
    };
    tinytree_store::LandingRequest {
        idempotency_key: "landing".into(),
        source_base: CommitId(initial().head.key.id.0),
        source_head: CommitId(replacement().head.key.id.0),
        reference_name: REFERENCE.into(),
        expected_generation: 1,
        expected_head: CommitId(initial().head.key.id.0),
        committer: person,
        check_profile: tinytree_store::LandingCheckProfile {
            name: "fixture".into(),
            version: "v1".into(),
        },
        expires_at: 10000,
    }
}
pub fn landing_identity(job: &tinytree_store::LandingJob) -> tinytree_store::LandingCheckIdentity {
    let candidate = job.candidate.as_ref().unwrap();
    tinytree_store::LandingCheckIdentity {
        candidate: candidate.head,
        root_tree: candidate.root_tree,
        profile: job.profile.clone(),
    }
}
