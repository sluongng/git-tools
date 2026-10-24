use std::{net::SocketAddr, time::Duration};
use tinytree_fixture::{Fixture, FixtureConfig, build_fixture};
use tinytree_model::{CommitId, Descriptor, Kind, Limits, Object, ObjectId, ObjectKey, TreeId};
use tinytree_proto::query_v1 as q;
use tinytree_proto::v1::{self as p, repository_client::RepositoryClient};
use tinytree_server::{Principal, RepositoryService, ServiceConfig, serve_tcp};
use tinytree_store::Store;
use tokio::sync::{mpsc, oneshot};
use tonic::{
    Code, Request, Status,
    codec::CompressionEncoding,
    transport::{Channel, Endpoint},
};

const ALPHA: &str = "alpha-test-token-not-a-secret";
const BETA: &str = "beta-test-token-not-a-secret";
const READER: &str = "reader-test-token-not-a-secret";
const PEER: &str = "peer-test-token-not-a-secret";

struct Running {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}
impl Running {
    async fn start(store: Store, config: ServiceConfig) -> Self {
        Self::start_with_peer_token(store, config, PEER).await
    }
    async fn start_with_peer_token(store: Store, config: ServiceConfig, peer_token: &str) -> Self {
        Self::start_services(store, config, peer_token, None).await
    }
    async fn start_query(
        store: Store,
        config: ServiceConfig,
        index: std::sync::Arc<tinytree_index::Index>,
    ) -> Self {
        Self::start_services(store, config, PEER, Some(index)).await
    }
    async fn start_services(
        store: Store,
        config: ServiceConfig,
        peer_token: &str,
        index: Option<std::sync::Arc<tinytree_index::Index>>,
    ) -> Self {
        let principals = vec![
            Principal::new("alpha", ALPHA, vec!["alpha".into()], true)
                .unwrap()
                .with_cloud(config.cloud.enabled),
            Principal::new("beta", BETA, vec!["beta".into()], true).unwrap(),
            Principal::new("reader", READER, vec!["alpha".into()], false)
                .unwrap()
                .with_retention(true)
                .with_cloud(config.cloud.enabled),
            Principal::new("peer", peer_token, vec!["alpha".into()], true)
                .unwrap()
                .with_retention(true)
                .with_cloud(config.cloud.enabled),
        ];
        let service = RepositoryService::new(
            store,
            config,
            vec!["alpha".into(), "beta".into()],
            principals,
        )
        .unwrap();
        let service = match index {
            Some(index) => service.with_query_index(index).unwrap(),
            None => service,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve_tcp(listener, service, None, async {
            let _ = stopped.await;
        }));
        Self {
            address,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn client(&self) -> RepositoryClient<Channel> {
        let channel = Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await
            .unwrap();
        RepositoryClient::new(channel)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(20 * 1024 * 1024)
    }
    async fn start_production(mut config: tinytree_server::Config) -> Self {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        config.listen = address.to_string();
        let repository = config.repositories[0].clone();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(tinytree_server::serve(config, async {
            let _ = stopped.await;
        }));
        let running = Self {
            address,
            stop: Some(stop),
            task: Some(task),
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    !running.task.as_ref().unwrap().is_finished(),
                    "production startup failed"
                );
                if let Ok(channel) = Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect()
                    .await
                {
                    let mut client = RepositoryClient::new(channel);
                    client
                        .open_repository(p::OpenRepositoryRequest {
                            repository_id: repository.clone(),
                            reference_names: Vec::new(),
                        })
                        .await
                        .unwrap();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        running
    }
    async fn query_client(&self) -> q::repository_query_client::RepositoryQueryClient<Channel> {
        let channel = Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .unwrap();
        q::repository_query_client::RepositoryQueryClient::new(channel)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
    }
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
fn request<T>(value: T, token: &str) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}
fn wire_key(key: ObjectKey) -> p::ObjectKey {
    p::ObjectKey {
        kind: key.kind as i32,
        native_id: key.id.0.to_vec(),
    }
}
fn descriptor(d: Descriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        key: Some(wire_key(d.key)),
        content_blake3: d.key.id.0.to_vec(),
        size_bytes: d.size_bytes,
        object_format_version: d.format_version as u32,
    }
}
fn root(fixture: &Fixture) -> ObjectKey {
    let bytes = decode_hex(&fixture.manifest.root_commit);
    ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(bytes.try_into().unwrap()),
    }
}
fn decode_hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks_exact(2)
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
fn fixture() -> Fixture {
    build_fixture(FixtureConfig {
        unused_files: 3,
        bytes_per_file: 3000,
    })
    .unwrap()
}
fn setup() -> (tempfile::TempDir, Store) {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(temporary.path(), Default::default()).unwrap();
    store.create_repository("alpha").unwrap();
    store.create_repository("beta").unwrap();
    (temporary, store)
}
fn frames(repo: &str, d: Descriptor, bytes: &[u8], size: usize) -> Vec<p::WriteFrame> {
    let mut result = vec![p::WriteFrame {
        frame: Some(p::write_frame::Frame::Header(p::WriteHeader {
            repository_id: repo.into(),
            descriptor: Some(descriptor(d)),
        })),
    }];
    let mut offset = 0;
    for data in bytes.chunks(size) {
        result.push(p::WriteFrame {
            frame: Some(p::write_frame::Frame::Chunk(p::DataChunk {
                offset,
                data: data.to_vec(),
            })),
        });
        offset += data.len() as u64;
    }
    result
}
async fn upload(
    client: &mut RepositoryClient<Channel>,
    repo: &str,
    token: &str,
    d: Descriptor,
    bytes: &[u8],
) -> Result<p::WriteReceipt, Status> {
    client
        .write_object(request(
            tokio_stream::iter(frames(repo, d, bytes, 1024)),
            token,
        ))
        .await
        .map(|v| v.into_inner())
}
async fn seed(client: &mut RepositoryClient<Channel>, fixture: &Fixture) -> Vec<String> {
    let mut leases = vec![];
    for object in fixture.objects() {
        let receipt = upload(client, "alpha", ALPHA, object.descriptor, &object.bytes)
            .await
            .unwrap();
        assert_eq!(receipt.descriptor, Some(descriptor(object.descriptor)));
        leases.push(receipt.staging_lease);
    }
    leases
}
fn publish(commit: ObjectKey, id: &str, generation: u64) -> p::PublishReferenceRequest {
    p::PublishReferenceRequest {
        repository_id: "alpha".into(),
        idempotency_key: id.into(),
        reference_name: "main".into(),
        expected_generation: generation,
        new_commit: Some(wire_key(commit)),
        staging_leases: vec![],
    }
}

#[tokio::test]
async fn protected_references_reject_ordinary_writes_and_replay_without_affecting_other_scopes() {
    let (_temporary, store) = setup();
    let server = Running::start(store.clone(), ServiceConfig::default()).await;
    let mut client = server.client().await;
    let fixture = fixture();
    seed(&mut client, &fixture).await;
    let head = root(&fixture);
    let original = publish(head, "before-protection", 0);
    let receipt = client
        .publish_reference(request(original.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    server.stop().await;
    let before = store
        .references("alpha", &["main".into(), "new-main".into()])
        .unwrap();
    let mut config = ServiceConfig::default();
    config.protected_references = ["main", "new-main"]
        .into_iter()
        .map(|reference| tinytree_server::ProtectedReference {
            repository: "alpha".into(),
            reference: reference.into(),
        })
        .collect();
    let server = Running::start(store.clone(), config).await;
    let mut client = server.client().await;
    struct Case {
        name: &'static str,
        input: p::PublishReferenceRequest,
    }
    for case in [
        Case {
            name: "create",
            input: p::PublishReferenceRequest {
                reference_name: "new-main".into(),
                ..publish(head, "blocked-create", 0)
            },
        },
        Case {
            name: "update",
            input: publish(head, "blocked-update", 1),
        },
        Case {
            name: "delete",
            input: p::PublishReferenceRequest {
                new_commit: None,
                ..publish(head, "blocked-delete", 1)
            },
        },
        Case {
            name: "replay predating protection",
            input: original,
        },
    ] {
        let error = client
            .publish_reference(request(case.input, ALPHA))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied, "{}", case.name);
        assert_eq!(
            error.message(),
            "reference requires checked landing",
            "{}",
            case.name
        );
        assert_eq!(
            store
                .references("alpha", &["main".into(), "new-main".into()])
                .unwrap(),
            before
        );
    }
    // Policy is checked only after ordinary repository/write authorization.
    for token in [BETA, READER] {
        let error = client
            .publish_reference(request(publish(head, "denied-caller", 1), token))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
        assert_eq!(error.message(), "native repository access denied");
    }
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "blocked-update".into(),
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "before-protection".into(),
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner(),
        receipt
    );
    // Exact matching keeps other names and the same name in another repository writable.
    for name in ["feature", "Main"] {
        let result = client
            .publish_reference(request(
                p::PublishReferenceRequest {
                    reference_name: name.into(),
                    ..publish(head, name, 0)
                },
                ALPHA,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.reference.unwrap().name, name);
    }
    for object in fixture.objects() {
        upload(&mut client, "beta", BETA, object.descriptor, &object.bytes)
            .await
            .unwrap();
    }
    let other = client
        .publish_reference(request(
            p::PublishReferenceRequest {
                repository_id: "beta".into(),
                ..publish(head, "other-repository", 0)
            },
            BETA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(other.reference.unwrap().generation, 1);
    let hold_name = format!(
        "_tinytree/hold/v1/{}/{}",
        "01".repeat(16),
        fixture.manifest.root_commit
    );
    let mut hold = publish(head, "private-hold", 0);
    hold.reference_name = hold_name.clone();
    let retained = client
        .publish_reference(request(hold.clone(), PEER))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retained.reference.unwrap().name, hold_name);
    hold.idempotency_key = "release-private-hold".into();
    hold.expected_generation = 1;
    hold.new_commit = None;
    assert!(
        client
            .publish_reference(request(hold, PEER))
            .await
            .unwrap()
            .into_inner()
            .reference
            .unwrap()
            .commit
            .is_none()
    );
    assert_eq!(
        store
            .references("alpha", &["main".into(), "new-main".into()])
            .unwrap(),
        before
    );
    server.stop().await;
}

#[tokio::test]
async fn compressed_native_upload_publication_fetch_and_restart() {
    let (temporary, store) = setup();
    let server = Running::start(store, ServiceConfig::default()).await;
    let mut client = server.client().await;
    let fixture = fixture();
    seed(&mut client, &fixture).await;
    let req = publish(root(&fixture), "publish-main", 0);
    let first = client
        .publish_reference(request(req.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.idempotency_key, "publish-main");
    assert_eq!(first.reference.as_ref().unwrap().generation, 1);
    assert_eq!(
        client
            .publish_reference(request(req.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        first
    );
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "publish-main".into()
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner(),
        first
    );
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "publish-main".into()
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let stale = client
        .publish_reference(request(publish(root(&fixture), "stale-writer", 0), ALPHA))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Aborted);
    let mut changed = req;
    changed.reference_name = "other".into();
    assert_eq!(
        client
            .publish_reference(request(changed, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    // The same caller UUID is independent across authorized principals.
    let peer = client
        .publish_reference(request(publish(root(&fixture), "publish-main", 1), PEER))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(peer.reference.as_ref().unwrap().generation, 2);
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "publish-main".into()
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner(),
        first
    );
    let mut writer_a = client.clone();
    let mut writer_b = client.clone();
    let (a, b) = tokio::join!(
        writer_a.publish_reference(request(publish(root(&fixture), "race-a", 2), ALPHA)),
        writer_b.publish_reference(request(publish(root(&fixture), "race-b", 2), PEER))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(a.err().or_else(|| b.err()).unwrap().code(), Code::Aborted);
    let view = client
        .open_repository(request(
            p::OpenRepositoryRequest {
                repository_id: "alpha".into(),
                reference_names: vec!["main".into(), "absent".into()],
            },
            READER,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(view.references.len(), 2);
    assert!(view.references[1].commit.is_none());
    assert_eq!(
        view.references[0].commit.as_ref().unwrap().key,
        Some(wire_key(root(&fixture)))
    );
    let file = fixture
        .objects()
        .iter()
        .find(|o| o.descriptor.key.kind == Kind::File && o.bytes.len() > 1024)
        .unwrap();
    let mut stream = client
        .read_file(request(
            p::ReadFileRequest {
                repository_id: "alpha".into(),
                native_file_id: file.descriptor.key.id.0.to_vec(),
            },
            READER,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        stream.message().await.unwrap().unwrap().frame,
        Some(p::file_frame::Frame::Header(_))
    ));
    let mut bytes = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        match frame.frame.unwrap() {
            p::file_frame::Frame::Chunk(c) => {
                assert_eq!(c.offset, bytes.len() as u64);
                bytes.extend(c.data);
            }
            _ => panic!("repeated header"),
        }
    }
    assert_eq!(bytes, file.bytes);
    drop(stream);
    drop(client);
    server.stop().await;
    // Maintenance runs while the service is stopped. Reopen once more after
    // packing so ordinary RPCs exercise durable packed locations, not a live
    // writer's in-memory state. Logical descriptors and wire messages stay equal.
    let reopened = Store::open_existing(temporary.path(), Default::default()).unwrap();
    let packed = reopened
        .pack("alpha", tinytree_store::PackOptions::default())
        .unwrap();
    let intrinsic_tree = tinytree_model::empty_tree().descriptor.key;
    let expected_members = fixture
        .objects()
        .iter()
        .filter(|object| object.descriptor.key != intrinsic_tree)
        .count() as u64;
    assert_eq!(packed.packed_objects, expected_members);
    assert_eq!(packed.removed_loose_files, expected_members);
    assert!(packed.pack_id.is_some());
    drop(reopened);
    let reopened = Store::open_existing(temporary.path(), Default::default()).unwrap();
    let server = Running::start(reopened, ServiceConfig::default()).await;
    let mut client = server.client().await;
    let tree = fixture
        .objects()
        .iter()
        .find(|object| {
            object.descriptor.key.kind == Kind::Tree && object.descriptor.key != intrinsic_tree
        })
        .unwrap();
    let commit = fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key == root(&fixture))
        .unwrap();
    let metadata = [commit, tree];
    let mut packed_metadata = client
        .batch_read_objects(request(
            p::BatchReadObjectsRequest {
                repository_id: "alpha".into(),
                keys: metadata
                    .iter()
                    .map(|object| wire_key(object.descriptor.key))
                    .collect(),
            },
            READER,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut seen = [false; 2];
    while let Some(result) = packed_metadata.message().await.unwrap() {
        let index = result.request_index as usize;
        assert!(index < metadata.len() && !seen[index]);
        seen[index] = true;
        let Some(p::object_result::Result::Object(object)) = result.result else {
            panic!("packed metadata must remain available through ordinary RPCs")
        };
        assert_eq!(result.key, Some(wire_key(metadata[index].descriptor.key)));
        assert_eq!(
            object.descriptor,
            Some(descriptor(metadata[index].descriptor))
        );
        assert_eq!(object.canonical_envelope, metadata[index].bytes);
    }
    assert!(seen.iter().all(|seen| *seen));
    drop(packed_metadata);
    let mut packed_file = client
        .read_file(request(
            p::ReadFileRequest {
                repository_id: "alpha".into(),
                native_file_id: file.descriptor.key.id.0.to_vec(),
            },
            READER,
        ))
        .await
        .unwrap()
        .into_inner();
    let Some(p::file_frame::Frame::Header(header)) =
        packed_file.message().await.unwrap().unwrap().frame
    else {
        panic!("packed file must start with its unchanged descriptor")
    };
    assert_eq!(header, descriptor(file.descriptor));
    let mut packed_bytes = Vec::new();
    while let Some(frame) = packed_file.message().await.unwrap() {
        let Some(p::file_frame::Frame::Chunk(chunk)) = frame.frame else {
            panic!("packed file repeated its header")
        };
        assert_eq!(chunk.offset, packed_bytes.len() as u64);
        packed_bytes.extend(chunk.data);
    }
    assert_eq!(packed_bytes, file.bytes);
    drop(packed_file);
    let receipt = client
        .get_publication(request(
            p::GetPublicationRequest {
                repository_id: "alpha".into(),
                idempotency_key: "publish-main".into(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(receipt, first);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn authorization_precedes_scope_existence_and_isolates_native_objects() {
    let (_temporary, store) = setup();
    let server = Running::start(store, ServiceConfig::default()).await;
    let mut client = server.client().await;
    let fixture = fixture();
    seed(&mut client, &fixture).await;
    let no_auth = client
        .open_repository(p::OpenRepositoryRequest {
            repository_id: "alpha".into(),
            reference_names: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(no_auth.code(), Code::Unauthenticated);
    for repo in ["beta", "does-not-exist"] {
        let e = client
            .find_missing_objects(request(
                p::FindMissingObjectsRequest {
                    repository_id: repo.into(),
                    keys: vec![wire_key(root(&fixture))],
                },
                ALPHA,
            ))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::PermissionDenied);
    }
    let absent = client
        .find_missing_objects(request(
            p::FindMissingObjectsRequest {
                repository_id: "beta".into(),
                keys: vec![wire_key(root(&fixture))],
            },
            BETA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(absent.keys.len(), 1);
    let object = &fixture.objects()[0];
    assert_eq!(
        upload(
            &mut client,
            "alpha",
            READER,
            object.descriptor,
            &object.bytes
        )
        .await
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    let private = client
        .open_repository(request(
            p::OpenRepositoryRequest {
                repository_id: "alpha".into(),
                reference_names: vec!["_tinytree/holds/x".into()],
            },
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert_eq!(private.code(), Code::PermissionDenied);
    let mut req = publish(root(&fixture), "private-ref", 0);
    req.reference_name = "_tinytree/holds/x".into();
    assert_eq!(
        client
            .publish_reference(request(req, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn graph_root_pagination_and_partial_metadata_are_explicit() {
    let (_temporary, store) = setup();
    let server = Running::start(store, ServiceConfig::default()).await;
    let mut client = server.client().await;
    let fixture = fixture();
    seed(&mut client, &fixture).await;
    let base = fixture
        .objects()
        .iter()
        .find(|o| o.descriptor.key == root(&fixture))
        .unwrap();
    let mut c = match tinytree_model::decode(&base.bytes, Limits::default()).unwrap() {
        Object::Commit(c) => c,
        _ => unreachable!(),
    };
    let mut head = root(&fixture);
    for n in 0..2 {
        c.parents = vec![CommitId(head.id.0)];
        c.description = format!("native child {n}");
        let encoded =
            tinytree_model::encode(&Object::Commit(c.clone()), Limits::default()).unwrap();
        upload(
            &mut client,
            "alpha",
            ALPHA,
            encoded.descriptor,
            &encoded.bytes,
        )
        .await
        .unwrap();
        head = encoded.descriptor.key;
    }
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let graph = p::GetCommitGraphRequest {
        repository_id: "alpha".into(),
        heads: vec![wire_key(head)],
        common: vec![wire_key(zero)],
        page_size: 1,
        continuation: String::new(),
    };
    let first = client
        .get_commit_graph(request(graph.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.entries.len(), 1);
    let entry = &first.entries[0];
    let descriptor = entry.commit.as_ref().unwrap();
    let commit = tinytree_model::decode(&entry.canonical_envelope, Limits::default()).unwrap();
    let encoded = tinytree_model::encode(&commit, Limits::default()).unwrap();
    assert_eq!(encoded.bytes, entry.canonical_envelope);
    assert_eq!(encoded.descriptor.size_bytes, descriptor.size_bytes);
    assert_eq!(
        encoded.descriptor.key.id.0.as_slice(),
        descriptor.key.as_ref().unwrap().native_id
    );
    let Object::Commit(commit) = commit else {
        panic!()
    };
    assert_eq!(commit.parents.len(), entry.parents.len());
    assert!(!first.continuation.is_empty());
    let mut next = graph.clone();
    next.continuation = first.continuation.clone();
    let second = client
        .get_commit_graph(request(next.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.entries.len(), 1);
    assert_ne!(first.entries[0].commit, second.entries[0].commit);
    let mut altered = next.clone();
    altered.common.clear();
    assert_eq!(
        client
            .get_commit_graph(request(altered, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        client
            .get_commit_graph(request(next.clone(), PEER))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let last = next.continuation.pop().unwrap();
    next.continuation.push(if last == '0' { '1' } else { '0' });
    assert_eq!(
        client
            .get_commit_graph(request(next, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let empty = client
        .get_commit_graph(request(
            p::GetCommitGraphRequest {
                heads: vec![wire_key(zero)],
                common: vec![],
                continuation: String::new(),
                ..graph.clone()
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(empty.entries.is_empty() && empty.continuation.is_empty());
    let unknown = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([8; 32]),
    };
    let unknown_graph = p::GetCommitGraphRequest {
        common: vec![wire_key(unknown)],
        ..graph
    };
    assert_eq!(
        client
            .get_commit_graph(request(unknown_graph, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let missing = client
        .find_missing_objects(request(
            p::FindMissingObjectsRequest {
                repository_id: "alpha".into(),
                keys: vec![wire_key(zero), wire_key(unknown)],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(missing.keys, vec![wire_key(unknown)]);
    let tree = fixture
        .objects()
        .iter()
        .find(|o| o.descriptor.key.kind == Kind::Tree)
        .unwrap();
    let file = fixture
        .objects()
        .iter()
        .find(|o| o.descriptor.key.kind == Kind::File)
        .unwrap();
    let mut batch = client
        .batch_read_objects(request(
            p::BatchReadObjectsRequest {
                repository_id: "alpha".into(),
                keys: vec![
                    wire_key(tree.descriptor.key),
                    wire_key(unknown),
                    wire_key(file.descriptor.key),
                    wire_key(zero),
                ],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut results = Vec::new();
    while let Some(item) = batch.message().await.unwrap() {
        results.push(item);
    }
    assert_eq!(results.len(), 4);
    assert!(matches!(
        results[0].result,
        Some(p::object_result::Result::Object(_))
    ));
    for (i, expected) in [
        (1, p::item_error::Code::NotFound),
        (2, p::item_error::Code::InvalidObject),
        (3, p::item_error::Code::InvalidObject),
    ] {
        let Some(p::object_result::Result::Error(ref error)) = results[i].result else {
            panic!("expected per-key error")
        };
        assert_eq!(error.code, expected as i32);
        assert_eq!(results[i].request_index, i as u32);
    }
    drop(batch);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn incomplete_conflict_closure_and_malformed_uploads_never_publish() {
    let (_temporary, store) = setup();
    let server = Running::start(store, ServiceConfig::default()).await;
    let mut client = server.client().await;
    let fixture = fixture();
    seed(&mut client, &fixture).await;
    let base = fixture
        .objects()
        .iter()
        .find(|o| o.descriptor.key == root(&fixture))
        .unwrap();
    let mut c = match tinytree_model::decode(&base.bytes, Limits::default()).unwrap() {
        Object::Commit(c) => c,
        _ => unreachable!(),
    };
    c.root_tree.terms = vec![c.root_tree.terms[0], TreeId([55; 32]), c.root_tree.terms[0]];
    c.root_tree.labels = vec![String::new(); 3];
    let missing = tinytree_model::encode(&Object::Commit(c), Limits::default()).unwrap();
    upload(
        &mut client,
        "alpha",
        ALPHA,
        missing.descriptor,
        &missing.bytes,
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .publish_reference(request(
                publish(missing.descriptor.key, "missing-negative", 0),
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    assert_eq!(
        client
            .publish_reference(request(publish(zero, "root-target", 0), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let bytes = b"an upload that is not already stored";
    let d = tinytree_model::describe_file(bytes);
    let mut wrong = bytes.to_vec();
    wrong[0] ^= 1;
    assert_eq!(
        upload(&mut client, "alpha", ALPHA, d, &wrong)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut offset = frames("alpha", d, bytes, 1024);
    if let Some(p::write_frame::Frame::Chunk(c)) = &mut offset[1].frame {
        c.offset = 7;
    }
    assert_eq!(
        client
            .write_object(request(tokio_stream::iter(offset), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let header = frames("alpha", d, bytes, 1024).remove(0);
    assert_eq!(
        client
            .write_object(request(tokio_stream::iter([header]), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let absent = client
        .find_missing_objects(request(
            p::FindMissingObjectsRequest {
                repository_id: "alpha".into(),
                keys: vec![wire_key(d.key)],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(absent.keys.len(), 1);
    let view = client
        .open_repository(request(
            p::OpenRepositoryRequest {
                repository_id: "alpha".into(),
                reference_names: vec!["main".into()],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(view.references[0].generation, 0);
    assert!(view.references[0].commit.is_none());
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn upload_idle_and_cancel_release_admission_and_bad_frames_fail() {
    let (_temporary, store) = setup();
    let mut config = ServiceConfig::default();
    config.max_inflight = 1;
    config.max_inflight_per_repository = 1;
    config.max_frame_bytes = 1024;
    config.rpc.stream_idle_timeout_ms = 40;
    config.rpc.operation_timeout_ms = 1000;
    let server = Running::start(store, config).await;
    let mut client = server.client().await;
    let bytes = vec![11; 4000];
    let d = tinytree_model::describe_file(&bytes);
    let (sender, receiver) = mpsc::channel(2);
    sender
        .send(frames("alpha", d, &bytes, 1024).remove(0))
        .await
        .unwrap();
    let status = client
        .write_object(request(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::DeadlineExceeded);
    drop(sender);
    let mut big = frames("alpha", d, &bytes, 4000);
    assert_eq!(big.len(), 2);
    assert_eq!(
        client
            .write_object(request(
                tokio_stream::iter(big.drain(..).collect::<Vec<_>>()),
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let (sender, receiver) = mpsc::channel(2);
    let mut other = server.client().await;
    let call = tokio::spawn(async move {
        other
            .write_object(request(
                tokio_stream::wrappers::ReceiverStream::new(receiver),
                ALPHA,
            ))
            .await
    });
    for frame in frames("alpha", d, &bytes, 1024).into_iter().take(2) {
        sender.send(frame).await.unwrap();
    }
    tokio::task::yield_now().await;
    call.abort();
    drop(sender);
    let _ = call.await;
    let receipt = upload(&mut client, "alpha", ALPHA, d, &bytes)
        .await
        .unwrap();
    assert_eq!(receipt.descriptor, Some(descriptor(d)));
    let absent = client
        .find_missing_objects(request(
            p::FindMissingObjectsRequest {
                repository_id: "alpha".into(),
                keys: vec![wire_key(d.key)],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(absent.keys.is_empty());
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn minimum_file_memory_budget_supports_upload_and_read() {
    let (_temporary, store) = setup();
    let mut config = ServiceConfig::default();
    config.max_frame_bytes = 1024;
    config.max_metadata_bytes = 1024;
    config.max_graph_bytes = 1024;
    config.max_buffered_bytes = 4096;
    assert!(config.validate().is_err());
    config.max_buffered_bytes = 5120;
    let server = Running::start(store, config).await;
    let mut client = server.client().await;
    let bytes = vec![31; 2000];
    let d = tinytree_model::describe_file(&bytes);
    upload(&mut client, "alpha", ALPHA, d, &bytes)
        .await
        .unwrap();
    let mut stream = client
        .read_file(request(
            p::ReadFileRequest {
                repository_id: "alpha".into(),
                native_file_id: d.key.id.0.to_vec(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut actual = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        if let Some(p::file_frame::Frame::Chunk(chunk)) = frame.frame {
            actual.extend(chunk.data);
        }
    }
    assert_eq!(actual, bytes);
    drop(stream);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn stalled_uncompressed_download_releases_admission_and_read_lease() {
    let (_temporary, store) = setup();
    // Deliberately exceed both HTTP/2 flow-control windows. No compression is enabled.
    let bytes: Vec<_> = (0..256 * 1024u64)
        .flat_map(|i| *blake3::hash(&i.to_le_bytes()).as_bytes())
        .collect();
    let d = tinytree_model::describe_file(&bytes);
    store.put("alpha", d, &bytes, 0).unwrap();
    let inspect = store.clone();
    let mut config = ServiceConfig::default();
    config.max_inflight = 1;
    config.max_inflight_per_repository = 1;
    config.rpc.operation_timeout_ms = 300;
    config.rpc.stream_idle_timeout_ms = 300;
    let server = Running::start(store, config).await;
    let channel = Endpoint::from_shared(format!("http://{}", server.address))
        .unwrap()
        .initial_stream_window_size(65535)
        .initial_connection_window_size(65535)
        .connect()
        .await
        .unwrap();
    let mut client = RepositoryClient::new(channel);
    let stream = client
        .read_file(request(
            p::ReadFileRequest {
                repository_id: "alpha".into(),
                native_file_id: d.key.id.0.to_vec(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Keep the unconsumed stream alive: a dropped client is not this regression.
    let mut second = server.client().await;
    second
        .find_missing_objects(request(
            p::FindMissingObjectsRequest {
                repository_id: "alpha".into(),
                keys: vec![wire_key(d.key)],
            },
            ALPHA,
        ))
        .await
        .unwrap();
    let gc = tokio::task::spawn_blocking(move || inspect.gc("alpha", i64::MAX / 2))
        .await
        .unwrap()
        .unwrap();
    assert!(
        gc.deleted_objects > 0,
        "expired producer must release its read lease"
    );
    drop(stream);
    drop(client);
    drop(second);
    server.stop().await;
}

#[tokio::test]
async fn graph_rejects_oversize_descriptor_before_reading_corrupt_payload() {
    let (temporary, store) = setup();
    let mut commit = tinytree_model::synthetic_root();
    commit.change_id = tinytree_model::ChangeId([7; 16]);
    commit.parents = vec![CommitId::ROOT];
    commit.description = "large native metadata".repeat(4096);
    let encoded = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    fn corrupt_payload(directory: &std::path::Path, bytes: &[u8]) -> bool {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if corrupt_payload(&path, bytes) {
                    return true;
                }
            } else if std::fs::read(&path).ok().as_deref() == Some(bytes) {
                let mut corrupt = bytes.to_vec();
                corrupt[0] ^= 1;
                std::fs::write(path, corrupt).unwrap();
                return true;
            }
        }
        false
    }
    assert!(corrupt_payload(temporary.path(), &encoded.bytes));
    let mut config = ServiceConfig::default();
    config.max_graph_bytes = 1024;
    let server = Running::start(store, config).await;
    let mut client = server.client().await;
    let error = client
        .get_commit_graph(request(
            p::GetCommitGraphRequest {
                repository_id: "alpha".into(),
                heads: vec![wire_key(encoded.descriptor.key)],
                common: vec![],
                page_size: 1,
                continuation: String::new(),
            },
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn failed_startup_never_creates_storage_or_runs_gc() {
    use tinytree_server::{Config, PrincipalConfig, TlsConfig, serve};
    let temporary = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage = temporary.path().join("uncreated");
    config.listen = "127.0.0.1:0".into();
    config.principals = vec![PrincipalConfig {
        id: "missing".into(),
        token_env: format!("TINYTREE_MISSING_{}", uuid::Uuid::new_v4().simple()),
        repositories: vec!["default".into()],
        write: true,
        retain: false,
        cloud: false,
        landing_profiles: Vec::new(),
    }];
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    config.principals.clear();
    config.repositories = vec!["con".into()];
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    config.repositories = vec!["default".into()];
    config.service.cloud.enabled = true;
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(
        !config.storage.exists(),
        "invalid cloud resource profile must fail before persistent state"
    );
    config.service.cloud.enabled = false;
    let pem = temporary.path().join("invalid.pem");
    std::fs::write(&pem, "not a PEM identity").unwrap();
    config.tls = Some(TlsConfig {
        certificate: pem.clone(),
        private_key: pem,
    });
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    config.tls = None;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.listen = occupied.local_addr().unwrap().to_string();
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    // Existing durable state also remains unchanged on listener failure.
    let existing = Store::open(&config.storage, Default::default()).unwrap();
    existing.create_repository("default").unwrap();
    let bytes = b"unreferenced native startup candidate";
    let d = tinytree_model::describe_file(bytes);
    existing.put("default", d, bytes, 0).unwrap();
    drop(existing);
    config.gc_interval_seconds = 1;
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    let existing = Store::open(&config.storage, Default::default()).unwrap();
    assert_eq!(existing.descriptor("default", d.key).unwrap(), Some(d));
}

#[cfg(unix)]
#[tokio::test]
async fn unix_listener_requires_private_real_parent_and_preserves_replacement() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    use tinytree_server::{Config, serve, serve_unix};
    let temporary = tempfile::tempdir().unwrap();
    let public = temporary.path().join("public");
    std::fs::create_dir(&public).unwrap();
    std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o777)).unwrap();
    let mut config = Config::default();
    config.storage = temporary.path().join("uncreated");
    config.listen = format!("unix://{}", public.join("server.sock").display());
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    let link = temporary.path().join("link");
    symlink(temporary.path(), &link).unwrap();
    config.listen = format!("unix://{}", link.join("server.sock").display());
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    let (_data, store) = setup();
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["alpha".into()],
        vec![],
    )
    .unwrap();
    std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = temporary
        .path()
        .canonicalize()
        .unwrap()
        .join("private.sock");
    let path = socket.clone();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve_unix(&path, service, async {
            let _ = stopped.await;
        })
        .await
    });
    for _ in 0..100 {
        if task.is_finished() {
            panic!("Unix startup failed before ready: {:?}", task.await);
        }
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let endpoint_socket = socket.clone();
    let channel = Endpoint::from_static("http://localhost")
        .connect_with_connector(tower::service_fn(move |_| {
            let path = endpoint_socket.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .unwrap();
    let mut landing = tinytree_proto::landing_v1::landing_client::LandingClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    assert_eq!(
        landing
            .get_landing(tinytree_proto::landing_v1::GetLandingRequest {
                repository_id: "alpha".into(),
                idempotency_key: "absent".into()
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        landing
            .start_landing(tinytree_proto::landing_v1::StartLandingRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    drop(landing);
    std::fs::rename(&socket, temporary.path().join("old.sock")).unwrap();
    std::fs::write(&socket, "replacement owned by caller").unwrap();
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(
        std::fs::read_to_string(socket).unwrap(),
        "replacement owned by caller"
    );
}

#[tokio::test]
async fn stalled_unary_graph_response_retains_admission_until_body_deadline() {
    let (_temporary, store) = setup();
    let mut commit = tinytree_model::synthetic_root();
    commit.change_id = tinytree_model::ChangeId([9; 16]);
    commit.parents = vec![CommitId::ROOT; 100_000];
    let encoded = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    let mut config = ServiceConfig::default();
    // This boundary fixture deliberately duplicates 100k parent summaries.
    // Budget their conservative retained shape plus canonical Vec capacity;
    // keep the response flow-controlled, rather than fail admission up front.
    config.max_graph_bytes = encoded.bytes.len().saturating_mul(2)
        + 100_000 * (4 * std::mem::size_of::<p::ObjectKey>() + 64)
        + std::mem::size_of::<p::CommitGraphEntry>()
        + 128;
    config.max_buffered_bytes = config.max_graph_bytes.max(config.max_metadata_bytes) * 4;
    config.max_inflight = 1;
    config.max_inflight_per_repository = 1;
    config.rpc.rpc_timeout_ms = 1000;
    config.rpc.operation_timeout_ms = 1000;
    config.rpc.stream_idle_timeout_ms = 1000;
    let server = Running::start(store, config).await;
    let channel = Endpoint::from_shared(format!("http://{}", server.address))
        .unwrap()
        .initial_stream_window_size(65535)
        .initial_connection_window_size(65535)
        .connect()
        .await
        .unwrap();
    let mut raw = tonic::client::Grpc::new(channel);
    raw.ready().await.unwrap();
    // Read only headers of a unary RPC, using the generic streaming client to
    // deliberately leave its multi-megabyte encoded response flow-controlled.
    let response: tonic::Response<tonic::codec::Streaming<p::CommitGraphPage>> = raw
        .server_streaming(
            request(
                p::GetCommitGraphRequest {
                    repository_id: "alpha".into(),
                    heads: vec![wire_key(encoded.descriptor.key)],
                    common: vec![],
                    page_size: 1,
                    continuation: String::new(),
                },
                ALPHA,
            ),
            http::uri::PathAndQuery::from_static("/tinytree.v1.Repository/GetCommitGraph"),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .unwrap();
    let held = response.into_inner();
    let mut second = server.client().await;
    let waiting = request(
        p::FindMissingObjectsRequest {
            repository_id: "alpha".into(),
            keys: vec![],
        },
        ALPHA,
    );
    // No grpc-timeout header: observe queueing without tonic's separate client
    // transport timer racing and mapping its TimeoutExpired to CANCELLED.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            second.find_missing_objects(waiting)
        )
        .await
        .is_err(),
        "stalled response must retain its admission permit"
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    second
        .find_missing_objects(request(
            p::FindMissingObjectsRequest {
                repository_id: "alpha".into(),
                keys: vec![],
            },
            ALPHA,
        ))
        .await
        .unwrap();
    drop(held);
    drop(raw);
    drop(second);
    server.stop().await;
}

#[tokio::test]
async fn private_retention_is_owner_scoped_durable_and_independent_of_public_writes() {
    async fn lookup(
        client: &mut RepositoryClient<Channel>,
        token: &str,
        name: &str,
    ) -> p::Reference {
        let view = client
            .open_repository(request(
                p::OpenRepositoryRequest {
                    repository_id: "alpha".into(),
                    reference_names: vec![name.into()],
                },
                token,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(view.references.len(), 1);
        assert_eq!(view.references[0].name, name);
        view.references.into_iter().next().unwrap()
    }
    let (temporary, store) = setup();
    let mut policy = ServiceConfig::default();
    policy.retention.max_receipts_per_client = 2;
    let server = Running::start(store.clone(), policy.clone()).await;
    let mut client = server.client().await;
    let fixture = fixture();
    seed(&mut client, &fixture).await;
    let head = root(&fixture);
    client
        .publish_reference(request(publish(head, "public-before-hold", 0), ALPHA))
        .await
        .unwrap();
    let name = format!(
        "_tinytree/hold/v1/{}/{}",
        "01".repeat(16),
        fixture.manifest.root_commit
    );
    let mut hold = publish(head, "acquire-private", 0);
    hold.reference_name = name.clone();
    let mut malformed = hold.clone();
    malformed.reference_name = format!(
        "_tinytree/hold/v1/{}/{}",
        "AB".repeat(16),
        fixture.manifest.root_commit
    );
    assert_eq!(
        client
            .publish_reference(request(malformed, READER))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut outside_scope = hold.clone();
    outside_scope.repository_id = "unknown-repository".into();
    assert_eq!(
        client
            .publish_reference(request(outside_scope, READER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    // Public write does not imply retain; retain does not imply public write.
    assert_eq!(
        client
            .publish_reference(request(hold.clone(), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .publish_reference(request(publish(head, "reader-public", 1), READER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let receipt = client
        .publish_reference(request(hold.clone(), READER))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(receipt.reference.as_ref().unwrap().name, name);
    assert_eq!(receipt.reference.as_ref().unwrap().generation, 1);
    assert_eq!(
        client
            .publish_reference(request(hold.clone(), READER))
            .await
            .unwrap()
            .into_inner(),
        receipt
    );
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "acquire-private".into(),
                },
                READER
            ))
            .await
            .unwrap()
            .into_inner(),
        receipt
    );
    let unseen = lookup(&mut client, PEER, &name).await;
    assert!(unseen.commit.is_none());
    assert_eq!(unseen.generation, 0);
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "acquire-private".into(),
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    // Another principal may acquire the same wire name, but cannot release ours.
    let mut unauthorized_release = hold.clone();
    unauthorized_release.idempotency_key = "cannot-release-peer".into();
    unauthorized_release.expected_generation = 1;
    unauthorized_release.new_commit = None;
    assert_eq!(
        client
            .publish_reference(request(unauthorized_release, PEER))
            .await
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    client
        .publish_reference(request(hold.clone(), PEER))
        .await
        .unwrap();
    let mut forged = hold.clone();
    forged.reference_name = format!(
        "_tinytree/holds/v1/{}/{}/{}",
        "02".repeat(32),
        "01".repeat(16),
        fixture.manifest.root_commit
    );
    forged.idempotency_key = "internal-name-denied".into();
    assert_eq!(
        client
            .publish_reference(request(forged, READER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut retarget = hold.clone();
    retarget.idempotency_key = "retarget-denied".into();
    retarget.new_commit = Some(wire_key(ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([9; 32]),
    }));
    assert_eq!(
        client
            .publish_reference(request(retarget, READER))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut erase_public = publish(head, "erase-public", 1);
    erase_public.new_commit = None;
    client
        .publish_reference(request(erase_public, ALPHA))
        .await
        .unwrap();
    store.gc("alpha", i64::MAX - 1).unwrap();
    assert!(store.descriptor("alpha", head).unwrap().is_some());
    let mut release = hold.clone();
    release.idempotency_key = "release-private".into();
    release.expected_generation = 1;
    release.new_commit = None;
    let released = client
        .publish_reference(request(release.clone(), READER))
        .await
        .unwrap()
        .into_inner();
    assert!(released.reference.as_ref().unwrap().commit.is_none());
    assert_eq!(released.reference.as_ref().unwrap().generation, 2);
    // The reserved receipt slot permits release at the cap; later acquisitions fail.
    let mut reacquire = hold.clone();
    reacquire.idempotency_key = "over-receipt-quota".into();
    reacquire.expected_generation = 2;
    assert_eq!(
        client
            .publish_reference(request(reacquire, READER))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        client
            .publish_reference(request(release.clone(), READER))
            .await
            .unwrap()
            .into_inner(),
        released
    );
    drop(client);
    server.stop().await;
    drop(store);

    let reopened = Store::open_existing(temporary.path(), Default::default()).unwrap();
    let rotated_peer = "rotated-peer-test-token-not-a-secret";
    let server = Running::start_with_peer_token(reopened.clone(), policy, rotated_peer).await;
    let mut client = server.client().await;
    let absent = lookup(&mut client, READER, &name).await;
    assert!(absent.commit.is_none());
    assert_eq!(absent.generation, 2);
    // Historical acquire receipts are replayable but are not proof of a current hold.
    assert_eq!(
        client
            .get_publication(request(
                p::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "acquire-private".into(),
                },
                READER
            ))
            .await
            .unwrap()
            .into_inner(),
        receipt
    );
    assert_eq!(
        client
            .open_repository(request(
                p::OpenRepositoryRequest {
                    repository_id: "alpha".into(),
                    reference_names: vec![name.clone()],
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert!(
        lookup(&mut client, rotated_peer, &name)
            .await
            .commit
            .is_some()
    );
    reopened.gc("alpha", i64::MAX - 1).unwrap();
    assert!(reopened.descriptor("alpha", head).unwrap().is_some());
    client
        .publish_reference(request(release, rotated_peer))
        .await
        .unwrap();
    reopened.gc("alpha", i64::MAX - 1).unwrap();
    assert!(reopened.descriptor("alpha", head).unwrap().is_none());
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn graph_multiple_packed_roots_do_not_reserve_a_packed_reader_per_root() {
    let (_temporary, store) = setup();
    let fixture = fixture();
    for object in fixture.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let base = fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key == root(&fixture))
        .unwrap();
    let Object::Commit(mut commit) =
        tinytree_model::decode(&base.bytes, Limits::default()).unwrap()
    else {
        panic!()
    };
    commit.parents = vec![CommitId::ROOT];
    let mut heads = Vec::new();
    for index in 0..9 {
        commit.description = format!("independent graph head {index}");
        let encoded =
            tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
        store
            .put("alpha", encoded.descriptor, &encoded.bytes, 0)
            .unwrap();
        heads.push(wire_key(encoded.descriptor.key));
    }
    let server = Running::start(store.clone(), ServiceConfig::default()).await;
    let mut client = server.client().await;
    let query = p::GetCommitGraphRequest {
        repository_id: "alpha".into(),
        heads,
        common: Vec::new(),
        page_size: 64,
        continuation: String::new(),
    };
    let before = client
        .get_commit_graph(request(query.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(before.entries.len(), 9);
    assert!(before.continuation.is_empty());
    let packed = store
        .pack("alpha", tinytree_store::PackOptions::default())
        .unwrap();
    assert!(packed.packed_objects >= 9);
    let after = client
        .get_commit_graph(request(query, ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        after, before,
        "lightweight root pins must leave packed-reader capacity for traversal"
    );
    drop(client);
    server.stop().await;
}

fn query_frames(roots: &[ObjectKey], replay: u8) -> Vec<q::OpenScopeFrame> {
    let mut roots: Vec<_> = roots
        .iter()
        .map(|key| q::CommitId {
            value: key.id.0.to_vec(),
        })
        .collect();
    roots.sort_by(|a, b| a.value.cmp(&b.value));
    vec![
        q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::Header(q::ScopeHeader {
                version: 1,
                repository_id: "alpha".into(),
                replay_key: vec![replay; 32],
                roots,
                overlay_object_count: 0,
                overlay_bytes: 0,
                cloud_source: None,
            })),
        },
        q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::ScopeEnd(q::ScopeEnd::default())),
        },
    ]
}
async fn query_ready(
    client: &mut q::repository_query_client::RepositoryQueryClient<Channel>,
    roots: &[ObjectKey],
    replay: u8,
) -> q::QueryScope {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client
                .open_scope(request(
                    tokio_stream::iter(query_frames(roots, replay)),
                    ALPHA,
                ))
                .await
            {
                Ok(response) => break response.into_inner(),
                Err(error) if error.code() == Code::Unavailable => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("query scope did not become ready: {error}"),
            }
        }
    })
    .await
    .unwrap()
}
fn query_id(key: ObjectKey) -> q::CommitId {
    q::CommitId {
        value: key.id.0.to_vec(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_query_service_warms_persistently_and_returns_exact_facts_and_prefixes() {
    let (temporary, store) = setup();
    let fixture = fixture();
    for object in fixture.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let base = root(&fixture);
    let Object::Commit(mut commit) = tinytree_model::decode(
        &fixture
            .objects()
            .iter()
            .find(|object| object.descriptor.key == base)
            .unwrap()
            .bytes,
        Limits::default(),
    )
    .unwrap() else {
        panic!()
    };
    commit.parents = vec![CommitId(base.id.0)];
    commit.change_id = tinytree_model::ChangeId([42; 16]);
    let mut heads = Vec::new();
    for name in ["left", "right"] {
        commit.description = name.into();
        let encoded =
            tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
        store
            .put("alpha", encoded.descriptor, &encoded.bytes, 0)
            .unwrap();
        heads.push(encoded.descriptor.key);
    }
    let index_path = temporary.path().join("derived");
    let index = tinytree_index::Index::open(&index_path, &Default::default()).unwrap();
    let server = Running::start_query(store.clone(), ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let error = client
        .open_scope(request(tokio_stream::iter(query_frames(&heads, 1)), ALPHA))
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::Unavailable,
        "first scope request schedules, never synchronously builds the graph"
    );
    let scope = query_ready(&mut client, &heads, 1).await;
    let key = scope.key.clone().unwrap();
    assert_eq!(scope.index_epoch.len(), 16);
    assert!(scope.high_water >= 3);
    let replay = client
        .open_scope(request(tokio_stream::iter(query_frames(&heads, 1)), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(scope, replay);
    let unknown = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([77; 32]),
    };
    let facts = vec![
        q::GraphFact {
            fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                commit: Some(query_id(base)),
            })),
        },
        q::GraphFact {
            fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                commit: Some(query_id(unknown)),
            })),
        },
        q::GraphFact {
            fact: Some(q::graph_fact::Fact::IsAncestor(q::IsAncestor {
                ancestor: Some(query_id(base)),
                descendant: Some(query_id(heads[0])),
            })),
        },
        q::GraphFact {
            fact: Some(q::graph_fact::Fact::Heads(q::Heads {
                commits: vec![query_id(base), query_id(heads[0]), query_id(heads[1])],
            })),
        },
        q::GraphFact {
            fact: Some(q::graph_fact::Fact::CommonAncestors(q::CommonAncestors {
                left: vec![query_id(heads[0])],
                right: vec![query_id(heads[1])],
            })),
        },
    ];
    let answers = client
        .query_facts(request(
            q::QueryFactsRequest {
                scope: Some(key.clone()),
                facts,
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(answers.answers.len(), 5);
    assert_eq!(answers.scope_digest, scope.content_digest);
    assert_eq!(answers.index_epoch, scope.index_epoch);
    assert!(matches!(
        answers.answers[0].answer,
        Some(q::graph_fact_answer::Answer::HasId(true))
    ));
    assert!(matches!(
        answers.answers[1].answer,
        Some(q::graph_fact_answer::Answer::HasId(false))
    ));
    assert!(matches!(
        answers.answers[2].answer,
        Some(q::graph_fact_answer::Answer::IsAncestor(true))
    ));
    let Some(q::graph_fact_answer::Answer::Heads(result)) = &answers.answers[3].answer else {
        panic!()
    };
    assert_eq!(result.commits.len(), 2);
    let Some(q::graph_fact_answer::Answer::CommonAncestors(result)) = &answers.answers[4].answer
    else {
        panic!()
    };
    assert_eq!(result.commits, vec![query_id(base)]);
    let resolved = client
        .resolve_commit_prefix(request(
            q::ResolveCommitPrefixRequest {
                selection: None,
                scope: Some(key.clone()),
                hex_prefix: heads[0].id.0.iter().map(|b| format!("{b:02x}")).collect(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        resolved.result,
        Some(q::commit_prefix_result::Result::Single(_))
    ));
    let changes = client
        .resolve_change_prefix(request(
            q::ResolveChangePrefixRequest {
                selection: None,
                scope: Some(key.clone()),
                hex_prefix: "2a".repeat(16),
                visible_heads: vec![query_id(heads[0])],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    let Some(q::change_prefix_result::Result::Single(changes)) = changes.result else {
        panic!()
    };
    assert_eq!(changes.targets.len(), 2);
    assert_eq!(
        changes
            .targets
            .iter()
            .filter(|target| target.visible == Some(true))
            .count(),
        1
    );
    assert_eq!(
        changes
            .targets
            .iter()
            .filter(|target| target.visible == Some(false))
            .count(),
        1
    );
    let shortest = client
        .shortest_prefix(request(
            q::ShortestPrefixRequest {
                selection: None,
                scope: Some(key.clone()),
                target: Some(q::shortest_prefix_request::Target::Commit(query_id(
                    heads[0],
                ))),
                visible_heads: Vec::new(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(shortest.hex_length <= 64);
    assert!(shortest.in_selection.is_none());
    assert!(shortest.selection_digest.is_none());
    let selection = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: vec![query_id(heads[0])],
        })),
    };
    let selected = client
        .resolve_commit_prefix(request(
            q::ResolveCommitPrefixRequest {
                scope: Some(key.clone()),
                hex_prefix: String::new(),
                selection: Some(selection.clone()),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        matches!(selected.result,Some(q::commit_prefix_result::Result::Single(q::CommitMatch{commit:Some(ref id)}))if id==&query_id(heads[0]))
    );
    let digest = selected.selection_digest.unwrap();
    assert_eq!(digest.len(), 32);
    let selected_change = client
        .resolve_change_prefix(request(
            q::ResolveChangePrefixRequest {
                scope: Some(key.clone()),
                hex_prefix: "2a".repeat(16),
                visible_heads: vec![query_id(heads[1])],
                selection: Some(selection.clone()),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(selected_change.selection_digest, Some(digest.clone()));
    let Some(q::change_prefix_result::Result::Single(selected_change)) = selected_change.result
    else {
        panic!("selected divergence must preserve its one distinct change")
    };
    assert_eq!(selected_change.targets.len(), 1);
    assert_eq!(selected_change.targets[0].commit, Some(query_id(heads[0])));
    assert_eq!(selected_change.targets[0].visible, Some(false));
    for (target, present) in [(heads[0], true), (heads[1], false)] {
        let value = client
            .shortest_prefix(request(
                q::ShortestPrefixRequest {
                    scope: Some(key.clone()),
                    target: Some(q::shortest_prefix_request::Target::Commit(query_id(target))),
                    visible_heads: vec![],
                    selection: Some(selection.clone()),
                },
                ALPHA,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(value.in_selection, Some(present));
        assert_eq!(value.hex_length, 0);
        assert_eq!(value.selection_digest, Some(digest.clone()));
    }
    let malformed = q::GraphExpr {
        version: 2,
        expression: selection.expression.clone(),
    };
    assert_eq!(
        client
            .shortest_prefix(request(
                q::ShortestPrefixRequest {
                    scope: Some(key.clone()),
                    target: Some(q::shortest_prefix_request::Target::Commit(query_id(
                        unknown
                    ))),
                    visible_heads: vec![],
                    selection: Some(malformed.clone())
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument,
        "an absent target must not disguise an invalid original AST"
    );
    assert_eq!(
        client
            .resolve_commit_prefix(request(
                q::ResolveCommitPrefixRequest {
                    scope: Some(key.clone()),
                    hex_prefix: String::new(),
                    selection: Some(malformed)
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "scope authorization precedes selection inspection"
    );
    assert_eq!(
        client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: Some(key.clone()),
                    facts: Vec::new()
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: Some(q::ScopeKey {
                        repository_id: "alpha".into(),
                        handle: vec![0; 32]
                    }),
                    facts: Vec::new()
                },
                BETA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut overlay = query_frames(&heads, 2);
    let Some(q::open_scope_frame::Frame::Header(header)) = &mut overlay[0].frame else {
        panic!()
    };
    header.overlay_object_count = 1;
    header.overlay_bytes = 1;
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(overlay), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .evaluate(request(
                q::EvaluateRequest {
                    scope: Some(key.clone()),
                    query_id: vec![99; 32],
                    expression: Some(q::GraphExpr {
                        version: 2,
                        expression: Some(q::graph_expr::Expression::Roots(
                            q::UnaryExpr {
                                input: Some(
                                    q::GraphExpr {
                                        version: 1,
                                        expression: Some(q::graph_expr::Expression::Commits(
                                            q::CommitSet {
                                                commits: vec![query_id(heads[0])]
                                            }
                                        ))
                                    }
                                    .into()
                                )
                            }
                            .into()
                        ))
                    }),
                    projection: q::Projection::Ids as i32,
                    page_size: 1,
                    cursor: Vec::new(),
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let root_selection = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Roots(
            q::UnaryExpr {
                input: Some(
                    q::GraphExpr {
                        version: 1,
                        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                            commits: vec![query_id(heads[0]), query_id(base)],
                        })),
                    }
                    .into(),
                ),
            }
            .into(),
        )),
    };
    let page = client
        .evaluate(request(
            q::EvaluateRequest {
                scope: Some(key.clone()),
                query_id: vec![100; 32],
                expression: Some(root_selection),
                projection: q::Projection::Ids as i32,
                page_size: 128,
                cursor: Vec::new(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.commit.clone().unwrap())
            .collect::<Vec<_>>(),
        vec![query_id(base)]
    );
    assert_eq!(page.exhausted, Some(true));
    drop(client);
    server.stop().await;
    // Pending in-memory jobs are not replayed after restart. Persisted verified
    // nodes make the same explicit root checkpoint ready without a history RPC.
    let index = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let path = index_path.clone();
            match tokio::task::spawn_blocking(move || {
                tinytree_index::Index::open(path, &Default::default())
            })
            .await
            .unwrap()
            {
                Ok(index) => break index,
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .unwrap();
    let restarted = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = restarted.query_client().await;
    assert_eq!(
        client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: Some(key),
                    facts: Vec::new()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let reopened = client
        .open_scope(request(tokio_stream::iter(query_frames(&heads, 3)), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reopened.index_epoch, scope.index_epoch);
    drop(client);
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_scope_expiry_and_close_release_roots_without_future_requests() {
    let (temporary, store) = setup();
    let fixture = fixture();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    for object in fixture.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            index
                .ingest("alpha", object.descriptor, &object.bytes)
                .unwrap();
        }
    }
    let root = root(&fixture);
    let mut config = ServiceConfig::default();
    config.query.scope_ttl_seconds = 2;
    config.query.expiry_interval_ms = 20;
    config.query.max_scopes = 2;
    config.query.max_scopes_per_principal = 2;
    config.query.max_scopes_per_repository = 2;
    config.query.max_scope_replays = 3;
    config.query.max_scope_replays_per_principal = 3;
    config.query.max_scope_replays_per_repository = 3;
    let server = Running::start_query(store.clone(), config, index).await;
    let mut client = server.query_client().await;
    let first = query_ready(&mut client, &[root], 1).await;
    let second = query_ready(&mut client, &[root], 2).await;
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(query_frames(&[root], 3)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    let close = q::CloseScopeRequest { scope: first.key };
    client
        .close_scope(request(close.clone(), ALPHA))
        .await
        .unwrap();
    client
        .close_scope(request(close.clone(), ALPHA))
        .await
        .unwrap();
    assert_eq!(
        client
            .close_scope(request(close, PEER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let replacement = query_ready(&mut client, &[root], 3).await;
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(query_frames(&[root], 1)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: replacement.key,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(query_frames(&[root], 4)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted,
        "closed replay bindings remain bounded until their absolute expiry"
    );
    store.gc("alpha", i64::MAX - 1).unwrap();
    assert!(store.descriptor("alpha", root).unwrap().is_some());
    tokio::time::sleep(Duration::from_millis(2300)).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let store = store.clone();
            let present = tokio::task::spawn_blocking(move || {
                store.gc("alpha", i64::MAX - 1).unwrap();
                store.descriptor("alpha", root).unwrap().is_some()
            })
            .await
            .unwrap();
            if !present {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: second.key,
                    facts: Vec::new()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let scope = query_ready(&mut client, &[zero], 4).await;
    let shortest = client
        .shortest_prefix(request(
            q::ShortestPrefixRequest {
                selection: None,
                scope: scope.key,
                target: Some(q::shortest_prefix_request::Target::Commit(query_id(zero))),
                visible_heads: Vec::new(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(shortest.hex_length, 0);
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_warm_missing_parent_failure_is_explicit_and_retries_after_canonical_upload() {
    let (temporary, store) = setup();
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![CommitId::ROOT];
    commit.change_id = tinytree_model::ChangeId([51; 16]);
    let parent =
        tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    commit.parents = vec![CommitId(parent.descriptor.key.id.0)];
    commit.description = "staged child before parent".into();
    let child = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", child.descriptor, &child.bytes, 0)
        .unwrap();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let server = Running::start_query(store.clone(), ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let first = client
        .open_scope(request(
            tokio_stream::iter(query_frames(&[child.descriptor.key], 1)),
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert_eq!(first.code(), Code::Unavailable);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .open_scope(request(
                    tokio_stream::iter(query_frames(&[child.descriptor.key], 1)),
                    ALPHA,
                ))
                .await
            {
                Err(error) if error.code() == Code::NotFound => break,
                Err(error) if error.code() == Code::Unavailable => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                other => {
                    panic!("missing canonical parent must be an explicit warm failure: {other:?}")
                }
            }
        }
    })
    .await
    .unwrap();
    store
        .put("alpha", parent.descriptor, &parent.bytes, 0)
        .unwrap();
    let scope = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .open_scope(request(
                    tokio_stream::iter(query_frames(&[child.descriptor.key], 1)),
                    ALPHA,
                ))
                .await
            {
                Ok(scope) => break scope.into_inner(),
                Err(error) if matches!(error.code(), Code::NotFound | Code::Unavailable) => {
                    tokio::time::sleep(Duration::from_millis(20)).await
                }
                other => panic!("warm repair failed: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let facts = client
        .query_facts(request(
            q::QueryFactsRequest {
                scope: scope.key,
                facts: vec![q::GraphFact {
                    fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                        commit: Some(query_id(parent.descriptor.key)),
                    })),
                }],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        facts.answers[0].answer,
        Some(q::graph_fact_answer::Answer::HasId(true))
    ));
    drop(client);
    server.stop().await;

    // Both parents and their canonical merge are valid in authoritative Store.
    // A tighter derived decoder is a configured resource boundary, not evidence
    // of canonical corruption or an empty/partially ready query scope.
    let (temporary, store) = setup();
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![CommitId::ROOT];
    commit.change_id = tinytree_model::ChangeId([52; 16]);
    let mut parents = Vec::new();
    for name in ["left parent", "right parent"] {
        commit.description = name.into();
        let canonical =
            tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
        store
            .put("alpha", canonical.descriptor, &canonical.bytes, 0)
            .unwrap();
        parents.push(CommitId(canonical.descriptor.key.id.0));
    }
    commit.parents = parents;
    commit.description = "two-parent merge".into();
    let merge = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", merge.descriptor, &merge.bytes, 0)
        .unwrap();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_commit_parents = 1;
    let server = Running::start_query(store, config, index).await;
    let mut client = server.query_client().await;
    assert_eq!(
        client
            .open_scope(request(
                tokio_stream::iter(query_frames(&[merge.descriptor.key], 1)),
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .open_scope(request(
                    tokio_stream::iter(query_frames(&[merge.descriptor.key], 1)),
                    ALPHA,
                ))
                .await
            {
                Err(error) if error.code() == Code::ResourceExhausted => {
                    assert!(error.message().contains("configured limits"));
                    break;
                }
                Err(error) if error.code() == Code::Unavailable => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                other => {
                    panic!("derived parent limit must not report canonical corruption: {other:?}")
                }
            }
        }
    })
    .await
    .unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_derived_index_does_not_prevent_production_object_service_startup() {
    #[derive(Clone, Copy)]
    enum Failure {
        GenerationFile,
        UnrecognizedGeneration,
        NestedUnrecognizedDatabase,
        InvalidDatabaseVersion,
        #[cfg(unix)]
        SymlinkDatabaseVersion,
        #[cfg(unix)]
        SymlinkVersion,
    }
    let cases = [
        Failure::GenerationFile,
        Failure::UnrecognizedGeneration,
        Failure::NestedUnrecognizedDatabase,
        Failure::InvalidDatabaseVersion,
        #[cfg(unix)]
        Failure::SymlinkDatabaseVersion,
        #[cfg(unix)]
        Failure::SymlinkVersion,
    ];
    for failure in cases {
        let temporary = tempfile::tempdir().unwrap();
        let storage = temporary.path().join("objects");
        let store = Store::open(&storage, Default::default()).unwrap();
        store.create_repository("alpha").unwrap();
        drop(store);
        let version_path =
            storage.join(format!("derived-index-v{}", tinytree_index::STORAGE_SCHEMA));
        let sentinel = match failure {
            Failure::GenerationFile => {
                let version = private_fs::PrivateDir::create(&version_path).unwrap();
                let path = version.path().join("generation-0");
                std::fs::write(&path, b"preserve unavailable derived state").unwrap();
                path
            }
            Failure::UnrecognizedGeneration => {
                let version = private_fs::PrivateDir::create(&version_path).unwrap();
                let generation =
                    private_fs::PrivateDir::create(version.path().join("generation-0")).unwrap();
                let path = generation.path().join("unrecognized.marker");
                std::fs::write(&path, b"preserve unavailable derived state").unwrap();
                path
            }
            #[cfg(unix)]
            Failure::SymlinkVersion => {
                let target =
                    private_fs::PrivateDir::create(temporary.path().join("external-index"))
                        .unwrap();
                let path = target.path().join("unrecognized.marker");
                std::fs::write(&path, b"preserve unavailable derived state").unwrap();
                std::os::unix::fs::symlink(target.path(), &version_path).unwrap();
                path
            }
            _ => {
                let version = private_fs::PrivateDir::create(&version_path).unwrap();
                let generation =
                    private_fs::PrivateDir::create(version.path().join("generation-0")).unwrap();
                drop(generation.open_lock("index.lock").unwrap());
                let database =
                    private_fs::PrivateDir::create(generation.path().join("index.fjall")).unwrap();
                let path = database.path().join("unrecognized.marker");
                std::fs::write(&path, b"preserve unavailable derived state").unwrap();
                match failure {
                    Failure::InvalidDatabaseVersion => {
                        std::fs::write(database.path().join("version"), b"NOPE").unwrap()
                    }
                    #[cfg(unix)]
                    Failure::SymlinkDatabaseVersion => {
                        let target = temporary.path().join("external-version");
                        std::fs::write(&target, b"FJL\x03").unwrap();
                        std::os::unix::fs::symlink(target, database.path().join("version"))
                            .unwrap();
                    }
                    _ => {}
                }
                path
            }
        };
        let preserved = match failure {
            Failure::GenerationFile => None,
            #[cfg(unix)]
            Failure::SymlinkVersion => Some(sentinel.parent().unwrap().to_owned()),
            _ => Some(version_path.join("generation-0")),
        }
        .map(|path| {
            let before = generation_contents(&path);
            (path, before)
        });
        let mut config = tinytree_server::Config::default();
        config.storage = storage;
        config.repositories = vec!["alpha".into()];
        config.gc_interval_seconds = 0;
        let server = Running::start_production(config).await;
        let mut objects = server.client().await;
        objects
            .open_repository(p::OpenRepositoryRequest {
                repository_id: "alpha".into(),
                reference_names: Vec::new(),
            })
            .await
            .unwrap();
        let mut query = server.query_client().await;
        assert_eq!(
            query
                .open_scope(request(
                    tokio_stream::iter(query_frames(
                        &[ObjectKey {
                            kind: Kind::Commit,
                            id: ObjectId([0; 32])
                        }],
                        1
                    )),
                    ALPHA
                ))
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        drop(query);
        drop(objects);
        server.stop().await;
        assert_eq!(
            std::fs::read(&sentinel).unwrap(),
            b"preserve unavailable derived state"
        );
        if let Some((path, before)) = preserved {
            assert_eq!(
                generation_contents(&path),
                before,
                "rejected derived storage must remain untouched, including missing version and engine files"
            );
        }
        #[cfg(unix)]
        if matches!(failure, Failure::SymlinkDatabaseVersion) {
            assert_eq!(
                std::fs::read(temporary.path().join("external-version")).unwrap(),
                b"FJL\x03",
                "rejected marker links must not modify their target"
            );
        }
        match failure {
            Failure::UnrecognizedGeneration => assert!(
                !sentinel.parent().unwrap().join("index.lock").exists(),
                "unrecognized state must not initialize an index beside unrelated data"
            ),
            #[cfg(unix)]
            Failure::SymlinkVersion => assert!(
                !sentinel.parent().unwrap().join("generation-0").exists(),
                "activation must not follow a version-directory symlink"
            ),
            _ => {}
        }
    }
}

async fn released_production_storage(storage: &std::path::Path, generation: &std::path::Path) {
    let storage = storage.to_owned();
    let generation = generation.to_owned();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let storage = storage.clone();
            let generation = generation.clone();
            let released = tokio::task::spawn_blocking(move || {
                let Ok(store) = Store::open_existing(storage, Default::default()) else {
                    return false;
                };
                let Ok(lock) = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(generation.join("index.lock"))
                else {
                    return false;
                };
                let released = lock.try_lock().is_ok();
                drop(lock);
                drop(store);
                released
            })
            .await
            .unwrap();
            if released {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[derive(Debug, PartialEq, Eq)]
enum GenerationEntry {
    Directory,
    File(Vec<u8>),
    Symlink(std::path::PathBuf),
}
fn generation_contents(
    directory: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, GenerationEntry> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        result: &mut std::collections::BTreeMap<std::path::PathBuf, GenerationEntry>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                result.insert(relative, GenerationEntry::Directory);
                visit(root, &path, result);
            } else if kind.is_symlink() {
                result.insert(
                    relative,
                    GenerationEntry::Symlink(std::fs::read_link(path).unwrap()),
                );
            } else {
                assert!(kind.is_file());
                result.insert(
                    relative,
                    GenerationEntry::File(std::fs::read(path).unwrap()),
                );
            }
        }
    }
    let mut result = std::collections::BTreeMap::new();
    visit(directory, directory, &mut result);
    result
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_index_generation_activation_reuses_ready_state_and_preserves_older_data() {
    let (temporary, store) = setup();
    let fixture = fixture();
    for object in fixture.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let head = root(&fixture);
    drop(store);
    let legacy = private_fs::PrivateDir::create(temporary.path().join("derived-index")).unwrap();
    std::fs::write(
        legacy.path().join("legacy.marker"),
        b"legacy generation remains operator-owned",
    )
    .unwrap();
    let legacy_contents = generation_contents(legacy.path());
    let previous_schema = private_fs::PrivateDir::create(temporary.path().join(format!(
        "derived-index-v{}",
        tinytree_index::STORAGE_SCHEMA - 1
    )))
    .unwrap();
    std::fs::write(
        previous_schema.path().join("previous-schema.marker"),
        b"previous schema remains operator-owned",
    )
    .unwrap();
    let previous_contents = generation_contents(previous_schema.path());
    let mut config = tinytree_server::Config::default();
    config.storage = temporary.path().to_owned();
    config.repositories = vec!["alpha".into(), "beta".into()];
    config.gc_interval_seconds = 0;
    let version = temporary
        .path()
        .join(format!("derived-index-v{}", tinytree_index::STORAGE_SCHEMA));
    let generation0 = version.join("generation-0");
    let server = Running::start_production(config.clone()).await;
    let mut client = server.query_client().await;
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(query_frames(&[head], 1)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let initial = query_ready(&mut client, &[head], 1).await;
    drop(client);
    server.stop().await;
    released_production_storage(temporary.path(), &generation0).await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Enforce Fjall's normal umask-022 directory mode even when the test
        // runner has a stricter umask. The private generation still encloses it.
        std::fs::set_permissions(
            generation0.join("index.fjall"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
    }
    let server = Running::start_production(config.clone()).await;
    let mut client = server.query_client().await;
    assert_eq!(
        client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: initial.key.clone(),
                    facts: Vec::new()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let reused = client
        .open_scope(request(tokio_stream::iter(query_frames(&[head], 2)), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reused.index_epoch, initial.index_epoch);
    assert_eq!(reused.high_water, initial.high_water);
    assert_ne!(reused.key, initial.key);
    drop(client);
    server.stop().await;
    released_production_storage(temporary.path(), &generation0).await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(generation0.join("index.fjall"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755,
            "valid engine permissions must be preserved on restart"
        );
    }
    let old_contents = generation_contents(&generation0);
    config.service.query.index_rebuild_generation = 1;
    let server = Running::start_production(config).await;
    let mut client = server.query_client().await;
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(query_frames(&[head], 3)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let rebuilt = query_ready(&mut client, &[head], 3).await;
    assert_ne!(rebuilt.index_epoch, initial.index_epoch);
    drop(client);
    server.stop().await;
    released_production_storage(temporary.path(), &version.join("generation-1")).await;
    assert_eq!(
        generation_contents(&generation0),
        old_contents,
        "selecting another generation must not rewrite the previous index"
    );
    assert_eq!(
        generation_contents(legacy.path()),
        legacy_contents,
        "schema activation must not migrate or delete legacy state"
    );
    assert_eq!(
        generation_contents(previous_schema.path()),
        previous_contents,
        "schema activation must not open or rewrite a previous format"
    );
}

fn private_query_frames(
    roots: &[ObjectKey],
    replay: u8,
    objects: &[&tinytree_model::EncodedObject],
) -> Vec<q::OpenScopeFrame> {
    let mut frames = query_frames(roots, replay);
    let bytes = objects.iter().map(|object| object.bytes.len() as u64).sum();
    let Some(q::open_scope_frame::Frame::Header(header)) = &mut frames[0].frame else {
        unreachable!()
    };
    header.overlay_object_count = objects.len() as u64;
    header.overlay_bytes = bytes;
    frames.pop();
    for object in objects {
        frames.push(q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::Commit(q::OverlayCommitHeader {
                descriptor: Some(descriptor(object.descriptor)),
            })),
        });
        frames.push(q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::Chunk(p::DataChunk {
                offset: 0,
                data: object.bytes.clone(),
            })),
        });
        frames.push(q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::CommitEnd(q::OverlayCommitEnd {
                size_bytes: object.bytes.len() as u64,
                content_blake3: object.descriptor.key.id.0.to_vec(),
            })),
        });
    }
    frames.push(q::OpenScopeFrame {
        frame: Some(q::open_scope_frame::Frame::ScopeEnd(q::ScopeEnd {
            overlay_object_count: objects.len() as u64,
            overlay_bytes: bytes,
        })),
    });
    frames
}
fn private_fork_frames(
    parent: q::ScopeKey,
    frames: Vec<q::OpenScopeFrame>,
) -> Vec<q::ForkScopeFrame> {
    frames
        .into_iter()
        .map(|frame| q::ForkScopeFrame {
            frame: frame.frame.map(|frame| match frame {
                q::open_scope_frame::Frame::Header(scope) => {
                    q::fork_scope_frame::Frame::Header(q::ForkScopeHeader {
                        scope: Some(scope),
                        parent_scope: Some(parent.clone()),
                    })
                }
                q::open_scope_frame::Frame::Commit(v) => q::fork_scope_frame::Frame::Commit(v),
                q::open_scope_frame::Frame::Chunk(v) => q::fork_scope_frame::Frame::Chunk(v),
                q::open_scope_frame::Frame::CommitEnd(v) => {
                    q::fork_scope_frame::Frame::CommitEnd(v)
                }
                q::open_scope_frame::Frame::ScopeEnd(v) => q::fork_scope_frame::Frame::ScopeEnd(v),
            }),
        })
        .collect()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_query_scopes_share_forks_without_publishing_or_disclosing_drafts() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let canonical = fixture();
    let ancestor = root(&canonical);
    let mut remote_commit = None;
    for object in canonical.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            index
                .ingest("alpha", object.descriptor, &object.bytes)
                .unwrap();
            if object.descriptor.key == ancestor {
                let Object::Commit(value) =
                    tinytree_model::decode(&object.bytes, Limits::default()).unwrap()
                else {
                    unreachable!()
                };
                remote_commit = Some(value);
            }
        }
    }
    let mut commit = remote_commit.unwrap();
    commit.parents = vec![CommitId(ancestor.id.0)];
    commit.change_id = tinytree_model::ChangeId([70; 16]);
    let remote =
        tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    store
        .put("alpha", remote.descriptor, &remote.bytes, 0)
        .unwrap();
    index
        .ingest("alpha", remote.descriptor, &remote.bytes)
        .unwrap();
    let checkpoint = remote.descriptor.key;
    let server = Running::start_query(store.clone(), ServiceConfig::default(), index.clone()).await;
    let mut client = server.query_client().await;
    commit.parents = vec![CommitId(checkpoint.id.0)];
    commit.change_id = tinytree_model::ChangeId([71; 16]);
    let first = tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    commit.description = "private divergent sibling".into();
    let sibling =
        tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    let frames = private_query_frames(&[checkpoint], 10, &[&first, &sibling]);
    let parent = client
        .open_scope(request(tokio_stream::iter(frames.clone()), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(frames.clone()), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        parent
    );
    let base = query_ready(&mut client, &[checkpoint], 11).await;
    for (scope, expected) in [(base.key.clone(), false), (parent.key.clone(), true)] {
        let facts = client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope,
                    facts: vec![q::GraphFact {
                        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                            commit: Some(query_id(first.descriptor.key)),
                        })),
                    }],
                },
                ALPHA,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            facts.answers[0].answer,
            Some(q::graph_fact_answer::Answer::HasId(expected))
        );
    }
    assert_eq!(
        client
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: parent.key.clone(),
                    facts: vec![]
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let changes = client
        .resolve_change_prefix(request(
            q::ResolveChangePrefixRequest {
                selection: None,
                scope: parent.key.clone(),
                hex_prefix: "47".repeat(16),
                visible_heads: vec![query_id(first.descriptor.key)],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    let Some(q::change_prefix_result::Result::Single(changes)) = changes.result else {
        panic!("private change must resolve")
    };
    assert_eq!(changes.targets.len(), 2);
    assert_eq!(
        changes
            .targets
            .iter()
            .filter(|target| target.visible == Some(true))
            .count(),
        1
    );
    assert_eq!(
        changes
            .targets
            .iter()
            .filter(|target| target.visible == Some(false))
            .count(),
        1
    );
    client
        .close_scope(request(q::CloseScopeRequest { scope: base.key }, ALPHA))
        .await
        .unwrap();
    commit.parents = vec![CommitId(first.descriptor.key.id.0)];
    commit.change_id = tinytree_model::ChangeId([72; 16]);
    let child = tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    let forks = private_fork_frames(
        parent.key.clone().unwrap(),
        private_query_frames(&[checkpoint], 12, &[&child]),
    );
    let fork = client
        .fork_scope(request(tokio_stream::iter(forks.clone()), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fork.index_epoch, parent.index_epoch);
    assert_eq!(fork.high_water, parent.high_water);
    assert!(fork.expires_at_unix_seconds <= parent.expires_at_unix_seconds);
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: parent.key.clone(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    assert_eq!(
        client
            .fork_scope(request(tokio_stream::iter(forks), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        fork,
        "known replay must not need the now-closed parent handle"
    );
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(frames), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // No canonical refs or unexpired staging leases protect this checkpoint.
    // Only the live private child retains the shared authoritative root pin.
    store.gc("alpha", i64::MAX - 1).unwrap();
    for key in [checkpoint, ancestor] {
        assert!(
            store.descriptor("alpha", key).unwrap().is_some(),
            "fork must retain the nonintrinsic checkpoint and its ancestry"
        );
    }
    let facts = client
        .query_facts(request(
            q::QueryFactsRequest {
                scope: fork.key.clone(),
                facts: vec![q::GraphFact {
                    fact: Some(q::graph_fact::Fact::IsAncestor(q::IsAncestor {
                        ancestor: Some(query_id(first.descriptor.key)),
                        descendant: Some(query_id(child.descriptor.key)),
                    })),
                }],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        facts.answers[0].answer,
        Some(q::graph_fact_answer::Answer::IsAncestor(true))
    );
    for object in [&first, &sibling, &child] {
        assert!(
            store
                .descriptor("alpha", object.descriptor.key)
                .unwrap()
                .is_none()
        );
        assert!(
            index
                .node("alpha", CommitId(object.descriptor.key.id.0))
                .unwrap()
                .is_none()
        );
    }
    // A different valid payload cannot reuse the installed fork replay key.
    commit.description = "different private child".into();
    let changed = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    let changed = private_fork_frames(
        parent.key.unwrap(),
        private_query_frames(&[checkpoint], 12, &[&changed]),
    );
    assert_eq!(
        client
            .fork_scope(request(tokio_stream::iter(changed), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    client
        .close_scope(request(q::CloseScopeRequest { scope: fork.key }, ALPHA))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let store = store.clone();
            let reclaimed = tokio::task::spawn_blocking(move || {
                store.gc("alpha", i64::MAX - 1).unwrap();
                [checkpoint, ancestor]
                    .into_iter()
                    .all(|key| store.descriptor("alpha", key).unwrap().is_none())
            })
            .await
            .unwrap();
            if reclaimed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_query_streams_reject_incomplete_duplicate_and_nonmember_payloads_atomically() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let server = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![CommitId::ROOT];
    commit.change_id = tinytree_model::ChangeId([73; 16]);
    let valid = tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    let mut truncated = private_query_frames(&[zero], 20, &[&valid]);
    truncated.pop();
    let mut gap = private_query_frames(&[zero], 21, &[&valid]);
    let Some(q::open_scope_frame::Frame::Chunk(chunk)) = &mut gap[2].frame else {
        unreachable!()
    };
    chunk.offset = 1;
    let duplicate = private_query_frames(&[zero], 22, &[&valid, &valid]);
    commit.parents = vec![CommitId([93; 32])];
    let unknown = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    let unknown = private_query_frames(&[zero], 23, &[&unknown]);
    let mut oversized = query_frames(&[zero], 24);
    let Some(q::open_scope_frame::Frame::Header(header)) = &mut oversized[0].frame else {
        unreachable!()
    };
    header.overlay_object_count = 4097;
    header.overlay_bytes = 1;
    for (name, frames, code) in [
        ("truncated", truncated, Code::InvalidArgument),
        ("gap", gap, Code::InvalidArgument),
        ("duplicate", duplicate, Code::InvalidArgument),
        ("unknown parent", unknown, Code::InvalidArgument),
        ("declaration limit", oversized, Code::ResourceExhausted),
    ] {
        assert_eq!(
            client
                .open_scope(request(tokio_stream::iter(frames), ALPHA))
                .await
                .unwrap_err()
                .code(),
            code,
            "{name}"
        );
    }
    // A failed stream installs neither a scope nor a replay tombstone.
    client
        .open_scope(request(
            tokio_stream::iter(private_query_frames(&[zero], 20, &[&valid])),
            ALPHA,
        ))
        .await
        .unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_query_reservations_follow_shared_physical_scope_lifetimes() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![CommitId::ROOT];
    commit.change_id = tinytree_model::ChangeId([74; 16]);
    let object = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_scope_overlay_bytes = object.bytes.len();
    config.query.max_scope_overlay_nodes = 1;
    config.query.max_overlay_memory_bytes = object.bytes.len() * 4 + 512;
    config.rpc.stream_idle_timeout_ms = 100;
    let server = Running::start_query(store, config, index).await;
    let mut client = server.query_client().await;
    let frames = private_query_frames(&[zero], 30, &[&object]);
    let parent = client
        .open_scope(request(tokio_stream::iter(frames.clone()), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        client
            .open_scope(request(tokio_stream::iter(frames), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        parent,
        "replay does not allocate another private graph"
    );
    let fork = client
        .fork_scope(request(
            tokio_stream::iter(private_fork_frames(
                parent.key.clone().unwrap(),
                query_frames(&[zero], 31),
            )),
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    client
        .close_scope(request(q::CloseScopeRequest { scope: parent.key }, ALPHA))
        .await
        .unwrap();
    assert_eq!(
        client
            .open_scope(request(
                tokio_stream::iter(private_query_frames(&[zero], 32, &[&object])),
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted,
        "child retains the shared private reservation after parent close"
    );
    client
        .close_scope(request(q::CloseScopeRequest { scope: fork.key }, ALPHA))
        .await
        .unwrap();
    // Blocking cleanup, not CloseScope response timing, determines availability.
    let recovered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .open_scope(request(
                    tokio_stream::iter(private_query_frames(&[zero], 32, &[&object])),
                    ALPHA,
                ))
                .await
            {
                Ok(value) => break value.into_inner(),
                Err(error) if error.code() == Code::ResourceExhausted => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                other => panic!("reservation did not become available: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: recovered.key,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    // Incomplete accepted bodies have an idle deadline and cannot hold a
    // preliminary anchor/reservation forever after a caller stops producing.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (sender, receiver) = mpsc::channel(1);
            sender
                .send(private_query_frames(&[zero], 33, &[&object]).remove(0))
                .await
                .unwrap();
            let error = client
                .open_scope(request(
                    tokio_stream::wrappers::ReceiverStream::new(receiver),
                    ALPHA,
                ))
                .await
                .unwrap_err();
            drop(sender);
            match error.code() {
                Code::DeadlineExceeded => break,
                Code::ResourceExhausted => tokio::time::sleep(Duration::from_millis(5)).await,
                other => panic!("accepted incomplete stream must expire: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .open_scope(request(
                    tokio_stream::iter(private_query_frames(&[zero], 34, &[&object])),
                    ALPHA,
                ))
                .await
            {
                Ok(_) => break,
                Err(error) if error.code() == Code::ResourceExhausted => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                other => panic!("incomplete stream leaked its reservation: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    drop(client);
    server.stop().await;
}

fn ids_evaluation(scope: &q::QueryScope, query: u8, commits: &[ObjectKey]) -> q::EvaluateRequest {
    q::EvaluateRequest {
        scope: scope.key.clone(),
        query_id: vec![query; 32],
        expression: Some(q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: commits.iter().copied().map(query_id).collect(),
            })),
        }),
        projection: q::Projection::Ids as i32,
        page_size: 128,
        cursor: Vec::new(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ids_query_pages_replay_concurrently_and_retain_roots_after_scope_close() {
    let (temporary, store) = setup();
    let canonical = fixture();
    let ancestor = root(&canonical);
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut parent = None;
    for object in canonical.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            index
                .ingest("alpha", object.descriptor, &object.bytes)
                .unwrap();
            if object.descriptor.key == ancestor {
                let Object::Commit(commit) =
                    tinytree_model::decode(&object.bytes, Limits::default()).unwrap()
                else {
                    unreachable!()
                };
                parent = Some(commit);
            }
        }
    }
    let mut commit = parent.unwrap();
    commit.parents = vec![CommitId(ancestor.id.0)];
    commit.change_id = tinytree_model::ChangeId([81; 16]);
    let head = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store.put("alpha", head.descriptor, &head.bytes, 0).unwrap();
    index.ingest("alpha", head.descriptor, &head.bytes).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_eval_quantum_steps = 1;
    let server = Running::start_query(store.clone(), config, index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[head.descriptor.key], 50).await;
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let input = ids_evaluation(&scope, 51, &[ancestor, zero, head.descriptor.key]);
    let mut other = client.clone();
    let (first, replayed) = tokio::join!(
        client.evaluate(request(input.clone(), ALPHA)),
        other.evaluate(request(input.clone(), ALPHA))
    );
    let first = first.unwrap().into_inner();
    assert_eq!(replayed.unwrap().into_inner(), first);
    assert_eq!(first.sequence, 0);
    assert_eq!(first.exhausted, Some(false));
    assert!(!first.cursor.is_empty());
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: scope.key.clone(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    store.gc("alpha", i64::MAX - 1).unwrap();
    assert!(
        store
            .descriptor("alpha", head.descriptor.key)
            .unwrap()
            .is_some()
    );
    let mut altered = input.clone();
    altered.page_size = 1;
    assert_eq!(
        client
            .evaluate(request(altered, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut next = input.clone();
    next.cursor = first.cursor.clone();
    let mut forged = next.clone();
    forged.cursor[0] ^= 1;
    assert_eq!(
        client
            .evaluate(request(forged, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .evaluate(request(next.clone(), PEER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut rows = first.rows.clone();
    let mut sequence = 1;
    let mut saw_terminal = false;
    for _ in 0..128 {
        let page = client
            .evaluate(request(next.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.sequence, sequence);
        sequence += 1;
        assert_eq!(page.scope_digest, scope.content_digest);
        assert_eq!(page.index_epoch, scope.index_epoch);
        assert_eq!(
            client
                .evaluate(request(next.clone(), ALPHA))
                .await
                .unwrap()
                .into_inner(),
            page,
            "latest input replay is byte-equivalent"
        );
        assert_eq!(
            client
                .evaluate(request(input.clone(), ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "older input never advances to the current page"
        );
        assert!(
            page.rows
                .iter()
                .all(|row| row.edges.is_empty() && row.canonical.is_none())
        );
        rows.extend(page.rows.clone());
        if page.exhausted == Some(true) {
            assert!(page.cursor.is_empty());
            saw_terminal = true;
            break;
        }
        assert_eq!(page.exhausted, Some(false));
        assert!(!page.cursor.is_empty());
        next.cursor = page.cursor;
    }
    assert!(saw_terminal);
    assert_eq!(
        rows.iter()
            .map(|row| row.commit.clone().unwrap())
            .collect::<Vec<_>>(),
        vec![
            query_id(head.descriptor.key),
            query_id(ancestor),
            query_id(zero)
        ]
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let store = store.clone();
            let removed = tokio::task::spawn_blocking(move || {
                store.gc("alpha", i64::MAX - 1).unwrap();
                store
                    .descriptor("alpha", head.descriptor.key)
                    .unwrap()
                    .is_none()
            })
            .await
            .unwrap();
            if removed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
    drop(other);
    server.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_queries_expire_and_release_authoritative_roots_without_next_page() {
    for projection in [
        q::Projection::Ids,
        q::Projection::Graph,
        q::Projection::Canonical,
    ] {
        let (temporary, store) = setup();
        let canonical = fixture();
        let root = root(&canonical);
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        for object in canonical.objects() {
            store
                .put("alpha", object.descriptor, &object.bytes, 0)
                .unwrap();
            if object.descriptor.key.kind == Kind::Commit {
                index
                    .ingest("alpha", object.descriptor, &object.bytes)
                    .unwrap();
            }
        }
        let mut config = ServiceConfig::default();
        config.query.max_eval_quantum_steps = 1;
        config.query.query_ttl_seconds = 2;
        config.query.expiry_interval_ms = 20;
        config.query.max_queries = 1;
        config.query.max_queries_per_principal = 1;
        config.query.max_queries_per_repository = 1;
        let server = Running::start_query(store.clone(), config, index).await;
        let mut client = server.query_client().await;
        let scope = query_ready(&mut client, &[root], 60).await;
        let mut input = ids_evaluation(&scope, 61, &[root]);
        input.projection = projection as i32;
        let first = client
            .evaluate(request(input.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.exhausted, Some(false));
        let mut second = input.clone();
        second.query_id = vec![62; 32];
        assert_eq!(
            client
                .evaluate(request(second, ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        client
            .close_scope(request(q::CloseScopeRequest { scope: scope.key }, ALPHA))
            .await
            .unwrap();
        store.gc("alpha", i64::MAX - 1).unwrap();
        assert!(store.descriptor("alpha", root).unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(2300)).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let store = store.clone();
                let removed = tokio::task::spawn_blocking(move || {
                    store.gc("alpha", i64::MAX - 1).unwrap();
                    store.descriptor("alpha", root).unwrap().is_none()
                })
                .await
                .unwrap();
                if removed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let mut expired = input;
        expired.cursor = first.cursor;
        assert_eq!(
            client
                .evaluate(request(expired, ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        drop(client);
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ids_query_budget_exhaustion_is_an_error_not_an_exhausted_page() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_eval_steps = 1;
    config.query.max_eval_step_work = 1;
    config.query.max_eval_quantum_steps = 1;
    let server = Running::start_query(store, config, index).await;
    let mut client = server.query_client().await;
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let scope = query_ready(&mut client, &[zero], 70).await;
    let mut input = ids_evaluation(&scope, 71, &[zero]);
    let mut failed = false;
    for _ in 0..8 {
        match client.evaluate(request(input.clone(), ALPHA)).await {
            Ok(response) => {
                let page = response.into_inner();
                assert_eq!(
                    page.exhausted,
                    Some(false),
                    "cumulative step exhaustion cannot prove completion"
                );
                assert!(!page.cursor.is_empty());
                input.cursor = page.cursor;
            }
            Err(error) => {
                assert_eq!(error.code(), Code::ResourceExhausted);
                assert_eq!(
                    client
                        .evaluate(request(input.clone(), ALPHA))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::ResourceExhausted,
                    "failed quantum is replayable without renewed work budget"
                );
                failed = true;
                break;
            }
        }
    }
    assert!(failed);
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_query_warming_streams_chain_payloads_within_one_object_residency() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut commit = tinytree_model::synthetic_root();
    let mut parent = CommitId::ROOT;
    let mut keys = Vec::new();
    let mut canonical_bytes = 0usize;
    for generation in 0..16u8 {
        commit.parents = vec![parent];
        commit.change_id = tinytree_model::ChangeId([generation + 1; 16]);
        commit.description = format!("cold chain generation {generation}");
        let object =
            tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
        assert!(
            object.bytes.len() <= 512,
            "each canonical commit fits the retained payload budget"
        );
        canonical_bytes += object.bytes.len();
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        keys.push(object.descriptor.key);
        parent = CommitId(object.descriptor.key.id.0);
    }
    assert!(
        canonical_bytes > 512,
        "the complete chain cannot fit the old retained-payload traversal"
    );
    let mut config = ServiceConfig::default();
    config.query.max_warm_bytes = 512;
    let server = Running::start_query(store, config, index.clone()).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[*keys.last().unwrap()], 80).await;
    for key in &keys {
        assert!(
            index.node("alpha", CommitId(key.id.0)).unwrap().is_some(),
            "parent-first warming must publish every chain commit"
        );
    }
    let facts = client
        .query_facts(request(
            q::QueryFactsRequest {
                scope: scope.key,
                facts: keys
                    .into_iter()
                    .map(|key| q::GraphFact {
                        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                            commit: Some(query_id(key)),
                        })),
                    })
                    .collect(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(facts.answers.len(), 16);
    assert!(
        facts
            .answers
            .iter()
            .all(|answer| answer.answer == Some(q::graph_fact_answer::Answer::HasId(true)))
    );
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_query_warming_charges_only_actual_cache_miss_reads() {
    for (name, retained_bytes, read_multiplier, ready) in [
        (
            "fitting cache reads each commit once",
            16 * 1024 * 1024,
            1,
            true,
        ),
        (
            "evicted payloads require additional read budget",
            512,
            1,
            false,
        ),
        ("evicted payloads progress with reread budget", 512, 2, true),
    ] {
        let (temporary, store) = setup();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        let mut commit = tinytree_model::synthetic_root();
        let mut parent = CommitId::ROOT;
        let mut total = 0usize;
        let mut keys = Vec::new();
        for generation in 0..16u8 {
            commit.parents = vec![parent];
            commit.change_id = tinytree_model::ChangeId([generation + 101; 16]);
            commit.description = format!("cache read budget chain {generation}");
            let object =
                tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
            assert!(object.bytes.len() <= 512);
            total += object.bytes.len();
            store
                .put("alpha", object.descriptor, &object.bytes, 0)
                .unwrap();
            keys.push(object.descriptor.key);
            parent = CommitId(object.descriptor.key.id.0);
        }
        assert!(total > 512);
        let mut config = ServiceConfig::default();
        config.query.max_warm_bytes = retained_bytes;
        config.query.max_warm_read_bytes = total * read_multiplier;
        let server = Running::start_query(store, config, index.clone()).await;
        let mut client = server.query_client().await;
        let frames = query_frames(&[*keys.last().unwrap()], 81);
        assert_eq!(
            client
                .open_scope(request(tokio_stream::iter(frames.clone()), ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match client
                    .open_scope(request(tokio_stream::iter(frames.clone()), ALPHA))
                    .await
                {
                    Err(error) if error.code() == Code::Unavailable => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    Err(error) => {
                        assert!(!ready, "{name}: {error}");
                        assert_eq!(error.code(), Code::ResourceExhausted, "{name}");
                        break;
                    }
                    Ok(_) => {
                        assert!(
                            ready,
                            "{name}: read-work exhaustion must not advertise complete coverage"
                        );
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            index.node("alpha", parent).unwrap().is_some(),
            ready,
            "{name}"
        );
        if ready {
            assert_eq!(index.stats().unwrap().nodes, keys.len() as u64, "{name}");
        }
        drop(client);
        server.stop().await;
    }
}

// BEGIN TINYTREE_COLD_WARM_READ_POLICY_BENCH_V1
// Copy this entire block unchanged for a same-fixture source-revision comparison.
// Warm canonical storage, fresh derived index, production TCP/zstd query RPCs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual cold-index read-policy benchmark; bounded to 1100 seconds"]
async fn benchmark_cold_warming_read_policy() {
    tokio::time::timeout(Duration::from_secs(1100), async {
        for count in [1000usize, 16_000] {
            let temporary = tempfile::tempdir().unwrap();
            let opened = std::time::Instant::now();
            let store = Store::open(temporary.path().join("canonical"), Default::default()).unwrap();
            store.create_repository("alpha").unwrap();
            store.create_repository("beta").unwrap();
            let source_open_ms = opened.elapsed().as_secs_f64() * 1000.0;
            let seeded = std::time::Instant::now();
            let mut parent = CommitId::ROOT;
            let mut keys = Vec::with_capacity(count);
            let mut canonical_payload_bytes = 0u64;
            let mut commit = tinytree_model::synthetic_root();
            for generation in 0..count {
                commit.parents = vec![parent];
                let mut change = [0u8; 16];
                change[..8].copy_from_slice(&(generation as u64 + 1).to_be_bytes());
                commit.change_id = tinytree_model::ChangeId(change);
                commit.description = format!("cold warm policy chain {generation}");
                let object = tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
                assert!(object.bytes.len() <= ServiceConfig::default().query.max_commit_bytes);
                canonical_payload_bytes += object.bytes.len() as u64;
                store.put("alpha", object.descriptor, &object.bytes, 0).unwrap();
                parent = CommitId(object.descriptor.key.id.0);
                keys.push(object.descriptor.key);
            }
            let seed_ms = seeded.elapsed().as_secs_f64() * 1000.0;
            assert!(canonical_payload_bytes < ServiceConfig::default().query.max_warm_bytes as u64,
                "both source revisions must admit the same complete graph under their default limits");
            for trial in 0..3 {
                let opened = std::time::Instant::now();
                let index = tinytree_index::Index::open(temporary.path().join(format!("derived-{trial}")), &Default::default()).unwrap();
                assert_eq!(index.stats().unwrap().nodes, 0);
                let index_open_ms = opened.elapsed().as_secs_f64() * 1000.0;
                let started = std::time::Instant::now();
                let server = Running::start_query(store.clone(), ServiceConfig::default(), index.clone()).await;
                let server_start_ms = started.elapsed().as_secs_f64() * 1000.0;
                let connected = std::time::Instant::now();
                let mut client = server.query_client().await;
                let connect_ms = connected.elapsed().as_secs_f64() * 1000.0;
                let requested = std::time::Instant::now();
                let mut open_rpcs = 0u64;
                let scope = tokio::time::timeout(Duration::from_secs(300), async {
                    loop {
                        open_rpcs += 1;
                        match client.open_scope(request(tokio_stream::iter(query_frames(&[*keys.last().unwrap()], 90)), ALPHA)).await {
                            Ok(response) => break response.into_inner(),
                            Err(error) if error.code() == Code::Unavailable => tokio::time::sleep(Duration::from_millis(10)).await,
                            Err(error) => panic!("equivalent cold warming workload failed: {error}"),
                        }
                    }
                }).await.unwrap();
                let warm_ready_ms = requested.elapsed().as_secs_f64() * 1000.0;
                let verified = std::time::Instant::now();
                let mut fact_rpcs = 0u64;
                for batch in keys.chunks(ServiceConfig::default().query.max_facts) {
                    fact_rpcs += 1;
                    let facts = client.query_facts(request(q::QueryFactsRequest {
                        scope: scope.key.clone(),
                        facts: batch.iter().copied().map(|key| q::GraphFact {
                            fact: Some(q::graph_fact::Fact::HasId(q::HasId { commit: Some(query_id(key)) })),
                        }).collect(),
                    }, ALPHA)).await.unwrap().into_inner();
                    assert_eq!(facts.answers.len(), batch.len());
                    assert!(facts.answers.iter().all(|answer| answer.answer == Some(q::graph_fact_answer::Answer::HasId(true))));
                }
                let verify_ms = verified.elapsed().as_secs_f64() * 1000.0;
                let stats = index.stats().unwrap();
                assert_eq!(stats.nodes, count as u64);
                println!("TINYTREE_COLD_WARM_READ_POLICY_BENCH_V1 {}", serde_json::json!({
                    "commits":count,"trial":trial,"canonical_payload_bytes":canonical_payload_bytes,
                    "source_store_open_ms":source_open_ms,"source_seed_ms":seed_ms,
                    "index_open_ms":index_open_ms,"server_start_ms":server_start_ms,"connect_ms":connect_ms,
                    "warm_ready_ms":warm_ready_ms,"verification_ms":verify_ms,
                    "open_scope_rpcs":open_rpcs,"fact_rpcs":fact_rpcs,"poll_resolution_ms":10,
                    "index_nodes":stats.nodes,"index_logical_bytes":stats.logical_bytes,
                    "storage_state":"warm canonical storage; fresh empty derived index each trial",
                    "read_policy_comparison":"one-read baseline versus bounded payload caching with cache-miss rereads; physical reads not measured",
                    "physical_read_bytes":"not measured","client_file_rpcs":0,
                    "transport":"production TCP with zstd query request/response compression"
                }));
                drop(client);
                server.stop().await;
                drop(index);
            }
        }
    }).await.expect("bounded cold warming benchmark expired");
}
// END TINYTREE_COLD_WARM_READ_POLICY_BENCH_V1

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_graph_and_canonical_pages_preserve_edges_payloads_and_byte_carryover() {
    let (temporary, store) = setup();
    let canonical = fixture();
    let ancestor = root(&canonical);
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut commit = None;
    for object in canonical.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            index
                .ingest("alpha", object.descriptor, &object.bytes)
                .unwrap();
            if object.descriptor.key == ancestor {
                let Object::Commit(value) =
                    tinytree_model::decode(&object.bytes, Limits::default()).unwrap()
                else {
                    unreachable!()
                };
                commit = Some(value);
            }
        }
    }
    // Give both selected nonroot rows large canonical payloads so the byte cap,
    // not the requested row count, splits them across pages.
    let mut commit = commit.unwrap();
    commit.parents = vec![CommitId(ancestor.id.0)];
    commit.change_id = tinytree_model::ChangeId([111; 16]);
    commit.description = "remote selected row ".repeat(50);
    let selected_remote =
        tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    store
        .put(
            "alpha",
            selected_remote.descriptor,
            &selected_remote.bytes,
            0,
        )
        .unwrap();
    index
        .ingest("alpha", selected_remote.descriptor, &selected_remote.bytes)
        .unwrap();
    commit.parents = vec![CommitId(selected_remote.descriptor.key.id.0)];
    commit.change_id = tinytree_model::ChangeId([112; 16]);
    commit.description = "omitted remote parent".into();
    let omitted =
        tinytree_model::encode(&Object::Commit(commit.clone()), Limits::default()).unwrap();
    store
        .put("alpha", omitted.descriptor, &omitted.bytes, 0)
        .unwrap();
    index
        .ingest("alpha", omitted.descriptor, &omitted.bytes)
        .unwrap();
    commit.parents = vec![CommitId(omitted.descriptor.key.id.0)];
    commit.change_id = tinytree_model::ChangeId([113; 16]);
    commit.description = "private selected row ".repeat(50);
    let draft = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_response_bytes = selected_remote.bytes.len().max(draft.bytes.len()) + 512;
    let max_response = config.query.max_response_bytes;
    let server = Running::start_query(store.clone(), config, index).await;
    let mut client = server.query_client().await;
    let scope = client
        .open_scope(request(
            tokio_stream::iter(private_query_frames(
                &[omitted.descriptor.key],
                100,
                &[&draft],
            )),
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let selected = [draft.descriptor.key, selected_remote.descriptor.key, zero];
    let mut graph = ids_evaluation(&scope, 101, &selected);
    graph.projection = q::Projection::Graph as i32;
    let mut graph_rows = Vec::new();
    for _ in 0..128 {
        let page = client
            .evaluate(request(graph.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert!(page.rows.iter().all(|row| row.canonical.is_none()));
        graph_rows.extend(page.rows);
        if page.exhausted == Some(true) {
            break;
        }
        graph.cursor = page.cursor;
    }
    assert_eq!(graph_rows.len(), 3);
    assert_eq!(
        graph_rows[0].edges,
        vec![q::SelectedEdge {
            edge: Some(q::selected_edge::Edge::Indirect(q::IndirectEdge {
                target: Some(query_id(selected_remote.descriptor.key))
            }))
        }]
    );
    assert_eq!(
        graph_rows[1].edges,
        vec![q::SelectedEdge {
            edge: Some(q::selected_edge::Edge::Indirect(q::IndirectEdge {
                target: Some(query_id(zero))
            }))
        }],
        "omitted ancestry rejoins the explicitly selected intrinsic root"
    );
    assert!(graph_rows[2].edges.is_empty());
    let mut input = ids_evaluation(&scope, 102, &selected);
    input.projection = q::Projection::Canonical as i32;
    let mut rows = Vec::new();
    let mut nonempty_pages = 0;
    let mut closed = false;
    let mut terminal = false;
    for _ in 0..128 {
        let page = client
            .evaluate(request(input.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            client
                .evaluate(request(input.clone(), ALPHA))
                .await
                .unwrap()
                .into_inner(),
            page,
            "lost acknowledgement retry preserves selected edges and canonical bytes"
        );
        assert!(prost::Message::encoded_len(&page) <= max_response);
        assert!(
            page.rows
                .iter()
                .filter(|row| row.canonical.is_some())
                .count()
                <= 1,
            "byte carryover limits each page despite page_size128"
        );
        if !page.rows.is_empty() {
            nonempty_pages += 1;
        }
        rows.extend(page.rows.clone());
        if !closed {
            client
                .close_scope(request(
                    q::CloseScopeRequest {
                        scope: scope.key.clone(),
                    },
                    ALPHA,
                ))
                .await
                .unwrap();
            closed = true;
            store.gc("alpha", i64::MAX - 1).unwrap();
            assert!(
                store
                    .descriptor("alpha", omitted.descriptor.key)
                    .unwrap()
                    .is_some()
            );
        }
        if page.exhausted == Some(true) {
            terminal = true;
            assert!(page.cursor.is_empty());
            break;
        }
        input.cursor = page.cursor;
    }
    assert!(terminal);
    assert!(nonempty_pages >= 2);
    assert_eq!(rows.len(), 3);
    for (row, graph) in rows.iter().zip(&graph_rows) {
        assert_eq!(row.commit, graph.commit);
        assert_eq!(row.change, graph.change);
        assert_eq!(row.edges, graph.edges);
    }
    for (row, expected) in rows[..2].iter().zip([&draft, &selected_remote]) {
        let payload = row.canonical.as_ref().unwrap();
        assert_eq!(payload.descriptor, Some(descriptor(expected.descriptor)));
        assert_eq!(payload.canonical_envelope, expected.bytes);
        tinytree_model::verify(
            &expected.descriptor,
            &payload.canonical_envelope,
            Limits::default(),
        )
        .unwrap();
    }
    assert!(rows[2].canonical.is_none());
    assert!(
        store
            .descriptor("alpha", draft.descriptor.key)
            .unwrap()
            .is_none(),
        "canonical projection never publishes a private draft"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let store = store.clone();
            let key = omitted.descriptor.key;
            let removed = tokio::task::spawn_blocking(move || {
                store.gc("alpha", i64::MAX - 1).unwrap();
                store.descriptor("alpha", key).unwrap().is_none()
            })
            .await
            .unwrap();
            if removed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_graph_quota_allows_two_live_queries_and_many_tiny_completed_histories() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_eval_quantum_steps = 1;
    let server = Running::start_query(store, config, index).await;
    let mut client = server.query_client().await;
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let scope = query_ready(&mut client, &[zero], 110).await;
    let mut first = ids_evaluation(&scope, 111, &[zero]);
    first.projection = q::Projection::Canonical as i32;
    let mut second = ids_evaluation(&scope, 112, &[zero]);
    second.projection = q::Projection::Canonical as i32;
    let mut peer = client.clone();
    let (a, b) = tokio::join!(
        client.evaluate(request(first.clone(), ALPHA)),
        peer.evaluate(request(second.clone(), ALPHA))
    );
    let a = a.unwrap().into_inner();
    let b = b.unwrap().into_inner();
    assert_eq!(a.exhausted, Some(false));
    assert_eq!(b.exhausted, Some(false));
    first.cursor = a.cursor;
    second.cursor = b.cursor;
    for mut query in [first, second] {
        let mut done = false;
        for _ in 0..128 {
            let page = client
                .evaluate(request(query.clone(), ALPHA))
                .await
                .unwrap()
                .into_inner();
            if page.exhausted == Some(true) {
                done = true;
                break;
            }
            query.cursor = page.cursor;
        }
        assert!(done);
    }
    for id in 120..144u8 {
        let mut query = ids_evaluation(&scope, id, &[zero]);
        query.projection = q::Projection::Graph as i32;
        let mut done = false;
        for _ in 0..128 {
            let page = client
                .evaluate(request(query.clone(), ALPHA))
                .await
                .unwrap()
                .into_inner();
            if page.exhausted == Some(true) {
                assert_eq!(
                    client
                        .evaluate(request(query.clone(), ALPHA))
                        .await
                        .unwrap()
                        .into_inner(),
                    page
                );
                done = true;
                break;
            }
            query.cursor = page.cursor;
        }
        assert!(
            done,
            "tiny completed query history must not reserve a maximum-sized response forever"
        );
    }
    drop(client);
    drop(peer);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_query_releases_partial_graphs_and_precreation_close_prevents_resurrection() {
    let (temporary, store) = setup();
    let canonical = fixture();
    let root = root(&canonical);
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    for object in canonical.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            index
                .ingest("alpha", object.descriptor, &object.bytes)
                .unwrap();
        }
    }
    let mut config = ServiceConfig::default();
    config.query.max_eval_quantum_steps = 1;
    let server = Running::start_query(store.clone(), config, index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[root], 150).await;
    // More partial queries than the default pool can retain simultaneously.
    // Closing one page, rather than draining history, must permit the next agent.
    let mut final_input = None;
    for (id, projection) in (151..159u8).zip(
        [q::Projection::Graph, q::Projection::Canonical]
            .into_iter()
            .cycle(),
    ) {
        let mut input = ids_evaluation(&scope, id, &[root]);
        input.projection = projection as i32;
        let page = client
            .evaluate(request(input.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.exhausted, Some(false));
        input.cursor = page.cursor;
        let close = q::CloseQueryRequest {
            scope: scope.key.clone(),
            query_id: input.query_id.clone(),
        };
        client
            .close_query(request(close.clone(), ALPHA))
            .await
            .unwrap();
        client.close_query(request(close, ALPHA)).await.unwrap();
        assert_eq!(
            client
                .evaluate(request(input.clone(), ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        input.cursor.clear();
        input.page_size = 1;
        assert_eq!(
            client
                .evaluate(request(input.clone(), ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "closed state is checked before changed-input replay binding"
        );
        final_input = Some(input);
    }
    let before = ids_evaluation(&scope, 160, &[root]);
    let close = q::CloseQueryRequest {
        scope: scope.key.clone(),
        query_id: before.query_id.clone(),
    };
    client
        .close_query(request(close.clone(), ALPHA))
        .await
        .unwrap();
    client.close_query(request(close, ALPHA)).await.unwrap();
    assert_eq!(
        client
            .evaluate(request(before, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition,
        "close arriving before Evaluate must install an ordered tombstone"
    );
    let other_scope = query_ready(&mut client, &[root], 161).await;
    let known = final_input.unwrap();
    assert_eq!(
        client
            .close_query(request(
                q::CloseQueryRequest {
                    scope: other_scope.key.clone(),
                    query_id: known.query_id.clone()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .close_query(request(
                q::CloseQueryRequest {
                    scope: scope.key.clone(),
                    query_id: known.query_id.clone()
                },
                PEER
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .close_query(request(
                q::CloseQueryRequest {
                    scope: scope.key.clone(),
                    query_id: known.query_id.clone()
                },
                BETA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: scope.key.clone(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: other_scope.key,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    client
        .close_query(request(
            q::CloseQueryRequest {
                scope: scope.key,
                query_id: known.query_id,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let store = store.clone();
            let removed = tokio::task::spawn_blocking(move || {
                store.gc("alpha", i64::MAX - 1).unwrap();
                store.descriptor("alpha", root).unwrap().is_none()
            })
            .await
            .unwrap();
            if removed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn precreation_query_close_counts_history_not_active_quota_and_expires_absolutely() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let mut config = ServiceConfig::default();
    config.query.max_eval_quantum_steps = 1;
    config.query.max_queries = 1;
    config.query.max_queries_per_principal = 1;
    config.query.max_queries_per_repository = 1;
    config.query.max_query_replays = 2;
    config.query.max_query_replays_per_principal = 2;
    config.query.max_query_replays_per_repository = 2;
    config.query.query_ttl_seconds = 2;
    config.query.expiry_interval_ms = 20;
    let server = Running::start_query(store, config, index).await;
    let mut client = server.query_client().await;
    let zero = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId([0; 32]),
    };
    let scope = query_ready(&mut client, &[zero], 170).await;
    let mut active = ids_evaluation(&scope, 171, &[zero]);
    active.projection = q::Projection::Graph as i32;
    let page = client
        .evaluate(request(active.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.exhausted, Some(false));
    let early = q::CloseQueryRequest {
        scope: scope.key.clone(),
        query_id: vec![172; 32],
    };
    client
        .close_query(request(early.clone(), ALPHA))
        .await
        .unwrap();
    assert_eq!(
        client
            .close_query(request(
                q::CloseQueryRequest {
                    scope: scope.key.clone(),
                    query_id: vec![173; 32]
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted,
        "closed precreation records retain bounded replay history"
    );
    client
        .close_query(request(
            q::CloseQueryRequest {
                scope: scope.key.clone(),
                query_id: active.query_id.clone(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    client.close_query(request(early, ALPHA)).await.unwrap();
    assert_eq!(
        client
            .evaluate(request(active, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    tokio::time::sleep(Duration::from_millis(2300)).await;
    // A fresh identifier works after absolute tombstone expiry without any
    // sliding lease extension from repeated CloseQuery calls.
    let fresh = ids_evaluation(&scope, 173, &[zero]);
    client.evaluate(request(fresh, ALPHA)).await.unwrap();
    let mut malformed = q::CloseQueryRequest {
        scope: scope.key.clone(),
        query_id: vec![1; 31],
    };
    assert_eq!(
        client
            .close_query(request(malformed.clone(), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    malformed.query_id = vec![174; 32];
    malformed.scope.as_mut().unwrap().handle = vec![0; 32];
    client.close_query(request(malformed, ALPHA)).await.unwrap();
    drop(client);
    server.stop().await;
}

async fn cloud_fixture(
    fixture: &Fixture,
) -> (
    tinytree_cloud::Manifest,
    Vec<tinytree_cloud::VerifiedObject>,
) {
    use jj_lib::{
        backend::{MillisSinceEpoch, Timestamp},
        object_id::ObjectId as _,
        op_store::{
            OpStore, Operation, OperationMetadata, RootOperationData, TimestampRange, View,
        },
        simple_op_store::SimpleOpStore,
    };
    let directory = tempfile::tempdir().unwrap();
    let source = SimpleOpStore::init(
        directory.path(),
        RootOperationData {
            root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
        },
    )
    .unwrap();
    let head = root(fixture);
    let commit = jj_lib::backend::CommitId::from_bytes(&head.id.0);
    let mut view = View::make_root(commit.clone());
    view.wc_commit_ids.insert("task".into(), commit);
    let view_id = source.write_view(&view).await.unwrap();
    let time = Timestamp {
        timestamp: MillisSinceEpoch(0),
        tz_offset: 0,
    };
    let operation = Operation {
        view_id: view_id.clone(),
        parents: vec![jj_lib::op_store::OperationId::from_bytes(&[0; 64])],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: time.clone(),
                end: time,
            },
            description: "snapshot task\n".repeat(6000),
            hostname: "agent".into(),
            username: "agent".into(),
            is_snapshot: true,
            workspace_name: Some("task".into()),
            attributes: Default::default(),
        },
        commit_predecessors: None,
    };
    let op_id = source.write_operation(&operation).await.unwrap();
    let objects = vec![
        tinytree_cloud::export_view(&source, &view_id, Default::default())
            .await
            .unwrap(),
        tinytree_cloud::export_operation(&source, &op_id, Default::default())
            .await
            .unwrap(),
    ];
    let native = fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key == head)
        .unwrap();
    let Object::Commit(commit) = tinytree_model::decode(&native.bytes, Limits::default()).unwrap()
    else {
        panic!("fixture commit");
    };
    let mut roots = vec![head];
    roots.extend(commit.root_tree.terms.iter().map(|tree| ObjectKey {
        kind: Kind::Tree,
        id: ObjectId(tree.0),
    }));
    roots.sort_by_key(|key| (key.kind as u8, key.id.0));
    roots.dedup();
    let mut members = objects
        .iter()
        .map(|object| tinytree_cloud::Member {
            id: object.id(),
            selected: object.digest().unwrap(),
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|member| member.id);
    let op = op_id.as_bytes().try_into().unwrap();
    (
        tinytree_cloud::Manifest {
            workspace: [71; 32],
            exporter: [72; 16],
            sequence: 1,
            operation_heads: vec![op],
            undo_operations: vec![],
            intent_operations: vec![],
            working_copy: tinytree_cloud::WorkingCopy {
                workspace_name: "task".into(),
                wc_operation: op,
                snapshot_operation: op,
                snapshot_commit: head.id.0,
                tree: commit.root_tree,
            },
            parents: vec![],
            members,
            intent_roots: vec![],
            native_roots: roots,
            coverage: tinytree_cloud::Coverage::FullRequired,
            detached: vec![],
        },
        objects,
    )
}
fn cloud_object_descriptor(
    object: &tinytree_cloud::VerifiedObject,
) -> tinytree_proto::cloud_v1::ObjectDescriptor {
    use tinytree_proto::cloud_v1 as c;
    let (kind, id) = match object.id() {
        tinytree_cloud::ObjectId::Operation(id) => (1, id),
        tinytree_cloud::ObjectId::View(id) => (2, id),
    };
    let digest = object.digest().unwrap();
    c::ObjectDescriptor {
        id: Some(c::ObjectId {
            kind,
            semantic_id: id.to_vec(),
        }),
        selected: Some(c::RawDigest {
            blake3: digest.blake3.to_vec(),
            size_bytes: digest.size_bytes,
        }),
    }
}
#[tokio::test]
async fn cloud_rpc_private_compressed_history_replay_gc_and_restart() {
    use jj_lib::object_id::ObjectId as _;
    use tinytree_proto::cloud_v1::{self as c, workspace_cloud_client::WorkspaceCloudClient};
    let directory = tempfile::tempdir().unwrap();
    let mut profile: tinytree_server::Config =
        toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
    profile.service.cloud.indexed_versions = true;
    profile.service.cloud.captured_versions = true;
    profile.service.cloud.max_capture_bytes = 128 * 1024;
    profile.service.cloud.max_inflight = 1;
    profile.service.query.max_scope_memory_bytes = 40 * 1024;
    profile.validate().unwrap();
    let store_config = profile
        .store
        .materialize(profile.service.max_metadata_bytes);
    let store = Store::open(directory.path(), store_config.clone()).unwrap();
    store.create_repository("alpha").unwrap();
    store.create_repository("beta").unwrap();
    let data = fixture();
    for object in data.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let (mut manifest, objects) = cloud_fixture(&data).await;
    let index =
        tinytree_index::Index::open(directory.path().join("query-index"), &Default::default())
            .unwrap();
    let server = Running::start_query(store.clone(), profile.service.clone(), index.clone()).await;
    let channel = Endpoint::from_shared(format!("http://{}", server.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut query = q::repository_query_client::RepositoryQueryClient::new(channel.clone())
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    let mut client = WorkspaceCloudClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(1024 * 1024);
    let mut leases = Vec::new();
    for object in &objects {
        let input = c::PutObjectRequest {
            repository_id: "alpha".into(),
            descriptor: Some(cloud_object_descriptor(object)),
            payload: object.payload().unwrap().to_vec(),
        };
        assert_eq!(
            client
                .put_object(request(input.clone(), READER))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let response = client.put_object(request(input, ALPHA)).await.unwrap();
        assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
        let receipt = response.into_inner();
        assert_eq!(receipt.descriptor, Some(cloud_object_descriptor(object)));
        leases.push(receipt.staging_lease);
    }
    let ids = objects
        .iter()
        .map(|object| cloud_object_descriptor(object).id.unwrap())
        .collect::<Vec<_>>();
    let lookup = c::LookupObjectsRequest {
        repository_id: "alpha".into(),
        ids: ids.clone(),
    };
    let peer = client
        .lookup_objects(request(lookup.clone(), PEER))
        .await
        .unwrap()
        .into_inner();
    assert!(
        peer.selections
            .iter()
            .all(|selection| selection.descriptor.is_none())
    );
    assert_eq!(
        client
            .lookup_objects(request(lookup.clone(), BETA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let denied = c::LookupObjectsRequest {
        repository_id: "beta".into(),
        ids: ids.clone(),
    };
    assert_eq!(
        client
            .lookup_objects(request(denied, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let selected = client
        .lookup_objects(request(lookup, ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(selected.selections.len(), 2);
    for object in &objects {
        let d = cloud_object_descriptor(object);
        let response = client
            .read_object(request(
                c::ReadObjectRequest {
                    repository_id: "alpha".into(),
                    id: d.id.clone(),
                },
                ALPHA,
            ))
            .await
            .unwrap();
        assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
        let mut stream = response.into_inner();
        let mut bytes = Vec::new();
        let mut eof = false;
        while let Some(chunk) = stream.message().await.unwrap() {
            assert_eq!(chunk.offset, bytes.len() as u64);
            assert_eq!(chunk.descriptor, Some(d.clone()));
            bytes.extend(chunk.data);
            eof = chunk.eof;
        }
        assert!(eof);
        assert_eq!(bytes, object.payload().unwrap());
    }
    let bytes = tinytree_cloud::encode_manifest(&manifest, Default::default()).unwrap();
    let first = c::PublishVersionRequest {
        repository_id: "alpha".into(),
        workspace_id: manifest.workspace.to_vec(),
        expected_generation: 0,
        idempotency_key: "first-cloud-capture".into(),
        manifest_id: tinytree_cloud::ManifestId::of(&bytes).0.to_vec(),
        manifest: bytes.clone(),
        staging_leases: leases,
        capture_data: vec![],
    };
    let published = client
        .publish_version(request(first.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.workspace.as_ref().unwrap().generation, 1);
    assert_eq!(
        client
            .publish_version(request(first.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        published
    );
    manifest.sequence = 2;
    manifest.parents = vec![tinytree_cloud::ManifestId::of(&bytes)];
    // Transition the same workspace from legacy history to an exact indexed
    // recovery version. Typed artifacts remain separate from OP/VIEW64.
    let intrinsic = tinytree_cloud::build_index_artifacts(
        &tinytree_cloud::PortableIndexSnapshot {
            roots: vec![tinytree_model::CommitId::ROOT],
            drafts: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let Object::Commit(mut frozen_model) = tinytree_model::decode(
        &data
            .objects()
            .iter()
            .find(|object| object.descriptor.key == root(&data))
            .unwrap()
            .bytes,
        Limits::default(),
    )
    .unwrap() else {
        panic!("fixture commit");
    };
    frozen_model.parents = vec![CommitId(root(&data).id.0)];
    let mut frozen = Vec::new();
    let mut draft_records = Vec::new();
    for order in 1..=2u64 {
        frozen_model.change_id = tinytree_model::ChangeId([80 + order as u8; 16]);
        frozen_model.description = format!("frozen baseline sibling {order}");
        let object =
            tinytree_model::encode(&Object::Commit(frozen_model.clone()), Limits::default())
                .unwrap();
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
        manifest.native_roots.push(object.descriptor.key);
        draft_records.push(tinytree_cloud::IndexDraft {
            descriptor: object.descriptor,
            change: frozen_model.change_id,
            parents: frozen_model.parents.clone(),
            order,
        });
        frozen.push(object);
    }
    manifest
        .native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    manifest.native_roots.dedup();
    draft_records.sort_by_key(|record| record.descriptor.key.id);
    let selected_index = tinytree_cloud::build_index_artifacts(
        &tinytree_cloud::PortableIndexSnapshot {
            roots: vec![tinytree_model::CommitId(
                manifest.working_copy.snapshot_commit,
            )],
            drafts: draft_records,
        },
        Default::default(),
    )
    .unwrap();
    let mut artifacts = intrinsic.artifacts;
    artifacts.extend(selected_index.artifacts);
    artifacts.sort_by_key(|artifact| artifact.descriptor.id);
    artifacts.dedup_by_key(|artifact| artifact.descriptor.id);
    let mut index_leases = Vec::new();
    for artifact in &artifacts {
        let (kind, semantic_id) = match artifact.descriptor.id {
            tinytree_cloud::IndexArtifactId::Root(id) => (1, id.0),
            tinytree_cloud::IndexArtifactId::Node(id) => (2, id.0),
        };
        let descriptor = c::IndexArtifactDescriptor {
            id: Some(c::IndexArtifactId {
                kind,
                semantic_id: semantic_id.to_vec(),
            }),
            selected: Some(c::RawDigest {
                blake3: artifact.descriptor.selected.blake3.to_vec(),
                size_bytes: artifact.descriptor.selected.size_bytes,
            }),
        };
        let put = c::PutIndexArtifactRequest {
            repository_id: "alpha".into(),
            descriptor: Some(descriptor.clone()),
            payload: artifact.bytes.clone(),
        };
        assert_eq!(
            client
                .put_index_artifact(request(put.clone(), READER))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let response = client
            .put_index_artifact(request(put, ALPHA))
            .await
            .unwrap();
        assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
        let receipt = response.into_inner();
        assert_eq!(receipt.descriptor, Some(descriptor.clone()));
        index_leases.push(receipt.staging_lease);
        let lookup = c::LookupIndexArtifactsRequest {
            repository_id: "alpha".into(),
            ids: vec![descriptor.id.clone().unwrap()],
        };
        assert!(
            client
                .lookup_index_artifacts(request(lookup.clone(), PEER))
                .await
                .unwrap()
                .into_inner()
                .selections[0]
                .descriptor
                .is_none()
        );
        assert_eq!(
            client
                .lookup_index_artifacts(request(lookup, ALPHA))
                .await
                .unwrap()
                .into_inner()
                .selections[0]
                .descriptor,
            Some(descriptor.clone())
        );
        let response = client
            .read_index_artifact(request(
                c::ReadIndexArtifactRequest {
                    repository_id: "alpha".into(),
                    id: descriptor.id.clone(),
                },
                ALPHA,
            ))
            .await
            .unwrap();
        assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
        let mut stream = response.into_inner();
        let mut bytes = Vec::new();
        let mut eof = false;
        while let Some(chunk) = stream.message().await.unwrap() {
            assert_eq!(chunk.descriptor, Some(descriptor.clone()));
            assert_eq!(chunk.offset, bytes.len() as u64);
            bytes.extend(chunk.data);
            eof = chunk.eof;
        }
        assert!(eof);
        assert_eq!(bytes, artifact.bytes);
    }
    let mut roots = vec![intrinsic.root, selected_index.root];
    roots.sort_by_key(|root| root.id);
    roots.dedup_by_key(|root| root.id);
    let indexed_manifest = tinytree_cloud::ManifestV2 {
        operation_indexes: vec![
            tinytree_cloud::OperationIndex {
                operation: [0; 64],
                root: intrinsic.root.id,
            },
            tinytree_cloud::OperationIndex {
                operation: manifest.working_copy.snapshot_operation,
                root: selected_index.root.id,
            },
        ],
        index_roots: roots,
        recovery: manifest.clone(),
    };
    // Protocol fixture: the policy is a publisher declaration, not a live
    // frontend proof. Actual native capture acceptance is tested by the host.
    let text = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let mut operations = vec![tinytree_cloud::OperationRecord {
        operation: "00".repeat(64),
        view: "00".repeat(64),
        parents: vec![],
    }];
    let mut blobs = Vec::new();
    for object in &objects {
        let (kind, id) = match object.id() {
            tinytree_cloud::ObjectId::Operation(id) => (1, id),
            tinytree_cloud::ObjectId::View(id) => (2, id),
        };
        let digest = object.digest().unwrap();
        blobs.push(tinytree_cloud::BlobRecord {
            kind,
            id: text(&id),
            blake3: digest.blake3,
            size: digest.size_bytes,
        });
        if let Some(operation) = object.operation() {
            operations.push(tinytree_cloud::OperationRecord {
                operation: text(&id),
                view: operation.view_id.hex(),
                parents: operation.parents.iter().map(|id| id.hex()).collect(),
            });
        }
    }
    operations.sort_by(|a, b| a.operation.cmp(&b.operation));
    for artifact in &artifacts {
        let (kind, id) = match artifact.descriptor.id {
            tinytree_cloud::IndexArtifactId::Root(id) => (3, id.0),
            tinytree_cloud::IndexArtifactId::Node(id) => (4, id.0),
        };
        blobs.push(tinytree_cloud::BlobRecord {
            kind,
            id: text(&id),
            blake3: artifact.descriptor.selected.blake3,
            size: artifact.descriptor.selected.size_bytes,
        });
    }
    blobs.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    let wc = &manifest.working_copy;
    let capture_data = tinytree_cloud::CaptureData {
        version: 1,
        policy: tinytree_cloud::CapturePolicy::HostQuiescentV1,
        codec: tinytree_cloud::CODEC.into(),
        operation_heads: manifest.operation_heads.iter().map(|id| text(id)).collect(),
        working_copy: tinytree_cloud::PortableWorkingCopy {
            workspace: wc.workspace_name.clone(),
            wc_operation: text(&wc.wc_operation),
            snapshot_operation: text(&wc.snapshot_operation),
            snapshot_commit: wc.snapshot_commit,
            tree: tinytree_cloud::CaptureTree {
                terms: wc.tree.terms.iter().map(|id| id.0).collect(),
                labels: if wc.tree.labels.iter().all(String::is_empty) {
                    vec![]
                } else {
                    wc.tree.labels.clone()
                },
            },
        },
        indexes: indexed_manifest
            .operation_indexes
            .iter()
            .map(|index| tinytree_cloud::IndexRecord {
                operation: text(&index.operation),
                root: index.root.0,
            })
            .collect(),
        operations,
        blobs,
        roots: manifest
            .native_roots
            .iter()
            .map(|key| tinytree_cloud::CapturedRoot {
                kind: key.kind as u8,
                id: key.id.0,
            })
            .collect(),
        remote: None,
    };
    let capture_bytes =
        tinytree_cloud::encode_capture_data(&capture_data, Default::default()).unwrap();
    let capture_descriptor = tinytree_cloud::CaptureDescriptor::of(&capture_bytes);
    let mut captured_history = indexed_manifest.clone();
    captured_history.recovery.sequence = 3;
    // Its parent is filled after the exact V2 encoding below.
    let next = tinytree_cloud::encode_manifest_v2(&indexed_manifest, Default::default()).unwrap();
    captured_history.recovery.parents = vec![tinytree_cloud::ManifestId::of(&next)];
    let captured_manifest = tinytree_cloud::encode_manifest_v3(
        &tinytree_cloud::ManifestV3 {
            history: captured_history,
            capture: capture_descriptor,
        },
        Default::default(),
    )
    .unwrap();
    let indexed_bytes = next.clone();
    let second = c::PublishVersionRequest {
        expected_generation: 1,
        idempotency_key: "second-cloud-capture".into(),
        manifest_id: tinytree_cloud::ManifestId::of(&next).0.to_vec(),
        manifest: next,
        staging_leases: index_leases,
        ..first.clone()
    };
    client
        .publish_version(request(second.clone(), ALPHA))
        .await
        .unwrap();
    let mut stale = second.clone();
    stale.idempotency_key = "stale-cloud-capture".into();
    assert_eq!(
        client
            .publish_version(request(stale, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    let mut changed = first.clone();
    changed.manifest.push(0);
    assert_eq!(
        client
            .publish_version(request(changed, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let retained = client
        .get_version(request(
            c::GetVersionRequest {
                repository_id: "alpha".into(),
                workspace_id: manifest.workspace.to_vec(),
                generation: 2,
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    let authority = retained
        .native_authority
        .clone()
        .expect("indexed version native authority");
    assert_eq!(authority.version, 1);
    assert_eq!(
        authority
            .manifest_selected
            .as_ref()
            .unwrap()
            .blake3
            .as_slice(),
        blake3::hash(&retained.manifest).as_bytes()
    );
    let tree_key = ObjectKey {
        kind: Kind::Tree,
        id: ObjectId(manifest.working_copy.tree.terms[0].0),
    };
    let tree_object = data
        .objects()
        .iter()
        .find(|object| object.descriptor.key == tree_key)
        .unwrap();
    let Object::Tree(native_tree) =
        tinytree_model::decode(&tree_object.bytes, Limits::default()).unwrap()
    else {
        panic!("fixture tree");
    };
    let file_key = Object::Tree(native_tree)
        .references()
        .into_iter()
        .find(|key| key.kind == Kind::File)
        .unwrap();
    let native_request = c::ReadNativeClosureRequest {
        repository_id: "alpha".into(),
        selector: Some(c::RetainedVersionSelector {
            workspace_id: retained.workspace_id.clone(),
            generation: retained.generation,
            manifest_id: retained.manifest_id.clone(),
        }),
        authority: Some(authority.clone()),
        requested: Some(wire_key(file_key)),
        proof: Some(c::NativeClosureProof {
            terminal: Some(c::native_closure_proof::Terminal::VersionRoot(wire_key(
                root(&data),
            ))),
            edge_path: vec![wire_key(tree_key), wire_key(file_key)],
        }),
    };
    for (requested, path) in [
        (root(&data), vec![]),
        (tree_key, vec![wire_key(tree_key)]),
        (file_key, vec![wire_key(tree_key), wire_key(file_key)]),
    ] {
        let mut input = native_request.clone();
        input.requested = Some(wire_key(requested));
        input.proof.as_mut().unwrap().edge_path = path;
        let response = client
            .read_native_closure(request(input, ALPHA))
            .await
            .unwrap();
        assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
        let mut stream = response.into_inner();
        let mut bytes = Vec::new();
        let mut eof = false;
        while let Some(chunk) = stream.message().await.unwrap() {
            assert_eq!(chunk.source_binding_digest, authority.binding_digest);
            assert_eq!(chunk.offset, bytes.len() as u64);
            assert_eq!(
                chunk.descriptor.as_ref().unwrap().key,
                Some(wire_key(requested))
            );
            bytes.extend(chunk.data);
            eof = chunk.eof;
        }
        assert!(eof);
        assert_eq!(blake3::hash(&bytes).as_bytes(), &requested.id.0);
        assert_eq!(
            bytes,
            data.objects()
                .iter()
                .find(|object| object.descriptor.key == requested)
                .unwrap()
                .bytes
        );
    }
    assert_eq!(
        client
            .read_native_closure(request(native_request.clone(), PEER))
            .await
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let mut bad = native_request.clone();
    bad.authority = None;
    assert_eq!(
        client
            .read_native_closure(request(bad, ALPHA))
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let mut bad = native_request.clone();
    bad.selector.as_mut().unwrap().generation += 1;
    assert_eq!(
        client
            .read_native_closure(request(bad, ALPHA))
            .await
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let mut bad = native_request.clone();
    bad.proof.as_mut().unwrap().edge_path = vec![wire_key(file_key)];
    assert_eq!(
        client
            .read_native_closure(request(bad, ALPHA))
            .await
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    let mut bad = native_request.clone();
    bad.proof.as_mut().unwrap().terminal = Some(c::native_closure_proof::Terminal::VersionRoot(
        wire_key(file_key),
    ));
    bad.proof.as_mut().unwrap().edge_path.clear();
    assert_eq!(
        client
            .read_native_closure(request(bad, ALPHA))
            .await
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    for terminal in [root(&data), frozen[1].descriptor.key] {
        let mut snapshot = native_request.clone();
        snapshot.proof.as_mut().unwrap().terminal = Some(
            c::native_closure_proof::Terminal::SnapshotCommit(c::SnapshotCommitTerminal {
                index_root_ids: vec![selected_index.root.id.0.to_vec()],
                commit_id: terminal.id.0.to_vec(),
            }),
        );
        let response = client
            .read_native_closure(request(snapshot, ALPHA))
            .await
            .unwrap();
        assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
        let mut stream = response.into_inner();
        let mut bytes = Vec::new();
        let mut eof = false;
        while let Some(chunk) = stream.message().await.unwrap() {
            assert_eq!(chunk.source_binding_digest, authority.binding_digest);
            assert_eq!(chunk.offset, bytes.len() as u64);
            assert_eq!(
                chunk.descriptor.as_ref().unwrap().key,
                Some(wire_key(file_key))
            );
            bytes.extend(chunk.data);
            eof = chunk.eof;
        }
        assert!(eof);
        assert_eq!(blake3::hash(&bytes).as_bytes(), &file_key.id.0);
    }
    let mut outside = native_request.clone();
    outside.proof.as_mut().unwrap().terminal = Some(
        c::native_closure_proof::Terminal::SnapshotCommit(c::SnapshotCommitTerminal {
            index_root_ids: vec![intrinsic.root.id.0.to_vec()],
            commit_id: frozen[1].descriptor.key.id.0.to_vec(),
        }),
    );
    assert_eq!(
        client
            .read_native_closure(request(outside, ALPHA))
            .await
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument,
        "another retained snapshot must not grant this selected snapshot's private draft"
    );
    let selected_ids = vec![selected_index.root.id.0];
    let source = q::CloudSnapshotSource {
        selector: native_request.selector.clone(),
        authority: native_request.authority.clone(),
        index_root_ids: selected_ids.iter().map(|id| id.to_vec()).collect(),
    };
    let source_frames = |key: u8| {
        vec![
            q::OpenScopeFrame {
                frame: Some(q::open_scope_frame::Frame::Header(q::ScopeHeader {
                    version: 2,
                    repository_id: "alpha".into(),
                    replay_key: vec![key; 32],
                    roots: vec![],
                    overlay_object_count: 0,
                    overlay_bytes: 0,
                    cloud_source: Some(source.clone()),
                })),
            },
            q::OpenScopeFrame {
                frame: Some(q::open_scope_frame::Frame::ScopeEnd(Default::default())),
            },
        ]
    };
    assert_eq!(
        query
            .open_scope(request(tokio_stream::iter(source_frames(91)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        query
            .open_cloud_scope(request(tokio_stream::iter(source_frames(91)), PEER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut intrinsic_frames = source_frames(89);
    if let Some(q::open_scope_frame::Frame::Header(header)) = &mut intrinsic_frames[0].frame {
        header.cloud_source.as_mut().unwrap().index_root_ids = vec![intrinsic.root.id.0.to_vec()];
    }
    let intrinsic_scope = query
        .open_cloud_scope(request(tokio_stream::iter(intrinsic_frames), ALPHA))
        .await
        .unwrap()
        .into_inner();
    let intrinsic_facts = query
        .query_facts(request(
            q::QueryFactsRequest {
                scope: intrinsic_scope.key.clone(),
                facts: vec![q::GraphFact {
                    fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                        commit: Some(query_id(frozen[1].descriptor.key)),
                    })),
                }],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        intrinsic_facts.answers[0].answer,
        Some(q::graph_fact_answer::Answer::HasId(false)),
        "other snapshot's frozen drafts must not leak into intrinsic-only selection"
    );
    assert_eq!(
        query
            .blame(request(
                q::BlameRequest {
                    scope: intrinsic_scope.key.clone(),
                    revision: Some(query_id(frozen[1].descriptor.key)),
                    path: "BUILD.bazel".into(),
                    policy: q::HistoryPolicy::LinearFixedPathV1 as i32,
                    max_commits: 0,
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument,
        "another retained snapshot cannot authorize frozen canonical file reads",
    );
    let full_prefix = frozen[1]
        .descriptor
        .key
        .id
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let intrinsic_prefix = query
        .resolve_commit_prefix(request(
            q::ResolveCommitPrefixRequest {
                scope: intrinsic_scope.key.clone(),
                hex_prefix: full_prefix.clone(),
                selection: None,
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        intrinsic_prefix.result,
        Some(q::commit_prefix_result::Result::NoMatch(_))
    ));
    query
        .close_scope(request(
            q::CloseScopeRequest {
                scope: intrinsic_scope.key,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    // Cold warming is independently bounded; retry the same exact source/key.
    let scope = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match query
                .open_cloud_scope(request(tokio_stream::iter(source_frames(91)), ALPHA))
                .await
            {
                Ok(response) => break response.into_inner(),
                Err(status)
                    if matches!(status.code(), Code::Unavailable | Code::ResourceExhausted) =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(status) => panic!("cloud scope: {status}"),
            }
        }
    })
    .await
    .unwrap();
    let epoch: [u8; 16] = scope.index_epoch.as_slice().try_into().unwrap();
    let binding: [u8; 32] = authority.binding_digest.as_slice().try_into().unwrap();
    assert_eq!(
        scope.cloud_source_binding,
        tinytree_model::cloud_query_source_binding(
            binding,
            &selected_ids,
            &[CommitId(root(&data).id.0)],
            epoch
        )
    );
    assert_eq!(
        query
            .open_cloud_scope(request(tokio_stream::iter(source_frames(91)), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        scope
    );
    // Frozen cloud drafts are not in the live index; their selected retained
    // source, not COMMIT-only overlay membership, authorizes canonical path I/O.
    let frozen_search = search_ready(
        &mut query,
        &search_input(&scope, frozen[1].descriptor.key, b"cc_binary", 1),
    )
    .await;
    assert_eq!(frozen_search.coverage, q::SearchCoverage::SearchMore as i32);
    assert_eq!(frozen_search.hits[0].path, "BUILD.bazel");
    let frozen_blame_response = query
        .blame(request(
            q::BlameRequest {
                scope: scope.key.clone(),
                revision: Some(query_id(frozen[1].descriptor.key)),
                path: "BUILD.bazel".into(),
                policy: q::HistoryPolicy::LinearFixedPathV1 as i32,
                max_commits: 0,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    assert_eq!(
        frozen_blame_response
            .metadata()
            .get("grpc-encoding")
            .unwrap(),
        "zstd"
    );
    let frozen_blame = frozen_blame_response.into_inner();
    assert_eq!(
        frozen_blame.coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(
        frozen_blame.revision,
        Some(query_id(frozen[1].descriptor.key))
    );
    assert!(!frozen_blame.target_bytes.is_empty());
    assert!(
        frozen_blame
            .ranges
            .iter()
            .all(|range| range.origin == Some(query_id(root(&data))))
    );
    let frozen_history = query
        .path_history(request(
            q::PathHistoryRequest {
                scope: scope.key.clone(),
                revision: Some(query_id(frozen[1].descriptor.key)),
                path: "BUILD.bazel".into(),
                policy: q::HistoryPolicy::LinearFixedPathV1 as i32,
                max_commits: 0,
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        frozen_history.coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(frozen_history.entries.len(), 1);
    assert_eq!(
        frozen_history.entries[0].commit,
        Some(query_id(root(&data)))
    );
    let selected_prefix = query
        .resolve_commit_prefix(request(
            q::ResolveCommitPrefixRequest {
                scope: scope.key.clone(),
                hex_prefix: full_prefix,
                selection: None,
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        matches!(selected_prefix.result, Some(q::commit_prefix_result::Result::Single(q::CommitMatch { commit:Some(ref id) })) if id==&query_id(frozen[1].descriptor.key))
    );
    for object in &frozen {
        assert!(
            index
                .node("alpha", CommitId(object.descriptor.key.id.0))
                .unwrap()
                .is_none(),
            "frozen private baseline is not published into the shared derived index"
        );
    }
    // Force global index position to disagree with portable order. The captured
    // frozen lane must still use parent-first (original order, ID) ranks.
    for object in frozen.iter().rev() {
        index
            .ingest("alpha", object.descriptor, &object.bytes)
            .unwrap();
    }
    let latest = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Latest(
            q::LatestExpr {
                candidates: Some(Box::new(q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                        commits: frozen
                            .iter()
                            .map(|object| query_id(object.descriptor.key))
                            .collect(),
                    })),
                })),
                count: 1,
            }
            .into(),
        )),
    };
    let facts = query
        .query_facts(request(
            q::QueryFactsRequest {
                scope: scope.key.clone(),
                facts: frozen
                    .iter()
                    .map(|object| q::GraphFact {
                        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                            commit: Some(query_id(object.descriptor.key)),
                        })),
                    })
                    .chain(frozen.iter().map(|object| q::GraphFact {
                        fact: Some(q::graph_fact::Fact::Membership(q::Membership {
                            expression: Some(latest.clone()),
                            candidate: Some(query_id(object.descriptor.key)),
                        })),
                    }))
                    .collect(),
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        facts
            .answers
            .iter()
            .map(|answer| answer.answer.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(q::graph_fact_answer::Answer::HasId(true)),
            Some(q::graph_fact_answer::Answer::HasId(true)),
            Some(q::graph_fact_answer::Answer::Membership(false)),
            Some(q::graph_fact_answer::Answer::Membership(true))
        ]
    );
    let scope_key = scope.key.clone().unwrap();
    let nonce = [95u8; 16];
    let mut query_identity = nonce.to_vec();
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-query-scope-id-v2\0");
    hash.update(&(scope_key.repository_id.len() as u64).to_be_bytes());
    hash.update(scope_key.repository_id.as_bytes());
    hash.update(&scope_key.handle);
    hash.update(&nonce);
    query_identity.extend_from_slice(&hash.finalize().as_bytes()[..16]);
    let page = query
        .evaluate(request(
            q::EvaluateRequest {
                scope: Some(scope_key.clone()),
                query_id: query_identity.clone(),
                expression: Some(latest.clone()),
                projection: q::Projection::Canonical as i32,
                page_size: 8,
                cursor: vec![],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        page.rows[0].commit,
        Some(query_id(frozen[1].descriptor.key))
    );
    assert_eq!(
        page.rows[0].canonical.as_ref().unwrap().canonical_envelope,
        frozen[1].bytes,
        "frozen canonical payload is read on demand under the selected version guard"
    );
    query
        .close_query(request(
            q::CloseQueryRequest {
                scope: Some(scope_key),
                query_id: query_identity,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    // The single cloud call permit must not remain in the idle scope.
    client
        .get_workspace(request(
            c::GetWorkspaceRequest {
                repository_id: "alpha".into(),
                workspace_id: manifest.workspace.to_vec(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    let mut child_model = frozen_model.clone();
    child_model.parents = vec![CommitId(frozen[1].descriptor.key.id.0)];
    child_model.change_id = tinytree_model::ChangeId([83; 16]);
    child_model.description = "private child of selected frozen draft".into();
    let child = tinytree_model::encode(&Object::Commit(child_model), Limits::default()).unwrap();
    let mut child_frames = private_query_frames(&[], 92, &[&child]);
    if let Some(q::open_scope_frame::Frame::Header(header)) = &mut child_frames[0].frame {
        header.version = 2;
    }
    let fork_frames = private_fork_frames(scope.key.clone().unwrap(), child_frames);
    let fork = query
        .fork_scope(request(tokio_stream::iter(fork_frames.clone()), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fork.cloud_source_binding, scope.cloud_source_binding);
    for (key, expected) in [(scope.key.clone(), false), (fork.key.clone(), true)] {
        let facts = query
            .query_facts(request(
                q::QueryFactsRequest {
                    scope: key,
                    facts: vec![q::GraphFact {
                        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                            commit: Some(query_id(child.descriptor.key)),
                        })),
                    }],
                },
                ALPHA,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            facts.answers[0].answer,
            Some(q::graph_fact_answer::Answer::HasId(expected))
        );
    }
    assert_eq!(
        store.descriptor("alpha", child.descriptor.key).unwrap(),
        None,
        "private fork child must not enter canonical Store"
    );
    assert!(
        index
            .node("alpha", CommitId(child.descriptor.key.id.0))
            .unwrap()
            .is_none(),
        "private fork child must not enter shared derived Index"
    );

    assert_eq!(fork.expires_at_unix_seconds, scope.expires_at_unix_seconds);
    assert_eq!(
        query
            .open_cloud_scope(request(tokio_stream::iter(source_frames(93)), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted,
        "an independent version guard must fit the shared retained scope pool"
    );
    let mut mixed = fork_frames.clone();
    if let Some(q::fork_scope_frame::Frame::Header(header)) = &mut mixed[0].frame {
        let header = header.scope.as_mut().unwrap();
        header.replay_key = vec![94; 32];
        header.roots = vec![q::CommitId {
            value: root(&data).id.0.to_vec(),
        }];
    }
    assert_eq!(
        query
            .fork_scope(request(tokio_stream::iter(mixed), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    query
        .close_scope(request(
            q::CloseScopeRequest {
                scope: scope.key.clone(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    assert_eq!(
        query
            .fork_scope(request(tokio_stream::iter(fork_frames.clone()), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        fork,
        "cloud replay must retain its exact source after parent close"
    );
    let facts = query
        .query_facts(request(
            q::QueryFactsRequest {
                scope: fork.key.clone(),
                facts: vec![q::GraphFact {
                    fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                        commit: Some(q::CommitId {
                            value: root(&data).id.0.to_vec(),
                        }),
                    })),
                }],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        facts.answers[0].answer,
        Some(q::graph_fact_answer::Answer::HasId(true))
    );
    query
        .close_scope(request(q::CloseScopeRequest { scope: fork.key }, ALPHA))
        .await
        .unwrap();
    let recovered = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match query
                .open_cloud_scope(request(tokio_stream::iter(source_frames(93)), ALPHA))
                .await
            {
                Ok(response) => break response.into_inner(),
                Err(status) if status.code() == Code::ResourceExhausted => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(status) => panic!("cloud scope cleanup: {status}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(recovered.cloud_source_binding, scope.cloud_source_binding);
    let recovered_latest = query
        .query_facts(request(
            q::QueryFactsRequest {
                scope: recovered.key.clone(),
                facts: vec![q::GraphFact {
                    fact: Some(q::graph_fact::Fact::Membership(q::Membership {
                        expression: Some(latest),
                        candidate: Some(query_id(frozen[1].descriptor.key)),
                    })),
                }],
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        recovered_latest.answers[0].answer,
        Some(q::graph_fact_answer::Answer::Membership(true)),
        "fresh cloud scope must ignore reversed shared-index positions for frozen Latest ties"
    );

    query
        .close_scope(request(
            q::CloseScopeRequest {
                scope: recovered.key,
            },
            ALPHA,
        ))
        .await
        .unwrap();
    drop(query);
    let held = store.clone();
    tokio::task::spawn_blocking(move || held.gc("alpha", i64::MAX).unwrap())
        .await
        .unwrap();
    let version = c::GetVersionRequest {
        repository_id: "alpha".into(),
        workspace_id: manifest.workspace.to_vec(),
        generation: 1,
    };
    assert_eq!(
        client
            .get_version(request(version.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner()
            .manifest,
        bytes
    );
    assert!(
        client
            .get_version(request(version.clone(), PEER))
            .await
            .is_err()
    );
    assert_eq!(
        client
            .get_version(request(
                c::GetVersionRequest {
                    generation: 2,
                    ..version.clone()
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner()
            .manifest,
        indexed_bytes
    );
    let captured_request = c::PublishVersionRequest {
        repository_id: "alpha".into(),
        workspace_id: manifest.workspace.to_vec(),
        expected_generation: 2,
        idempotency_key: "captured-native-state".into(),
        manifest_id: tinytree_cloud::ManifestId::of(&captured_manifest)
            .0
            .to_vec(),
        manifest: captured_manifest.clone(),
        staging_leases: vec![],
        capture_data: capture_bytes.clone(),
    };
    assert_eq!(
        client
            .publish_version(request(captured_request.clone(), READER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let captured_publication = client
        .publish_version(request(captured_request.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        captured_publication.workspace.as_ref().unwrap().generation,
        3
    );
    assert_eq!(
        client
            .publish_version(request(captured_request.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner(),
        captured_publication
    );
    let mut changed_capture = captured_request.clone();
    changed_capture.capture_data[0] ^= 1;
    assert_eq!(
        client
            .publish_version(request(changed_capture, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let captured_read = c::ReadCaptureRequest {
        repository_id: "alpha".into(),
        selector: Some(c::RetainedVersionSelector {
            workspace_id: manifest.workspace.to_vec(),
            generation: 3,
            manifest_id: captured_request.manifest_id.clone(),
        }),
        expected_capture_id: capture_descriptor.id.0.to_vec(),
        manifest_selected: Some(c::RawDigest {
            blake3: blake3::hash(&captured_manifest).as_bytes().to_vec(),
            size_bytes: captured_manifest.len() as u64,
        }),
    };
    assert!(
        client
            .read_capture(request(captured_read.clone(), PEER))
            .await
            .is_err()
    );
    let held = store.clone();
    tokio::task::spawn_blocking(move || held.gc("alpha", i64::MAX).unwrap())
        .await
        .unwrap();
    let response = client
        .read_capture(request(captured_read.clone(), ALPHA))
        .await
        .unwrap();
    assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
    let mut capture_stream = response.into_inner();
    let mut transferred_capture = Vec::new();
    let mut capture_eof = false;
    while let Some(chunk) = capture_stream.message().await.unwrap() {
        assert!(!capture_eof);
        assert_eq!(chunk.offset, transferred_capture.len() as u64);
        assert_eq!(chunk.manifest_id, captured_request.manifest_id);
        let descriptor = chunk.descriptor.unwrap();
        assert_eq!(descriptor.capture_id, capture_descriptor.id.0);
        assert_eq!(
            descriptor.selected.unwrap().blake3,
            capture_descriptor.selected.blake3
        );
        transferred_capture.extend(chunk.data);
        capture_eof = chunk.eof;
    }
    assert!(capture_eof);
    assert_eq!(transferred_capture, capture_bytes);
    drop(client);
    server.stop().await;
    drop(store);
    let store = Store::open_existing(directory.path(), store_config).unwrap();
    let server = Running::start_query(store, profile.service, index).await;
    let channel = Endpoint::from_shared(format!("http://{}", server.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WorkspaceCloudClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    let mut capture_stream = client
        .read_capture(request(captured_read, ALPHA))
        .await
        .unwrap()
        .into_inner();
    let mut reopened_capture = Vec::new();
    while let Some(chunk) = capture_stream.message().await.unwrap() {
        assert_eq!(chunk.offset, reopened_capture.len() as u64);
        reopened_capture.extend(chunk.data);
    }
    assert_eq!(
        reopened_capture, capture_bytes,
        "capture and version survive reopening together"
    );
    assert_eq!(
        client
            .get_version(request(
                c::GetVersionRequest {
                    generation: 2,
                    ..version.clone()
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner()
            .manifest,
        indexed_bytes
    );
    assert_eq!(
        client
            .get_version(request(version, ALPHA))
            .await
            .unwrap()
            .into_inner()
            .manifest,
        bytes
    );
    let mut restored = client
        .read_native_closure(request(native_request, ALPHA))
        .await
        .unwrap()
        .into_inner();
    let mut restored_bytes = Vec::new();
    while let Some(chunk) = restored.message().await.unwrap() {
        restored_bytes.extend(chunk.data);
    }
    assert_eq!(blake3::hash(&restored_bytes).as_bytes(), &file_key.id.0);
    assert_eq!(
        client
            .get_publication(request(
                c::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: first.idempotency_key.clone()
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner()
            .publication,
        Some(published)
    );
    assert_eq!(
        client
            .publish_version(request(first, ALPHA))
            .await
            .unwrap()
            .into_inner()
            .workspace
            .unwrap()
            .generation,
        1
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn cloud_rpc_decompressed_and_semantic_request_bounds_reject_before_store_publication() {
    use tinytree_proto::cloud_v1::{self as c, workspace_cloud_client::WorkspaceCloudClient};
    let directory = tempfile::tempdir().unwrap();
    let mut profile: tinytree_server::Config =
        toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
    profile.service.cloud.max_object_bytes = 1024;
    profile.service.cloud.max_manifest_bytes = 1024;
    profile.service.cloud.max_staging_leases = 1;
    let store = Store::open(
        directory.path(),
        profile
            .store
            .materialize(profile.service.max_metadata_bytes),
    )
    .unwrap();
    store.create_repository("alpha").unwrap();
    store.create_repository("beta").unwrap();
    let server = Running::start(store, profile.service).await;
    let channel = Endpoint::from_shared(format!("http://{}", server.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WorkspaceCloudClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    // Highly compressible wire fits the raw-frame bound but must not bypass the
    // independent decompressed message cap before Prost constructs the payload.
    let error = client
        .put_object(request(
            c::PutObjectRequest {
                repository_id: "alpha".into(),
                descriptor: None,
                payload: vec![0; 32 * 1024],
            },
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error.code(),
        Code::ResourceExhausted | Code::OutOfRange
    ));
    let error = client
        .lookup_objects(request(
            c::LookupObjectsRequest {
                repository_id: "alpha".into(),
                ids: vec![
                    c::ObjectId {
                        kind: 1,
                        semantic_id: vec![1; 64]
                    };
                    257
                ],
            },
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    let error = client
        .publish_version(request(
            c::PublishVersionRequest {
                repository_id: "alpha".into(),
                workspace_id: vec![1; 32],
                expected_generation: 0,
                idempotency_key: "oversized-manifest".into(),
                manifest_id: vec![2; 32],
                manifest: vec![0; 1025],
                staging_leases: vec![],
                capture_data: vec![],
            },
            ALPHA,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        client
            .get_workspace(request(
                c::GetWorkspaceRequest {
                    repository_id: "alpha".into(),
                    workspace_id: vec![1; 32]
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner()
            .generation,
        0
    );
    assert!(
        client
            .get_publication(request(
                c::GetPublicationRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "oversized-manifest".into()
                },
                ALPHA
            ))
            .await
            .unwrap()
            .into_inner()
            .publication
            .is_none()
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn standalone_cloud_profile_registers_private_workspace_service() {
    use tinytree_proto::cloud_v1::{self as c, workspace_cloud_client::WorkspaceCloudClient};
    let directory = tempfile::tempdir().unwrap();
    let mut profile: tinytree_server::Config =
        toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
    profile.storage = directory.path().join("standalone-cloud");
    profile.principals.clear(); // Explicit loopback local-owner mode.
    profile.gc_interval_seconds = 0;
    let server = Running::start_production(profile).await;
    let channel = Endpoint::from_shared(format!("http://{}", server.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = WorkspaceCloudClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    let workspace = client
        .get_workspace(c::GetWorkspaceRequest {
            repository_id: "example".into(),
            workspace_id: vec![5; 32],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(workspace.generation, 0);
    assert!(workspace.manifest_id.is_none());
    drop(client);
    server.stop().await;
}

fn history_file(store: &Store, bytes: &[u8], executable: bool) -> tinytree_model::TreeValue {
    let descriptor = tinytree_model::describe_file(bytes);
    store.put("alpha", descriptor, bytes, 0).unwrap();
    tinytree_model::TreeValue::File {
        id: tinytree_model::FileId(descriptor.key.id.0),
        executable,
        size_bytes: bytes.len() as u64,
        copy_id: None,
    }
}
fn history_commit(
    store: &Store,
    index: &tinytree_index::Index,
    parents: &[ObjectKey],
    value: Option<tinytree_model::TreeValue>,
    change: u8,
) -> tinytree_model::EncodedObject {
    let tree = tinytree_model::encode(
        &Object::Tree(tinytree_model::Tree {
            entries: value
                .into_iter()
                .map(|value| tinytree_model::TreeEntry {
                    name: "file".into(),
                    value,
                })
                .collect(),
        }),
        Limits::default(),
    )
    .unwrap();
    store.put("alpha", tree.descriptor, &tree.bytes, 0).unwrap();
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = if parents.is_empty() {
        vec![CommitId::ROOT]
    } else {
        parents.iter().map(|parent| CommitId(parent.id.0)).collect()
    };
    commit.change_id = tinytree_model::ChangeId([change; 16]);
    commit.description = format!("history {change}");
    commit.root_tree = tinytree_model::TreeMerge {
        terms: vec![TreeId(tree.descriptor.key.id.0)],
        labels: vec![String::new()],
    };
    let object = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", object.descriptor, &object.bytes, 0)
        .unwrap();
    index
        .ingest("alpha", object.descriptor, &object.bytes)
        .unwrap();
    object
}
fn history_request(
    scope: &q::QueryScope,
    revision: ObjectKey,
    maximum: u32,
) -> q::PathHistoryRequest {
    q::PathHistoryRequest {
        scope: scope.key.clone(),
        revision: Some(query_id(revision)),
        path: "file".into(),
        policy: q::HistoryPolicy::LinearFixedPathV1 as i32,
        max_commits: maximum,
    }
}
fn blame_request(scope: &q::QueryScope, revision: ObjectKey, maximum: u32) -> q::BlameRequest {
    let input = history_request(scope, revision, maximum);
    q::BlameRequest {
        scope: input.scope,
        revision: input.revision,
        path: input.path,
        policy: input.policy,
        max_commits: input.max_commits,
    }
}
fn expanded_origins(result: &q::BlameResult) -> Vec<Option<(Vec<u8>, u32)>> {
    result
        .ranges
        .iter()
        .flat_map(|range| {
            (range.start_line..range.end_line).map(move |line| {
                range.origin.as_ref().map(|origin| {
                    (
                        origin.value.clone(),
                        range.original_line + line - range.start_line,
                    )
                })
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn path_history_and_blame_pin_revision_and_distinguish_partial_attribution() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let a = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(&store, b"old\nshared\n", false)),
        1,
    )
    .descriptor
    .key;
    let b = history_commit(
        &store,
        &index,
        &[a],
        Some(history_file(&store, b"new\nshared\n", false)),
        2,
    )
    .descriptor
    .key;
    let mode = history_commit(
        &store,
        &index,
        &[b],
        Some(history_file(&store, b"new\nshared\n", true)),
        3,
    )
    .descriptor
    .key;
    let deleted = history_commit(&store, &index, &[mode], None, 4)
        .descriptor
        .key;
    let readded = history_commit(
        &store,
        &index,
        &[deleted],
        Some(history_file(&store, b"old\nshared\n", false)),
        5,
    )
    .descriptor
    .key;
    let symlink =
        tinytree_model::encode(&Object::Symlink("elsewhere".into()), Limits::default()).unwrap();
    store
        .put("alpha", symlink.descriptor, &symlink.bytes, 0)
        .unwrap();
    let link = history_commit(
        &store,
        &index,
        &[readded],
        Some(tinytree_model::TreeValue::Symlink {
            id: tinytree_model::SymlinkId(symlink.descriptor.key.id.0),
            target_bytes: 9,
        }),
        6,
    )
    .descriptor
    .key;
    let binary = history_commit(
        &store,
        &index,
        &[link],
        Some(history_file(&store, b"tail\xff\0", false)),
        7,
    )
    .descriptor
    .key;
    let server = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[binary], 81).await;
    let history = client
        .path_history(request(history_request(&scope, mode, 0), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(history.coverage, q::HistoryCoverage::HistoryComplete as i32);
    assert!(history.boundary.is_none());
    assert_eq!(history.scanned_commits, 3);
    assert_eq!(
        history
            .entries
            .iter()
            .map(|entry| entry.commit.as_ref().unwrap().value.clone())
            .collect::<Vec<_>>(),
        vec![mode.id.0.to_vec(), b.id.0.to_vec(), a.id.0.to_vec()]
    );
    assert_eq!(history.scope_digest, scope.content_digest);
    assert_eq!(history.index_epoch, scope.index_epoch);
    assert_eq!(history.revision, Some(query_id(mode)));
    let limited = client
        .path_history(request(history_request(&scope, mode, 1), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        limited.coverage,
        q::HistoryCoverage::HistoryScanLimit as i32
    );
    assert_eq!(limited.scanned_commits, 1);
    assert_eq!(limited.boundary, Some(query_id(b)));
    let blame = client
        .blame(request(blame_request(&scope, b, 0), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(blame.coverage, q::HistoryCoverage::HistoryComplete as i32);
    assert_eq!(blame.target_bytes, b"new\nshared\n");
    assert_eq!(
        blame.target.as_ref().unwrap().content_blake3,
        blake3::hash(&blame.target_bytes).as_bytes()
    );
    assert_eq!(
        expanded_origins(&blame),
        vec![Some((b.id.0.to_vec(), 0)), Some((a.id.0.to_vec(), 1))]
    );
    let limited = client
        .blame(request(blame_request(&scope, b, 1), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        limited.coverage,
        q::HistoryCoverage::HistoryScanLimit as i32
    );
    assert_eq!(limited.boundary, Some(query_id(a)));
    assert_eq!(
        expanded_origins(&limited),
        vec![Some((b.id.0.to_vec(), 0)), None]
    );
    let readded_blame = client
        .blame(request(blame_request(&scope, readded, 0), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        readded_blame.coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(
        expanded_origins(&readded_blame),
        vec![
            Some((readded.id.0.to_vec(), 0)),
            Some((readded.id.0.to_vec(), 1))
        ],
        "identical bytes before deletion cannot cross the missing-file boundary"
    );
    let binary_blame = client
        .blame(request(blame_request(&scope, binary, 0), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        binary_blame.coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(binary_blame.target_bytes, b"tail\xff\0");
    assert_eq!(
        expanded_origins(&binary_blame),
        vec![Some((binary.id.0.to_vec(), 0))]
    );
    for (revision, code) in [(deleted, Code::NotFound), (link, Code::FailedPrecondition)] {
        assert_eq!(
            client
                .blame(request(blame_request(&scope, revision, 0), ALPHA))
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    for path in [
        "",
        "/file",
        "file/",
        "a//b",
        "a/../file",
        "./file",
        "a\\file",
        "file\0",
    ] {
        let mut input = history_request(&scope, mode, 0);
        input.path = path.into();
        assert_eq!(
            client
                .path_history(request(input, ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument,
            "{path:?}"
        );
    }
    client
        .close_scope(request(
            q::CloseScopeRequest {
                scope: scope.key.clone(),
            },
            ALPHA,
        ))
        .await
        .unwrap();
    assert_eq!(
        client
            .blame(request(blame_request(&scope, b, 0), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_canonical_source_excludes_private_metadata_only_overlays() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let base = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(&store, b"base\n", false)),
        11,
    )
    .descriptor
    .key;
    let secret = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(&store, b"not in selected ancestry\n", false)),
        12,
    );
    // Even though its trees/files happen to exist, this metadata-only overlay
    // lacks selected canonical source authority. Repo-wide readers still cannot
    // make a graph-only scope silently read that unselected FILE closure.
    let Object::Commit(mut draft) =
        tinytree_model::decode(&secret.bytes, Limits::default()).unwrap()
    else {
        unreachable!()
    };
    draft.parents = vec![CommitId(base.id.0)];
    draft.change_id = tinytree_model::ChangeId([13; 16]);
    let draft = tinytree_model::encode(&Object::Commit(draft), Limits::default()).unwrap();
    let server = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[base], 82).await;
    assert_eq!(
        client
            .blame(request(
                blame_request(&scope, secret.descriptor.key, 0),
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let overlay = client
        .open_scope(request(
            tokio_stream::iter(private_query_frames(&[base], 83, &[&draft])),
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner();
    for key in [scope.key.clone(), overlay.key.clone()] {
        let mut input = blame_request(&overlay, draft.descriptor.key, 0);
        input.scope = key;
        if input.scope == scope.key {
            assert_eq!(
                client
                    .blame(request(input, ALPHA))
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        } else {
            let response = client
                .blame(request(input, ALPHA))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response.coverage,
                q::HistoryCoverage::HistoryPrivateOverlay as i32
            );
            assert!(response.target.is_none());
            let search = client
                .search(request(
                    search_input(&overlay, draft.descriptor.key, b"selected", 1),
                    ALPHA,
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                search.coverage,
                q::SearchCoverage::SearchPrivateOverlay as i32
            );
            assert!(search.root_tree.is_empty());
            assert!(search.hits.is_empty());
            assert!(response.target_bytes.is_empty());
            assert!(response.ranges.is_empty());
            let response = client
                .path_history(request(
                    history_request(&overlay, draft.descriptor.key, 0),
                    ALPHA,
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response.coverage,
                q::HistoryCoverage::HistoryPrivateOverlay as i32
            );
            assert!(response.entries.is_empty());
        }
    }
    assert_eq!(
        client
            .blame(request(blame_request(&overlay, base, 0), BETA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .blame(request(blame_request(&overlay, base, 0), PEER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_limits_and_unsupported_graphs_never_claim_complete_blame() {
    for (name, configure, expected, has_target) in [
        (
            "diff work",
            (|config: &mut ServiceConfig| config.query.max_blame_diff_steps = 1)
                as fn(&mut ServiceConfig),
            q::HistoryCoverage::HistoryDiffLimit,
            true,
        ),
        (
            "file bytes",
            |config: &mut ServiceConfig| config.query.max_blame_file_bytes = 1,
            q::HistoryCoverage::HistoryReadLimit,
            false,
        ),
        (
            "selected lines",
            |config: &mut ServiceConfig| config.query.max_blame_lines = 1,
            q::HistoryCoverage::HistoryDiffLimit,
            false,
        ),
        (
            "commit scan",
            |config: &mut ServiceConfig| config.query.max_history_commits = 1,
            q::HistoryCoverage::HistoryScanLimit,
            true,
        ),
    ] {
        let (temporary, store) = setup();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        let a = history_commit(
            &store,
            &index,
            &[],
            Some(history_file(&store, b"old\nshared\n", false)),
            21,
        )
        .descriptor
        .key;
        let b = history_commit(
            &store,
            &index,
            &[a],
            Some(history_file(&store, b"new\nshared\n", false)),
            22,
        )
        .descriptor
        .key;
        let mut config = ServiceConfig::default();
        configure(&mut config);
        let server = Running::start_query(store, config, index).await;
        let mut client = server.query_client().await;
        let scope = query_ready(&mut client, &[b], 84).await;
        let result = client
            .blame(request(blame_request(&scope, b, u32::MAX), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.coverage, expected as i32, "{name}");
        assert!(result.boundary.is_some(), "{name}");
        assert_eq!(result.target.is_some(), has_target, "{name}");
        if has_target {
            assert!(
                result.ranges.iter().any(|range| range.origin.is_none()),
                "{name}: unresolved lines cannot receive a guessed origin"
            );
            assert_eq!(expanded_origins(&result).len(), 2, "{name}");
        } else {
            assert!(result.target_bytes.is_empty(), "{name}");
            assert!(result.ranges.is_empty(), "{name}");
        }
        drop(client);
        server.stop().await;
    }
    // A medium file across many edits reuses physically released diff scratch,
    // while aggregate CPU work still spans all comparisons in the request.
    let (medium_directory, medium_store) = setup();
    let medium_index =
        tinytree_index::Index::open(medium_directory.path().join("derived"), &Default::default())
            .unwrap();
    let mut chain = Vec::new();
    for version in 0..16 {
        let mut bytes = format!("version {version:04}\n").into_bytes();
        for line in 1..1024 {
            bytes.extend_from_slice(format!("shared line {line:06} fixed content\n").as_bytes());
        }
        let parents = chain.last().copied().into_iter().collect::<Vec<_>>();
        chain.push(
            history_commit(
                &medium_store,
                &medium_index,
                &parents,
                Some(history_file(&medium_store, &bytes, false)),
                version + 1,
            )
            .descriptor
            .key,
        );
    }
    let head = *chain.last().unwrap();
    for (aggregate_work, expected) in [
        (Some(1_000_000), q::HistoryCoverage::HistoryDiffLimit),
        (None, q::HistoryCoverage::HistoryComplete),
    ] {
        let mut config = ServiceConfig::default();
        if let Some(work) = aggregate_work {
            config.query.max_blame_diff_steps = work;
        }
        assert_eq!(
            config.query.max_blame_diff_memory_bytes,
            8 * 1024 * 1024,
            "reuse must not require a larger scratch reservation"
        );
        let server = Running::start_query(medium_store.clone(), config, medium_index.clone()).await;
        let mut client = server.query_client().await;
        let scope = query_ready(&mut client, &[head], 86).await;
        let result = client
            .blame(request(blame_request(&scope, head, 0), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.coverage, expected as i32);
        let origins = expanded_origins(&result);
        assert_eq!(origins.len(), 1024);
        if expected == q::HistoryCoverage::HistoryComplete {
            assert_eq!(result.scanned_commits, 16);
            assert_eq!(origins[0], Some((head.id.0.to_vec(), 0)));
            for (line, origin) in origins.iter().enumerate().skip(1) {
                assert_eq!(*origin, Some((chain[0].id.0.to_vec(), line as u32)));
            }
        } else {
            assert!(result.scanned_commits > 0 && result.scanned_commits < 16);
            assert!(
                origins.iter().any(Option::is_none),
                "aggregate work must not reset with scratch"
            );
        }
        drop(client);
        server.stop().await;
    }
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let a = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(&store, b"left\n", false)),
        31,
    )
    .descriptor
    .key;
    let b = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(&store, b"right\n", false)),
        32,
    )
    .descriptor
    .key;
    let merge = history_commit(
        &store,
        &index,
        &[a, b],
        Some(history_file(&store, b"resolved\n", false)),
        33,
    );
    let Object::Commit(mut conflict) =
        tinytree_model::decode(&merge.bytes, Limits::default()).unwrap()
    else {
        unreachable!()
    };
    let tree = conflict.root_tree.terms[0];
    conflict.parents = vec![CommitId(merge.descriptor.key.id.0)];
    conflict.change_id = tinytree_model::ChangeId([34; 16]);
    conflict.root_tree = tinytree_model::TreeMerge {
        terms: vec![tree, tree, tree],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };
    let conflict = tinytree_model::encode(&Object::Commit(conflict), Limits::default()).unwrap();
    store
        .put("alpha", conflict.descriptor, &conflict.bytes, 0)
        .unwrap();
    index
        .ingest("alpha", conflict.descriptor, &conflict.bytes)
        .unwrap();
    let server = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[conflict.descriptor.key], 85).await;
    for (revision, expected) in [
        (
            merge.descriptor.key,
            q::HistoryCoverage::HistoryMergeBoundary,
        ),
        (
            conflict.descriptor.key,
            q::HistoryCoverage::HistoryConflictBoundary,
        ),
    ] {
        let history = client
            .path_history(request(history_request(&scope, revision, 0), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(history.coverage, expected as i32);
        assert!(history.boundary.is_some());
        assert!(history.entries.is_empty());
        let blame = client
            .blame(request(blame_request(&scope, revision, 0), ALPHA))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(blame.coverage, expected as i32);
        assert!(blame.boundary.is_some());
        assert!(blame.ranges.iter().all(|range| range.origin.is_none()));
    }
    drop(client);
    server.stop().await;
}

// Warm canonical Store and derived index; each trial opens a fresh scope.
// This is not a shallow-clone comparison or an OS-cache-cold benchmark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual bounded default-limit history/blame RPC benchmark"]
async fn benchmark_remote_blame_default_limits() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for (lines, versions) in [(128usize,4usize),(128,16),(1024,4),(1024,16),(4096,4)] {
            let (temporary,store)=setup();
            let index=tinytree_index::Index::open(temporary.path().join("derived"),&Default::default()).unwrap();
            let mut commits=Vec::with_capacity(versions);
            let mut files=Vec::with_capacity(versions);
            for version in 0..versions {
                let mut bytes=format!("version {version:04}\n").into_bytes();
                for line in 1..lines {
                    bytes.extend_from_slice(format!("shared line {line:06} fixed content\n").as_bytes());
                }
                let file=tinytree_model::describe_file(&bytes);
                let value=history_file(&store,&bytes,false);
                let parents=commits.last().copied().into_iter().collect::<Vec<_>>();
                let commit=history_commit(&store,&index,&parents,Some(value),version as u8 + 1).descriptor.key;
                commits.push(commit); files.push((file,bytes));
            }
            let config=ServiceConfig::default();
            let max_diff_work=config.query.max_blame_diff_steps;
            let max_diff_allocation=config.query.max_blame_diff_memory_bytes;
            let max_scan=config.query.max_history_commits;
            let server=Running::start_query(store,config,index).await;
            let mut query=server.query_client().await;
            let mut objects=server.client().await;
            let first=commits[0];
            let selected=*commits.last().unwrap();
            let selected_bytes=&files.last().unwrap().1;
            let known_historical_file_bytes=files.iter().map(|(descriptor,_)|descriptor.size_bytes).sum::<u64>();
            for trial in 0..3 {
                let started=std::time::Instant::now();
                let scope=query_ready(&mut query,&[selected],90+trial).await;
                let scope_open_ms=started.elapsed().as_secs_f64()*1000.0;
                let started=std::time::Instant::now();
                let response=query.blame(request(blame_request(&scope,selected,0),ALPHA)).await.unwrap();
                assert_eq!(response.metadata().get("grpc-encoding").unwrap(),"zstd");
                let blame=response.into_inner();
                let blame_ms=started.elapsed().as_secs_f64()*1000.0;
                let coverage=q::HistoryCoverage::try_from(blame.coverage).unwrap();
                assert_eq!(blame.target_bytes,*selected_bytes);
                let line_origins=expanded_origins(&blame);
                assert_eq!(line_origins.len(),lines);
                let unresolved=line_origins.iter().filter(|origin|origin.is_none()).count();
                let complete=coverage==q::HistoryCoverage::HistoryComplete;
                if complete {
                    assert!(blame.boundary.is_none());assert_eq!(unresolved,0);
                    assert_eq!(line_origins[0],Some((selected.id.0.to_vec(),0)));
                    for (line,origin) in line_origins.iter().enumerate().skip(1) {
                        assert_eq!(*origin,Some((first.id.0.to_vec(),line as u32)));
                    }
                } else {
                    assert!(blame.boundary.is_some());assert!(unresolved>0);
                }
                let blame_protobuf_bytes=prost::Message::encoded_len(&blame);
                // Actual serial Repository.ReadFile calls for all known versions.
                // This only measures a naive transfer baseline: no client blame
                // CPU, clone/index checkout, directory traversal or WAN latency.
                let started=std::time::Instant::now();
                let mut download_file_bytes=0usize;
                let mut download_protobuf_bytes=0usize;
                for (descriptor,expected) in &files {
                    let response=objects.read_file(request(p::ReadFileRequest {
                        repository_id:"alpha".into(), native_file_id:descriptor.key.id.0.to_vec(),
                    },ALPHA)).await.unwrap();
                    assert_eq!(response.metadata().get("grpc-encoding").unwrap(),"zstd");
                    let mut stream=response.into_inner();
                    let mut received=Vec::with_capacity(expected.len());
                    let mut header=false;
                    while let Some(frame)=stream.message().await.unwrap() {
                        download_protobuf_bytes+=prost::Message::encoded_len(&frame);
                        match frame.frame.unwrap() {
                            p::file_frame::Frame::Header(value)=>{
                                assert!(!header);assert_eq!(value,crate::descriptor(*descriptor));header=true;
                            }
                            p::file_frame::Frame::Chunk(chunk)=>{
                                assert!(header);assert_eq!(chunk.offset,received.len() as u64);received.extend(chunk.data);
                            }
                        }
                    }
                    assert!(header);assert_eq!(received,*expected);
                    download_file_bytes+=received.len();
                }
                let naive_download_ms=started.elapsed().as_secs_f64()*1000.0;
                assert_eq!(download_file_bytes as u64,known_historical_file_bytes);
                println!("TINYTREE_REMOTE_BLAME_DEFAULT_LIMITS_V1 {}",serde_json::json!({
                    "lines":lines,"changing_versions":versions,"trial":trial,
                    "scope_open_ms":scope_open_ms,"blame_ms":blame_ms,
                    "coverage":coverage.as_str_name(),"complete":complete,
                    "scanned_commits":blame.scanned_commits,"unresolved_lines":unresolved,
                    "selected_file_bytes":blame.target_bytes.len(),
                    "blame_uncompressed_protobuf_bytes":blame_protobuf_bytes,
                    "naive_history_file_rpcs":versions,"naive_history_download_ms":naive_download_ms,
                    "naive_history_download_file_bytes":download_file_bytes,
                    "naive_history_download_uncompressed_protobuf_bytes":download_protobuf_bytes,
                    "max_scan_commits":max_scan,"max_diff_work":max_diff_work,
                    "max_diff_scratch_allocation_bytes":max_diff_allocation,
                    "diff_allocation_policy":"per-comparison conservative scratch charge; aggregate input, line and work limits",
                    "storage_state":"warm canonical Store and derived index; fresh scope each trial; OS cache uncontrolled",
                    "transport":"production loopback TCP with Zstd request and response compression",
                    "byte_measurement":"decoded application and uncompressed protobuf bytes; compressed wire bytes not measured",
                    "baseline_scope":"actual naive serial full-history FILE downloads only; not clone or complete client blame",
                    "latency_comparability":if complete {"phase timings only; different computations"} else {"incomplete blame is not comparable to a complete-history result"}
                }));
                query.close_scope(request(q::CloseScopeRequest {scope:scope.key},ALPHA)).await.unwrap();
            }
            drop(objects);drop(query);server.stop().await;
        }
    }).await.expect("bounded remote blame benchmark expired");
}

fn search_input(
    scope: &q::QueryScope,
    revision: ObjectKey,
    pattern: &[u8],
    page_size: u32,
) -> q::SearchRequest {
    q::SearchRequest {
        scope: scope.key.clone(),
        revision: Some(query_id(revision)),
        pattern: pattern.to_vec(),
        path_prefix: String::new(),
        policy: q::SearchPolicy::LiteralBytesV1 as i32,
        page_size,
        cursor: vec![],
    }
}
async fn search_ready(
    client: &mut q::repository_query_client::RepositoryQueryClient<Channel>,
    input: &q::SearchRequest,
) -> q::SearchResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let response = client.search(request(input.clone(), ALPHA)).await.unwrap();
            assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
            let value = response.into_inner();
            if value.coverage != q::SearchCoverage::SearchWarming as i32 {
                break value;
            }
            assert!(value.hits.is_empty());
            assert!(value.cursor.is_empty());
            assert!(value.manifest_digest.is_empty());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_pins_revision_and_pages_exact_matches_with_bound_cursors() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let a = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(
            &store,
            b"needle one\nmiddle needle\n\xffneedle\0 tail",
            false,
        )),
        101,
    )
    .descriptor
    .key;
    let b = history_commit(
        &store,
        &index,
        &[a],
        Some(history_file(
            &store,
            b"different selected revision\n",
            false,
        )),
        102,
    )
    .descriptor
    .key;
    let server = Running::start_query(store.clone(), ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[b], 101).await;
    let input = search_input(&scope, a, b"needle", 1);
    let first = client
        .search(request(input.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.coverage, q::SearchCoverage::SearchWarming as i32);
    assert!(first.hits.is_empty());
    let first = search_ready(&mut client, &input).await;
    assert_eq!(first.coverage, q::SearchCoverage::SearchMore as i32);
    assert_eq!(first.hits.len(), 1);
    assert_eq!(
        (
            first.hits[0].byte_offset,
            first.hits[0].line,
            first.hits[0].byte_column
        ),
        (0, 1, 0)
    );
    assert_eq!(first.revision, Some(query_id(a)));
    let mut next = input.clone();
    next.cursor = first.cursor.clone();
    let second = search_ready(&mut client, &next).await;
    assert_eq!(second.coverage, q::SearchCoverage::SearchMore as i32);
    assert_eq!(
        (
            second.hits[0].byte_offset,
            second.hits[0].line,
            second.hits[0].byte_column
        ),
        (18, 2, 7)
    );
    let replay = client
        .search(request(next.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        replay, second,
        "same authenticated continuation replays the exact immutable page"
    );
    for changed in [
        q::SearchRequest {
            pattern: b"one".to_vec(),
            ..next.clone()
        },
        q::SearchRequest {
            path_prefix: "other".into(),
            ..next.clone()
        },
        q::SearchRequest {
            revision: Some(query_id(b)),
            ..next.clone()
        },
        q::SearchRequest {
            page_size: 2,
            ..next.clone()
        },
    ] {
        assert_eq!(
            client
                .search(request(changed, ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    let mut tampered = next.clone();
    tampered.cursor[9] ^= 1;
    assert_eq!(
        client
            .search(request(tampered, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .search(request(next.clone(), PEER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    next.cursor = second.cursor.clone();
    let third = search_ready(&mut client, &next).await;
    assert_eq!(third.hits[0].byte_offset, 26);
    next.cursor = third.cursor.clone();
    let final_page = if third.coverage == q::SearchCoverage::SearchComplete as i32 {
        third
    } else {
        search_ready(&mut client, &next).await
    };
    assert_eq!(
        final_page.coverage,
        q::SearchCoverage::SearchComplete as i32
    );
    assert!(final_page.cursor.is_empty());
    let negative = search_ready(&mut client, &search_input(&scope, b, b"needle", 2)).await;
    assert_eq!(negative.coverage, q::SearchCoverage::SearchComplete as i32);
    assert!(negative.hits.is_empty());
    let short = search_ready(&mut client, &search_input(&scope, a, b"e", 128)).await;
    assert_eq!(short.coverage, q::SearchCoverage::SearchComplete as i32);
    assert_eq!(
        short.hits.len(),
        b"needle one\nmiddle needle\n\xffneedle\0 tail"
            .iter()
            .filter(|&&v| v == b'e')
            .count()
    );
    for (prefix, count) in [("file", 3), ("fi", 0), ("other", 0)] {
        let mut selected = search_input(&scope, a, b"needle", 128);
        selected.path_prefix = prefix.into();
        let selected = search_ready(&mut client, &selected).await;
        assert_eq!(selected.coverage, q::SearchCoverage::SearchComplete as i32);
        assert_eq!(selected.hits.len(), count);
    }
    for pattern in [b"".as_slice(), b"line\nbreak".as_slice()] {
        assert_eq!(
            client
                .search(request(search_input(&scope, a, pattern, 1), ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    client
        .close_scope(request(q::CloseScopeRequest { scope: scope.key }, ALPHA))
        .await
        .unwrap();
    assert_eq!(
        client
            .search(request(next, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_regex_search_verifies_candidates_and_pages_actual_spans() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let bytes = b"foo12 foo1234\nbar!\nfoo\nbar\nfoozzzbar\n\xffFOO7\n";
    let head = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(&store, bytes, false)),
        100,
    )
    .descriptor
    .key;
    let server = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[head], 100).await;
    for (pattern, expected) in [
        (
            r"foo[0-9]+|bar!",
            vec![&b"foo12"[..], &b"foo1234"[..], &b"bar!"[..]],
        ),
        (r"^foo[0-9]+", vec![&b"foo12"[..]]),
        (r"\bfoo[0-9]+\b", vec![&b"foo12"[..], &b"foo1234"[..]]),
        (r"^|foo[0-9]+", vec![&b"foo1234"[..]]),
        (
            r"(?i)foo[0-9]+",
            vec![&b"foo12"[..], &b"foo1234"[..], &b"FOO7"[..]],
        ),
        (r"foo.*bar", vec![&b"foozzzbar"[..]]),
        (r"foo\nbar", vec![]),
        (r"absent|missing", vec![]),
    ] {
        let mut input = search_input(&scope, head, pattern.as_bytes(), 1);
        input.policy = q::SearchPolicy::RegexBytesV1 as i32;
        let mut hits = Vec::new();
        for _ in 0..32 {
            let page = search_ready(&mut client, &input).await;
            assert_eq!(page.policy, q::SearchPolicy::RegexBytesV1 as i32);
            hits.extend(page.hits.clone());
            if page.coverage == q::SearchCoverage::SearchComplete as i32 {
                assert!(page.cursor.is_empty());
                break;
            }
            assert_eq!(
                page.coverage,
                q::SearchCoverage::SearchMore as i32,
                "{pattern}"
            );
            assert_ne!(input.cursor, page.cursor);
            let replay = search_ready(&mut client, &input).await;
            assert_eq!(page, replay);
            // A continuation cannot change from regex interpretation to literal.
            let mut wrong_policy = input.clone();
            wrong_policy.cursor = page.cursor.clone();
            wrong_policy.policy = q::SearchPolicy::LiteralBytesV1 as i32;
            assert_eq!(
                client
                    .search(request(wrong_policy, ALPHA))
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
            input.cursor = page.cursor;
        }
        let actual = hits
            .iter()
            .map(|hit| {
                let start = hit.byte_offset as usize;
                let end = start + hit.match_bytes as usize;
                let line_start = bytes[..start]
                    .iter()
                    .rposition(|&byte| byte == b'\n')
                    .map_or(0, |i| i + 1);
                assert_eq!(
                    hit.line,
                    1 + bytes[..start].iter().filter(|&&byte| byte == b'\n').count() as u64
                );
                assert_eq!(hit.byte_column, (start - line_start) as u64);
                let relative = (hit.byte_offset - hit.snippet_offset) as usize;
                assert_eq!(
                    &hit.snippet[relative..relative + hit.match_bytes as usize],
                    &bytes[start..end]
                );
                &bytes[start..end]
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{pattern}");
    }
    for pattern in [b"[".as_slice(), b"(?=foo)".as_slice(), b"\xff".as_slice()] {
        let mut input = search_input(&scope, head, pattern, 1);
        input.policy = q::SearchPolicy::RegexBytesV1 as i32;
        assert_eq!(
            client
                .search(request(input, ALPHA))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    client
        .close_scope(request(q::CloseScopeRequest { scope: scope.key }, ALPHA))
        .await
        .unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_limits_do_not_turn_unread_or_unscanned_files_into_empty_success() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let head = history_commit(
        &store,
        &index,
        &[],
        Some(history_file(
            &store,
            b"needle much larger than the requested read cap",
            false,
        )),
        103,
    )
    .descriptor
    .key;
    let mut config = ServiceConfig::default();
    config.query.max_search_file_bytes = 4;
    let server = Running::start_query(store, config, index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[head], 102).await;
    let oversized = search_ready(&mut client, &search_input(&scope, head, b"needle", 128)).await;
    assert_eq!(
        oversized.coverage,
        q::SearchCoverage::SearchReadLimit as i32
    );
    assert!(oversized.hits.is_empty());
    assert!(oversized.cursor.is_empty());
    // A verified filter negative is still conclusive for an oversized body;
    // oversized positives, not all large files indiscriminately, are incomplete.
    let negative = search_ready(&mut client, &search_input(&scope, head, b"zzzzzzzzzz", 128)).await;
    assert_eq!(negative.coverage, q::SearchCoverage::SearchComplete as i32);
    assert!(negative.hits.is_empty());
    for (pattern, coverage) in [
        (r"absent|missing", q::SearchCoverage::SearchComplete),
        (r"needle.*cap", q::SearchCoverage::SearchReadLimit),
        (r".*", q::SearchCoverage::SearchReadLimit),
    ] {
        let mut input = search_input(&scope, head, pattern.as_bytes(), 128);
        input.policy = q::SearchPolicy::RegexBytesV1 as i32;
        let result = search_ready(&mut client, &input).await;
        assert_eq!(result.coverage, coverage as i32, "{pattern}");
        assert!(result.hits.is_empty());
    }
    drop(client);
    server.stop().await;
}

fn search_many_files(store: &Store, index: &tinytree_index::Index, files: usize) -> ObjectKey {
    let mut entries = Vec::new();
    for i in 0..files {
        let bytes = format!("distinct file {i:04} has no requested match\n");
        entries.push(tinytree_model::TreeEntry {
            name: format!("a{i:04}"),
            value: history_file(store, bytes.as_bytes(), false),
        });
    }
    entries.push(tinytree_model::TreeEntry {
        name: "z-match".into(),
        value: history_file(store, b"needle final\n", false),
    });
    let tree = tinytree_model::encode(
        &Object::Tree(tinytree_model::Tree { entries }),
        Limits::default(),
    )
    .unwrap();
    store.put("alpha", tree.descriptor, &tree.bytes, 0).unwrap();
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![CommitId::ROOT];
    commit.change_id = tinytree_model::ChangeId([104; 16]);
    commit.root_tree.terms = vec![TreeId(tree.descriptor.key.id.0)];
    let commit = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", commit.descriptor, &commit.bytes, 0)
        .unwrap();
    index
        .ingest("alpha", commit.descriptor, &commit.bytes)
        .unwrap();
    commit.descriptor.key
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_empty_progress_and_cache_quota_preserve_completeness() {
    for quota in [65_536, 16] {
        let (temporary, store) = setup();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        let head = search_many_files(&store, &index, 17);
        let mut config = ServiceConfig::default();
        config.query.max_search_cache_records = quota;
        config.query.max_search_entries_per_page = 1;
        let server = Running::start_query(store, config, index).await;
        let mut client = server.query_client().await;
        let scope = query_ready(&mut client, &[head], 105).await;
        let mut input = search_input(&scope, head, b"needle", 1);
        let mut page = search_ready(&mut client, &input).await;
        if quota == 16 {
            assert_eq!(page.coverage, q::SearchCoverage::SearchIndexLimit as i32);
            assert!(page.hits.is_empty());
            assert!(page.cursor.is_empty());
        } else {
            let mut hits = Vec::new();
            let mut empty_pages = 0;
            loop {
                if page.hits.is_empty() && page.coverage == q::SearchCoverage::SearchMore as i32 {
                    empty_pages += 1;
                }
                hits.extend(page.hits.clone());
                if page.coverage == q::SearchCoverage::SearchComplete as i32 {
                    break;
                }
                assert_eq!(page.coverage, q::SearchCoverage::SearchMore as i32);
                assert!(!page.cursor.is_empty());
                let progress = page.entries_examined;
                input.cursor = page.cursor;
                page = search_ready(&mut client, &input).await;
                assert!(page.entries_examined > progress);
            }
            assert_eq!(empty_pages, 17);
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].path, "z-match");
        }
        drop(client);
        server.stop().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_filter_positive_is_not_a_match_or_canonical_authority() {
    let (temporary, store) = setup();
    let index =
        tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
    let bytes = b"haystack without the requested literal\n";
    let value = history_file(&store, bytes, false);
    let head = history_commit(&store, &index, &[], Some(value), 106)
        .descriptor
        .key;
    // An all-positive Bloom filter is conservative but cannot establish a hit.
    index
        .put_content_file(
            "alpha",
            tinytree_model::file_id(bytes),
            bytes.len() as u64,
            tinytree_search::FileFilter::from_bytes([255; 512]),
            &Default::default(),
        )
        .unwrap();
    let server = Running::start_query(store, ServiceConfig::default(), index).await;
    let mut client = server.query_client().await;
    let scope = query_ready(&mut client, &[head], 106).await;
    let found = search_ready(&mut client, &search_input(&scope, head, b"needle", 128)).await;
    assert_eq!(found.coverage, q::SearchCoverage::SearchComplete as i32);
    assert!(found.hits.is_empty());
    assert_eq!(found.file_bytes_read, bytes.len() as u64);
    drop(client);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_work_is_spent_on_visited_entries_and_stays_aggregate_across_pages() {
    for limited in [false, true] {
        let (temporary, store) = setup();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        let mut bytes = vec![b'x'; 16 * 1024];
        for offset in (0..bytes.len()).step_by(2048) {
            bytes[offset..offset + 6].copy_from_slice(b"needle");
        }
        let head = history_commit(
            &store,
            &index,
            &[],
            Some(history_file(&store, &bytes, false)),
            107,
        )
        .descriptor
        .key;
        let mut config = ServiceConfig::default();
        if limited {
            config.query.max_search_match_work = 20_000;
            config.query.max_search_total_work = 25_000;
        }
        let server = Running::start_query(store, config, index).await;
        let mut client = server.query_client().await;
        let scope = query_ready(&mut client, &[head], 107).await;
        let mut input = search_input(&scope, head, b"needle", if limited { 1 } else { 128 });
        let mut count = 0;
        let mut terminal = false;
        for _ in 0..32 {
            let page = search_ready(&mut client, &input).await;
            count += page.hits.len();
            if page.coverage == q::SearchCoverage::SearchMore as i32 {
                input.cursor = page.cursor;
                continue;
            }
            if limited {
                assert_eq!(page.coverage, q::SearchCoverage::SearchWorkLimit as i32);
                assert!(
                    count > 0 && count < 8,
                    "aggregate work cannot be reset by a result page"
                );
            } else {
                assert_eq!(page.coverage, q::SearchCoverage::SearchComplete as i32);
                assert_eq!(
                    count, 8,
                    "one ordinary 16KiB candidate must have its full matcher allowance"
                );
                let short = search_ready(&mut client, &search_input(&scope, head, b"e", 128)).await;
                assert_eq!(short.coverage, q::SearchCoverage::SearchComplete as i32);
                assert_eq!(short.hits.len(), 24);
            }
            terminal = true;
            break;
        }
        assert!(
            terminal,
            "bounded query must reach its complete or exhausted terminal state"
        );
        drop(client);
        server.stop().await;
    }
}

fn search_benchmark_revision(
    store: &Store,
    index: &tinytree_index::Index,
    files: &[(String, Descriptor, Vec<u8>)],
    parent: Option<ObjectKey>,
    change: u8,
) -> ObjectKey {
    let entries = files
        .iter()
        .map(|(path, descriptor, bytes)| tinytree_model::TreeEntry {
            name: path.clone(),
            value: tinytree_model::TreeValue::File {
                id: tinytree_model::FileId(descriptor.key.id.0),
                executable: false,
                size_bytes: bytes.len() as u64,
                copy_id: None,
            },
        })
        .collect();
    let tree = tinytree_model::encode(
        &Object::Tree(tinytree_model::Tree { entries }),
        Limits::default(),
    )
    .unwrap();
    store.put("alpha", tree.descriptor, &tree.bytes, 0).unwrap();
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![parent.map_or(CommitId::ROOT, |key| CommitId(key.id.0))];
    commit.change_id = tinytree_model::ChangeId([change; 16]);
    commit.root_tree.terms = vec![TreeId(tree.descriptor.key.id.0)];
    let commit = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
    store
        .put("alpha", commit.descriptor, &commit.bytes, 0)
        .unwrap();
    index
        .ingest("alpha", commit.descriptor, &commit.bytes)
        .unwrap();
    commit.descriptor.key
}
async fn search_benchmark_server(
    store: Store,
    index: std::sync::Arc<tinytree_index::Index>,
) -> (Running, RepositoryService) {
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["alpha".into()],
        vec![Principal::new("alpha", ALPHA, vec!["alpha".into()], true).unwrap()],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let observer = service.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(serve_tcp(listener, service, None, async {
        let _ = stopped.await;
    }));
    (
        Running {
            address,
            stop: Some(stop),
            task: Some(task),
        },
        observer,
    )
}
async fn benchmark_search_complete(
    client: &mut q::repository_query_client::RepositoryQueryClient<Channel>,
    mut input: q::SearchRequest,
) -> (q::SearchResult, Vec<(String, u64)>, u64, u64) {
    let mut hits = Vec::new();
    let mut polls = 0;
    let mut pages = 0;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let response = client.search(request(input.clone(), ALPHA)).await.unwrap();
            assert_eq!(response.metadata().get("grpc-encoding").unwrap(), "zstd");
            let page = response.into_inner();
            assert_eq!(page.revision, input.revision);
            if page.coverage == q::SearchCoverage::SearchWarming as i32 {
                polls += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            pages += 1;
            hits.extend(
                page.hits
                    .iter()
                    .map(|hit| (hit.path.clone(), hit.byte_offset)),
            );
            if page.coverage == q::SearchCoverage::SearchMore as i32 {
                input.cursor = page.cursor.clone();
                continue;
            }
            break (page, hits, polls, pages);
        }
    })
    .await
    .unwrap()
}
fn naive_literal_offsets(bytes: &[u8], pattern: &[u8]) -> Vec<u64> {
    let mut offsets = Vec::new();
    let mut at = 0;
    while at + pattern.len() <= bytes.len() {
        if &bytes[at..at + pattern.len()] == pattern {
            offsets.push(at as u64);
            at += pattern.len();
        } else {
            at += 1;
        }
    }
    offsets
}
async fn benchmark_search_stats(
    service: &RepositoryService,
    jobs: u64,
) -> tinytree_server::SearchStats {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = service.search_stats().unwrap();
            if stats.warm_jobs_completed >= jobs {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual indexed-search cold, warm and one-file iteration RPC benchmark"]
async fn benchmark_indexed_search_cold_warm_and_single_file_iteration() {
    tokio::time::timeout(Duration::from_secs(120), async {
        const PATTERN: &[u8] = b"UNIQUE_NEEDLE_9fe7";
        for count in [64usize, 1024] {
            for trial in 0..3u8 {
                let (temporary,store) = setup();
                let index = tinytree_index::Index::open(temporary.path().join("derived"), &Default::default()).unwrap();
                let mut files = Vec::new();
                for i in 0..count {
                    let text = b"fn shared_code() { return 7; }\n";
                    let mut bytes = (0..4096).map(|at| text[at%text.len()]).collect::<Vec<_>>();
                    let header = format!("// file {i:06}\n"); bytes[..header.len()].copy_from_slice(header.as_bytes());
                    if i == 0 { bytes[2048..2048+PATTERN.len()].copy_from_slice(PATTERN); }
                    let descriptor = tinytree_model::describe_file(&bytes);
                    store.put("alpha",descriptor,&bytes,0).unwrap();
                    files.push((format!("file{i:06}.rs"),descriptor,bytes));
                }
                let first = search_benchmark_revision(&store,&index,&files,None,108);
                let (server,observer) = search_benchmark_server(store.clone(),index.clone()).await;
                let mut query = server.query_client().await;
                let mut objects = server.client().await;
                let scope = query_ready(&mut query,&[first],110+trial).await;
                let input = search_input(&scope,first,PATTERN,128);
                let start = std::time::Instant::now();
                let (cold,cold_hits,cold_polls,cold_pages) = benchmark_search_complete(&mut query,input.clone()).await;
                let cold_ms = start.elapsed().as_secs_f64()*1000.0;
                let base_stats = benchmark_search_stats(&observer,1).await;
                assert_eq!(base_stats.warm_files_built,count as u64);
                assert_eq!(base_stats.warm_file_bytes,(count*4096) as u64);
                let start = std::time::Instant::now();
                let (warm,warm_hits,warm_polls,warm_pages) = benchmark_search_complete(&mut query,input).await;
                let warm_ms = start.elapsed().as_secs_f64()*1000.0;
                assert_eq!(cold_hits,warm_hits);
                assert_eq!(warm_polls,0);
                let start = std::time::Instant::now();
                let mut downloaded = 0u64; let mut baseline_hits = Vec::new();
                for (path,descriptor,expected) in &files {
                    let response = objects.read_file(request(p::ReadFileRequest { repository_id:"alpha".into(),native_file_id:descriptor.key.id.0.to_vec() },ALPHA)).await.unwrap();
                    assert_eq!(response.metadata().get("grpc-encoding").unwrap(),"zstd");
                    let mut stream = response.into_inner(); let mut bytes = Vec::new();
                    while let Some(frame) = stream.message().await.unwrap() {
                        match frame.frame.unwrap() {
                            p::file_frame::Frame::Header(actual) => assert_eq!(actual,crate::descriptor(*descriptor)),
                            p::file_frame::Frame::Chunk(chunk) => { assert_eq!(chunk.offset,bytes.len() as u64); bytes.extend(chunk.data); }
                        }
                    }
                    assert_eq!(&bytes,expected); downloaded += bytes.len() as u64;
                    baseline_hits.extend(naive_literal_offsets(&bytes,PATTERN).into_iter().map(|offset| (path.clone(),offset)));
                }
                let baseline_ms = start.elapsed().as_secs_f64()*1000.0;
                if warm.coverage == q::SearchCoverage::SearchComplete as i32 { assert_eq!(warm_hits,baseline_hits); }
                else { assert!(warm_hits.iter().all(|hit| baseline_hits.contains(hit))); }
                files[1].2[2048..2048+PATTERN.len()].copy_from_slice(PATTERN);
                files[1].1 = tinytree_model::describe_file(&files[1].2);
                store.put("alpha",files[1].1,&files[1].2,0).unwrap();
                let second = search_benchmark_revision(&store,&index,&files,Some(first),109);
                let second_scope = query_ready(&mut query,&[second],120+trial).await;
                let start = std::time::Instant::now();
                let (iteration,iteration_hits,iteration_polls,iteration_pages) = benchmark_search_complete(&mut query,search_input(&second_scope,second,PATTERN,128)).await;
                let iteration_ms = start.elapsed().as_secs_f64()*1000.0;
                let after_stats = benchmark_search_stats(&observer,2).await;
                assert_eq!(after_stats.warm_files_built-base_stats.warm_files_built,1);
                assert_eq!(after_stats.warm_files_reused-base_stats.warm_files_reused,(count-1) as u64);
                assert_eq!(after_stats.warm_file_bytes-base_stats.warm_file_bytes,4096);
                let expected_iteration = files.iter().flat_map(|(path,_,bytes)| naive_literal_offsets(bytes,PATTERN).into_iter().map(move |offset| (path.clone(),offset))).collect::<Vec<_>>();
                if iteration.coverage == q::SearchCoverage::SearchComplete as i32 { assert_eq!(iteration_hits,expected_iteration); }
                let start = std::time::Instant::now();
                let (short,short_hits,short_polls,short_pages) = benchmark_search_complete(&mut query,search_input(&second_scope,second,&[0xfe],128)).await;
                let short_ms = start.elapsed().as_secs_f64()*1000.0;
                assert!(short_hits.is_empty());
                if short.coverage == q::SearchCoverage::SearchComplete as i32 { assert_eq!(short.file_bytes_read,downloaded); }
                println!("TINYTREE_INDEXED_SEARCH_RPC_V1 {}",serde_json::json!({
                    "files":count,"file_bytes":4096,"trial":trial,
                    "transport":"production compressed loopback TCP; decoded bytes, not compressed wire bytes",
                    "storage_state":"warm canonical Store and graph index; cold content index per trial; OS cache uncontrolled",
                    "poll_interval_ms":5,
                    "cold_ready_ms":cold_ms,"cold_warming_polls":cold_polls,"cold_pages":cold_pages,"cold_coverage":q::SearchCoverage::try_from(cold.coverage).unwrap().as_str_name(),
                    "warm_query_ms":warm_ms,"warm_pages":warm_pages,"warm_coverage":q::SearchCoverage::try_from(warm.coverage).unwrap().as_str_name(),"warm_candidate_file_bytes":warm.file_bytes_read,
                    "base_filters_built":base_stats.warm_files_built,"base_filter_file_bytes":base_stats.warm_file_bytes,
                    "naive_snapshot_download_and_scan_ms":baseline_ms,"naive_snapshot_file_rpcs":count,"naive_snapshot_file_bytes":downloaded,
                    "iteration_ready_ms":iteration_ms,"iteration_warming_polls":iteration_polls,"iteration_pages":iteration_pages,"iteration_coverage":q::SearchCoverage::try_from(iteration.coverage).unwrap().as_str_name(),
                    "iteration_filters_built":after_stats.warm_files_built-base_stats.warm_files_built,"iteration_filters_reused":after_stats.warm_files_reused-base_stats.warm_files_reused,"iteration_filter_file_bytes":after_stats.warm_file_bytes-base_stats.warm_file_bytes,
                    "short_negative_ms":short_ms,"short_pages":short_pages,"short_warming_polls":short_polls,"short_coverage":q::SearchCoverage::try_from(short.coverage).unwrap().as_str_name(),"short_candidate_file_bytes":short.file_bytes_read,
                    "baseline_scope":"actual serial ReadFile of every known snapshot FILE plus independent naive nonoverlapping literal scan; excludes tree enumeration, clone, checkout and client indexing",
                    "comparability":"same complete literal hit set only when COMPLETE; no partial-result speedup, WAN, shallow-clone or ripgrep claim",
                    "warming_durability":"buffered rebuildable filter transactions; SyncAll complete manifest barrier",
                    "logical_bytes":"FILE payloads opened, not physical disk IO; Store verification/reconstruction can read more"
                }));
                drop(query);drop(objects);server.stop().await;
            }
        }
    }).await.unwrap();
}

fn landing_config() -> tinytree_server::Config {
    let mut config: tinytree_server::Config =
        toml::from_str(include_str!("../examples/landing.toml")).unwrap();
    config.repositories = vec!["alpha".into(), "beta".into()];
    config.service.landing.profiles[0].repositories = vec!["alpha".into()];
    config.principals.clear();
    config.service.landing.poll_interval_ms = 10;
    config.validate().unwrap();
    config
}
fn landing_principals(grant: bool) -> Vec<Principal> {
    vec![
        Principal::new("alpha", ALPHA, vec!["alpha".into()], grant)
            .unwrap()
            .with_landing_profiles(if grant { vec!["checks".into()] } else { vec![] }),
        Principal::new("peer", PEER, vec!["alpha".into()], true)
            .unwrap()
            .with_landing_profiles(vec!["checks".into()]),
        Principal::new("beta", BETA, vec!["beta".into()], true)
            .unwrap()
            .with_landing_profiles(vec!["checks".into()]),
        Principal::new("reader", READER, vec!["alpha".into()], false)
            .unwrap()
            .with_landing_profiles(vec!["checks".into()]),
    ]
}
fn landing_setup() -> (
    tempfile::TempDir,
    Store,
    tinytree_server::Config,
    tinytree_proto::landing_v1::StartLandingRequest,
) {
    let config = landing_config();
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        temporary.path(),
        config.store.materialize(config.service.max_metadata_bytes),
    )
    .unwrap();
    store.create_repository("alpha").unwrap();
    store.create_repository("beta").unwrap();
    let fixture = fixture();
    for object in fixture.objects() {
        store
            .put("alpha", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let base = root(&fixture);
    let (_, bytes) = store.read_metadata("alpha", base).unwrap();
    let Object::Commit(mut draft) = tinytree_model::decode(&bytes, Limits::default()).unwrap()
    else {
        panic!()
    };
    draft.parents = vec![CommitId(base.id.0)];
    draft.change_id = tinytree_model::ChangeId([99; 16]);
    draft.description = "private source draft".into();
    let source = tinytree_model::encode(&Object::Commit(draft), Limits::default()).unwrap();
    store
        .put("alpha", source.descriptor, &source.bytes, 0)
        .unwrap();
    store
        .publish(
            "alpha",
            &tinytree_store::PublishRequest {
                idempotency_key: "landing-baseline".into(),
                reference_name: "main".into(),
                expected_generation: 0,
                new_commit: Some(CommitId(base.id.0)),
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
    let input = tinytree_proto::landing_v1::StartLandingRequest {
        repository_id: "alpha".into(),
        idempotency_key: "landing-rpc".into(),
        source_base: base.id.0.to_vec(),
        source_head: source.descriptor.key.id.0.to_vec(),
        destination: "main".into(),
        expected_generation: 1,
        expected_head: base.id.0.to_vec(),
        profile_name: "checks".into(),
        expected_profile_version: "v1".into(),
    };
    (temporary, store, config, input)
}

// Pinned executable fixture validates service orchestration only; native/RBE
// acceptance is a separate workflow using the actual check runner.
fn checked_landing_setup(
    mode: &str,
) -> (
    tempfile::TempDir,
    Store,
    tinytree_server::Config,
    tinytree_proto::landing_v1::StartLandingRequest,
    std::path::PathBuf,
) {
    use sha2::{Digest, Sha256};
    let (temporary, store, mut config, mut input) = landing_setup();
    let directory = private_fs::PrivateDir::create(temporary.path().join("operator")).unwrap();
    let executable = std::path::Path::new(env!("TINYTREE_CHECKER_FIXTURE"))
        .canonicalize()
        .unwrap();
    let pin = format!("{:x}", Sha256::digest(std::fs::read(&executable).unwrap()));
    let daemon = directory.path().join("daemon.toml");
    std::fs::write(&daemon, b"# public protocol fixture\n").unwrap();
    let scratch = directory.path().join("fixture-state");
    let policy = directory.path().join("checker.toml");
    let quoted = |path: &std::path::Path| serde_json::to_string(path.to_str().unwrap()).unwrap();
    let text = format!(
        r#"scratch={}
daemon_config={}
rbe_socket={}
runner_sha256="{}"
[workspace]
path={}
sha256="{}"
[bazel]
path={}
sha256="{}"
[[profiles]]
name="checks"
repositories=["alpha"]
store="alpha"
mode="build"
targets=["//fixture:checks"]
flags=["--define=fixture_mode={}"]
timeout_seconds=60
"#,
        quoted(&scratch),
        quoted(&daemon),
        quoted(&directory.path().join("rbe.sock")),
        pin,
        quoted(&executable),
        pin,
        quoted(&executable),
        pin,
        mode
    );
    std::fs::write(&policy, text).unwrap();
    let runner = tinytree_check_runner::Config::load(&policy).unwrap();
    let version = runner.profile_version("checks").unwrap();
    config.service.landing.profiles[0].version = version.clone();
    input.expected_profile_version = version;
    config.service.landing.max_buffered_bytes = 6 * 1024 * 1024 * 1024;
    config.service.landing.job_ttl_seconds = 30;
    config.service.landing.checker = Some(tinytree_server::LandingCheckerConfig {
        enabled: true,
        executable,
        sha256: pin,
        config: policy,
        max_checks: 1,
        max_checks_per_repository: 1,
        max_jobs: 16,
        max_attempts: 2,
        max_recovery_attempts: 8,
        retry_delay_seconds: 1,
        execution_timeout_seconds: 30,
        cleanup_grace_seconds: 10,
    });
    (temporary, store, config, input, scratch)
}
async fn wait_landing_phase(
    client: &mut tinytree_proto::landing_v1::landing_client::LandingClient<Channel>,
    input: &tinytree_proto::landing_v1::StartLandingRequest,
    phase: tinytree_proto::landing_v1::Phase,
) -> tinytree_proto::landing_v1::LandingJob {
    let mut last = None;
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let job = client
                .get_landing(request(
                    tinytree_proto::landing_v1::GetLandingRequest {
                        repository_id: input.repository_id.clone(),
                        idempotency_key: input.idempotency_key.clone(),
                    },
                    ALPHA,
                ))
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            last = Some(job.phase);
            if job.phase == phase as i32 {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    result.unwrap_or_else(|_| panic!("landing did not reach {phase:?}; last phase {last:?}"))
}

#[tokio::test]
async fn pinned_checker_receipts_publish_or_reject_and_recover_lost_reply() {
    use tinytree_proto::landing_v1 as l;
    for (mode, phase, generation, expect_recovery) in [
        ("pass", l::Phase::Committed, 2, false),
        ("fail", l::Phase::CheckFailed, 1, false),
        ("lost_reply", l::Phase::Committed, 2, true),
    ] {
        let (_temporary, store, mut config, input, scratch) = checked_landing_setup(mode);
        config.service.protected_references = vec![tinytree_server::ProtectedReference {
            repository: "alpha".into(),
            reference: "main".into(),
        }];
        let running = Running::start_landing(
            store.clone(),
            config.service,
            landing_principals(true),
            true,
        )
        .await;
        let mut client = running.landing_client().await;
        client
            .start_landing(request(input.clone(), ALPHA))
            .await
            .unwrap();
        let job = wait_landing_phase(&mut client, &input, phase).await;
        assert_eq!(
            store.references("alpha", &["main".into()]).unwrap()[0].generation,
            generation,
            "{mode}"
        );
        if phase == l::Phase::Committed {
            assert_eq!(
                job.receipt
                    .as_ref()
                    .unwrap()
                    .reference
                    .as_ref()
                    .unwrap()
                    .generation,
                2
            );
            assert_eq!(job.receipt.as_ref().unwrap().evidence.len(), 32);
        }
        let replay = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match client.start_landing(request(input.clone(), ALPHA)).await {
                    Ok(response) => break response.into_inner().job.unwrap(),
                    Err(status) if status.code() == Code::ResourceExhausted => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Err(status) => panic!("unexpected replay error: {status}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(replay.id, job.id);
        assert_eq!(replay.receipt, job.receipt);
        let executions = std::fs::read_to_string(scratch.join("executions")).unwrap();
        assert_eq!(executions.lines().filter(|line| *line == "run").count(), 1);
        if expect_recovery {
            assert!(executions.lines().any(|line| line == "recover"));
        }
        running.stop().await;
    }
}

#[tokio::test]
async fn target_movement_while_checking_rejects_exact_old_candidate() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, config, input, scratch) = checked_landing_setup("pause");
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !scratch.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut queued = input.clone();
    queued.idempotency_key = "preparation-during-check".into();
    client
        .start_landing(request(queued.clone(), ALPHA))
        .await
        .unwrap();
    wait_landing_phase(&mut client, &queued, l::Phase::PreparedPendingChecks).await;
    store
        .publish(
            "alpha",
            &tinytree_store::PublishRequest {
                idempotency_key: "advance-during-check".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(CommitId(input.expected_head.clone().try_into().unwrap())),
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
    std::fs::write(scratch.join("release"), b"release fixture").unwrap();
    let job = wait_landing_phase(&mut client, &input, l::Phase::StaleTarget).await;
    assert!(job.receipt.is_none());
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        2
    );
    running.stop().await;
}

#[tokio::test]
async fn uncertain_cleanup_vetoes_ttl_and_gc_across_checker_disabled_restart() {
    use tinytree_proto::landing_v1 as l;
    let (temporary, store, mut config, input, scratch) = checked_landing_setup("uncertain");
    config.service.landing.job_ttl_seconds = 2;
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    let job = wait_landing_phase(&mut client, &input, l::Phase::PreparedPendingChecks).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !scratch.join("executions").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let retained = wait_landing_phase(&mut client, &input, l::Phase::PreparedPendingChecks).await;
    assert!(retained.roots_retained);
    running.stop().await;
    drop(client);
    drop(store);
    let store = Store::open_existing(
        temporary.path(),
        config.store.materialize(config.service.max_metadata_bytes),
    )
    .unwrap();
    config.service.landing.checker = None;
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(false),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let retained = wait_landing_phase(&mut client, &input, l::Phase::PreparedPendingChecks).await;
    assert!(retained.roots_retained);
    store.gc("alpha", i64::MAX - 1).unwrap();
    let candidate = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(job.candidate.unwrap().head.try_into().unwrap()),
    };
    assert!(store.read_metadata("alpha", candidate).is_ok());
    std::fs::write(scratch.join("release"), b"fixture cleanup now confirmed").unwrap();
    let checked_input = input.clone();
    let result = tokio::spawn(async move {
        wait_landing_phase(&mut client, &checked_input, l::Phase::Expired).await
    })
    .await;
    if result.is_err() {
        eprintln!(
            "fixture executions: {:?}",
            std::fs::read_to_string(scratch.join("executions"))
        );
        let jobs = store
            .external_checks_directory()
            .unwrap()
            .unwrap()
            .join("jobs");
        for entry in std::fs::read_dir(jobs).unwrap() {
            let entry = entry.unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(entry.path().join("state.json")).unwrap())
                    .unwrap();
            eprintln!(
                "checker state {}",
                serde_json::json!({"phase":value["record"]["phase"],"recoveries":value["record"]["recoveries"],"retry_at":value["record"]["retry_at"],"receipt":value["record"]["receipt"]})
            );
        }
    }
    let expired = result.unwrap();
    assert!(!expired.roots_retained);
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    running.stop().await;
}
impl Running {
    async fn landing_client(
        &self,
    ) -> tinytree_proto::landing_v1::landing_client::LandingClient<Channel> {
        let channel = Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .unwrap();
        tinytree_proto::landing_v1::landing_client::LandingClient::new(channel)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
    }
    async fn start_landing(
        store: Store,
        config: ServiceConfig,
        principals: Vec<Principal>,
        dispatch: bool,
    ) -> Self {
        let service = RepositoryService::new(
            store,
            config,
            vec!["alpha".into(), "beta".into()],
            principals,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = if dispatch {
            tokio::spawn(serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            }))
        } else {
            // Crash-window fixture: accept through the actual compressed RPC
            // boundary before starting dispatch, then reopen with the listener
            // helper which must discover accepted work without a client retry.
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .layer(service.deadline_layer())
                    .add_service(service.landing_grpc_service())
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = stopped.await;
                        },
                    )
                    .await?;
                service.drain_landing().await;
                Ok(())
            })
        };
        Self {
            address,
            stop: Some(stop),
            task: Some(task),
        }
    }
}

#[tokio::test]
async fn landing_preparation_is_owner_scoped_compressed_replayable_and_never_publishes() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, config, input) = landing_setup();
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    let initial = client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(initial.repository_id, input.repository_id);
    assert_eq!(initial.idempotency_key, input.idempotency_key);
    let initial = initial.job.unwrap();
    assert_eq!(initial.phase, l::Phase::Intent as i32);
    assert!(initial.candidate.is_none());
    assert_eq!(initial.profile_version, "v1");
    assert!(initial.roots_retained);
    let get = l::GetLandingRequest {
        repository_id: "alpha".into(),
        idempotency_key: input.idempotency_key.clone(),
    };
    let echo = client
        .get_landing(request(get.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(echo.repository_id, get.repository_id);
    assert_eq!(echo.idempotency_key, get.idempotency_key);

    let prepared = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let job = client
                .get_landing(request(get.clone(), ALPHA))
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            if job.phase == l::Phase::PreparedPendingChecks as i32 {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(prepared.candidate.is_some());
    assert!(prepared.receipt.is_none());
    assert_eq!(
        prepared.preparation,
        l::PreparationAvailability::ProfileConfigured as i32
    );
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    let replay = client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    assert_eq!(replay.id, initial.id);
    assert_eq!(replay.expires_at_seconds, initial.expires_at_seconds);
    assert_eq!(replay.candidate, prepared.candidate);
    assert_eq!(
        client
            .get_landing(request(get.clone(), PEER))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        client
            .get_landing(request(get.clone(), BETA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut changed = input.clone();
    changed.source_head = [7u8; 32].to_vec();
    assert_eq!(
        client
            .start_landing(request(changed, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut high_ratio = input.clone();
    high_ratio.idempotency_key = "decompression-cap".into();
    high_ratio.destination = "a".repeat(128 * 1024);
    let error = client
        .start_landing(request(high_ratio, ALPHA))
        .await
        .unwrap_err();
    assert!(matches!(
        error.code(),
        Code::ResourceExhausted | Code::OutOfRange
    ));
    assert_eq!(
        client
            .get_landing(request(
                l::GetLandingRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: "decompression-cap".into()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let mut malformed = input.clone();
    malformed.source_head.pop();
    assert_eq!(
        client
            .start_landing(request(malformed, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut wrong = input.clone();
    wrong.idempotency_key = "wrong-profile".into();
    wrong.profile_name = "other".into();
    assert_eq!(
        client
            .start_landing(request(wrong, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut wrong = input.clone();
    wrong.idempotency_key = "wrong-version".into();
    wrong.expected_profile_version = "v2".into();
    assert_eq!(
        client
            .start_landing(request(wrong, ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        client
            .start_landing(request(input.clone(), READER))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut wrong = input;
    wrong.repository_id = "beta".into();
    assert_eq!(
        client
            .start_landing(request(wrong, BETA))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    running.stop().await;
}

#[tokio::test]
async fn landing_restart_discovers_intents_revalidates_profiles_and_preserves_owner_recovery_reads()
{
    use tinytree_proto::landing_v1 as l;
    let (temporary, store, mut config, input) = landing_setup();
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(true),
        false,
    )
    .await;
    let mut client = running.landing_client().await;
    let accepted = client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    assert_eq!(accepted.phase, l::Phase::Intent as i32);
    running.stop().await;
    drop(client);
    drop(store);
    config.service.landing.profiles[0].version = "v2".into();
    let store = Store::open_existing(
        temporary.path(),
        config.store.materialize(config.service.max_metadata_bytes),
    )
    .unwrap();
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(false),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    let get = l::GetLandingRequest {
        repository_id: "alpha".into(),
        idempotency_key: input.idempotency_key.clone(),
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let unchanged = client
        .get_landing(request(get.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    assert_eq!(unchanged.phase, l::Phase::Intent as i32);
    assert_eq!(
        unchanged.preparation,
        l::PreparationAvailability::ProfileUnavailable as i32
    );
    assert_eq!(
        client
            .start_landing(request(input.clone(), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    running.stop().await;
    drop(client);
    drop(store);
    config.service.landing.profiles[0].version = "v1".into();
    let store = Store::open_existing(
        temporary.path(),
        config.store.materialize(config.service.max_metadata_bytes),
    )
    .unwrap();
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(false),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    let prepared = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let job = client
                .get_landing(request(get.clone(), ALPHA))
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            if job.phase == l::Phase::PreparedPendingChecks as i32 {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(prepared.id, accepted.id);
    assert!(prepared.receipt.is_none());
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn landing_local_owner_launch_requires_explicit_scoped_opt_in_and_disabled_status_still_reads()
 {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, mut config, input) = landing_setup();
    let running = Running::start_landing(store.clone(), config.service.clone(), vec![], true).await;
    let mut client = running.landing_client().await;
    assert_eq!(
        client
            .start_landing(input.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    running.stop().await;
    config.service.landing.local_owner_profiles = vec!["checks".into()];
    let running = Running::start_landing(store.clone(), config.service.clone(), vec![], true).await;
    let mut client = running.landing_client().await;
    let accepted = client
        .start_landing(input.clone())
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    running.stop().await;
    let named = vec![Principal::new("local-owner", ALPHA, vec!["alpha".into()], true).unwrap()];
    let running = Running::start_landing(store.clone(), config.service.clone(), named, true).await;
    let mut client = running.landing_client().await;
    assert_eq!(
        client
            .start_landing(request(input.clone(), ALPHA))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .get_landing(request(
                l::GetLandingRequest {
                    repository_id: "alpha".into(),
                    idempotency_key: input.idempotency_key.clone()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    running.stop().await;
    config.service.landing.enabled = false;
    let running = Running::start_landing(store.clone(), config.service, vec![], true).await;
    let mut client = running.landing_client().await;
    let job = client
        .get_landing(l::GetLandingRequest {
            repository_id: "alpha".into(),
            idempotency_key: input.idempotency_key,
        })
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    assert_eq!(job.id, accepted.id);
    assert_eq!(
        job.preparation,
        l::PreparationAvailability::PreparationDisabled as i32
    );
    running.stop().await;
}

#[tokio::test]
async fn landing_poison_intent_does_not_starve_later_jobs_or_expiry_under_one_heavy_slot() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, mut config, input) = landing_setup();
    let base = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(input.source_base.clone().try_into().unwrap()),
    };
    let (_, bytes) = store.read_metadata("alpha", base).unwrap();
    let Object::Commit(template) = tinytree_model::decode(&bytes, Limits::default()).unwrap()
    else {
        panic!()
    };
    let mut parent = CommitId(base.id.0);
    for index in 0..65 {
        let mut commit = template.clone();
        commit.parents = vec![parent];
        let mut change = [(index + 1) as u8; 16];
        if tinytree_model::ChangeId(change) == template.change_id {
            change[15] ^= 1;
        }
        commit.change_id = tinytree_model::ChangeId(change);
        commit.description = format!("oversized stack {index}");
        let encoded = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
        store
            .put("alpha", encoded.descriptor, &encoded.bytes, 0)
            .unwrap();
        parent = CommitId(encoded.descriptor.key.id.0);
    }
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(true),
        false,
    )
    .await;
    let mut client = running.landing_client().await;
    let mut valid = input.clone();
    valid.idempotency_key = "valid-after-poison".into();
    let accepted = client
        .start_landing(request(valid.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    let mut poison = input.clone();
    poison.source_head = parent.0.to_vec();
    let mut first_poison = None;
    for index in 0..64 {
        poison.idempotency_key = format!("poison-{index}");
        let job = client
            .start_landing(request(poison.clone(), ALPHA))
            .await
            .unwrap()
            .into_inner()
            .job
            .unwrap();
        if job.id < accepted.id {
            first_poison = Some(poison.idempotency_key.clone());
            break;
        }
    }
    let first_poison =
        first_poison.expect("fixed fixture must include a poison ID before the valid job");
    running.stop().await;
    config.service.landing.job_ttl_seconds = 1;
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(true),
        false,
    )
    .await;
    let mut client = running.landing_client().await;
    let mut due = input.clone();
    due.idempotency_key = "due-amid-poison".into();
    let expiring = client
        .start_landing(request(due.clone(), ALPHA))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    running.stop().await;
    config.service.landing.job_ttl_seconds = 86400;
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let job = client
                .get_landing(request(
                    l::GetLandingRequest {
                        repository_id: "alpha".into(),
                        idempotency_key: valid.idempotency_key.clone(),
                    },
                    ALPHA,
                ))
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            if job.phase == l::Phase::PreparedPendingChecks as i32 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let poison_job = client
        .get_landing(request(
            l::GetLandingRequest {
                repository_id: "alpha".into(),
                idempotency_key: first_poison,
            },
            ALPHA,
        ))
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap();
    assert_eq!(poison_job.phase, l::Phase::Intent as i32);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let job = client
                .get_landing(request(
                    l::GetLandingRequest {
                        repository_id: "alpha".into(),
                        idempotency_key: due.idempotency_key.clone(),
                    },
                    ALPHA,
                ))
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            if job.phase == l::Phase::Expired as i32 {
                assert!(!job.roots_retained);
                assert_eq!(job.id, expiring.id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn mismatched_checker_receipt_never_checks_or_publishes() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, config, input, scratch) = checked_landing_setup("mismatch");
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !scratch.join("executions").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wait_checker_record(&store, &["Uncertain"]).await;
    let job = wait_landing_phase(&mut client, &input, l::Phase::PreparedPendingChecks).await;
    assert!(job.roots_retained && job.receipt.is_none());
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn changed_checker_policy_cannot_authorize_a_completed_old_check() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, config, input, scratch) = checked_landing_setup("pause");
    let policy = config
        .service
        .landing
        .checker
        .as_ref()
        .unwrap()
        .config
        .clone();
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !scratch.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let bytes = std::fs::read_to_string(&policy)
        .unwrap()
        .replace("fixture_mode=pause", "fixture_mode=pass");
    std::fs::write(policy, bytes).unwrap();
    std::fs::write(scratch.join("release"), b"complete old check").unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if std::fs::read_dir(&scratch).unwrap().any(|item| {
                item.unwrap()
                    .path()
                    .extension()
                    .is_some_and(|s| s == "json")
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wait_checker_record(&store, &["Verified"]).await;
    let job = wait_landing_phase(&mut client, &input, l::Phase::PreparedPendingChecks).await;
    assert!(job.receipt.is_none());
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn a_bare_checked_state_does_not_authorize_coordinator_finalization() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, config, input, scratch) = checked_landing_setup("pass");
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(true),
        false,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    running.stop().await;
    drop(client);
    let pending = store
        .scan_pending_landings("alpha", None, Default::default())
        .unwrap()
        .jobs
        .pop()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    pending.handle.prepare(&store, now).unwrap();
    let lease = pending.handle.pin_candidate(&store, now).unwrap();
    pending
        .handle
        .record_check(
            &store,
            lease.identity(),
            tinytree_store::LandingVerdict {
                passed: true,
                evidence: [9; 32],
            },
            now,
        )
        .unwrap();
    drop(lease);
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    let job = wait_landing_phase(&mut client, &input, l::Phase::Checked).await;
    assert!(job.receipt.is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    assert!(
        !scratch.exists(),
        "no coordinator receipt means no unchecked finalize or replacement check"
    );
    running.stop().await;
}

#[tokio::test]
async fn unpublished_reservation_prefix_is_recovered_but_foreign_data_is_preserved() {
    for foreign in [false, true] {
        let (_temporary, store, config, _input, _scratch) = checked_landing_setup("pass");
        let root = store.enable_external_checks().unwrap();
        let staging =
            root.join("jobs")
                .join(format!(".pending-{}-{}", "11".repeat(32), "22".repeat(32)));
        let directory = private_fs::PrivateDir::create(&staging).unwrap();
        let child = if foreign {
            "foreign.txt"
        } else {
            ".private-1-1.tmp"
        };
        std::fs::write(directory.path().join(child), b"partial reservation").unwrap();
        drop(directory);
        let result = RepositoryService::new(
            store,
            config.service,
            vec!["alpha".into(), "beta".into()],
            landing_principals(true),
        );
        if foreign {
            assert!(result.is_err());
            assert_eq!(
                std::fs::read(staging.join(child)).unwrap(),
                b"partial reservation"
            );
        } else {
            assert!(result.is_ok());
            assert!(!staging.exists());
        }
    }
}

#[tokio::test]
async fn disabled_master_never_activates_fresh_checker_layout_on_open_or_reopen() {
    let (temporary, store, mut config, _input, _scratch) = checked_landing_setup("pass");
    config.service.landing.enabled = false;
    config.service.landing.max_buffered_bytes = 512 * 1024 * 1024;
    let service = RepositoryService::new(
        store.clone(),
        config.service.clone(),
        vec!["alpha".into(), "beta".into()],
        landing_principals(true),
    )
    .unwrap();
    assert!(store.external_checks_directory().unwrap().is_none());
    drop(service);
    drop(store);
    let store = Store::open_existing(
        temporary.path(),
        config.store.materialize(config.service.max_metadata_bytes),
    )
    .unwrap();
    let _service = RepositoryService::new(
        store.clone(),
        config.service,
        vec!["alpha".into(), "beta".into()],
        landing_principals(true),
    )
    .unwrap();
    assert!(store.external_checks_directory().unwrap().is_none());
}

#[tokio::test]
async fn different_checked_evidence_cannot_be_replaced_by_a_later_runner_receipt() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, config, input, scratch) = checked_landing_setup("pause");
    let running = Running::start_landing(
        store.clone(),
        config.service.clone(),
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !scratch.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pending = store
        .scan_pending_landings("alpha", None, Default::default())
        .unwrap()
        .jobs
        .into_iter()
        .find(|j| j.state == tinytree_store::LandingState::Prepared)
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let lease = pending.handle.pin_candidate(&store, now).unwrap();
    pending
        .handle
        .record_check(
            &store,
            lease.identity(),
            tinytree_store::LandingVerdict {
                passed: true,
                evidence: [9; 32],
            },
            now,
        )
        .unwrap();
    drop(lease);
    std::fs::write(scratch.join("release"), b"complete differently bound check").unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !std::fs::read_dir(&scratch).unwrap().any(|item| {
            item.unwrap()
                .path()
                .extension()
                .is_some_and(|s| s == "json")
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wait_checker_record(&store, &["Verified"]).await;
    running.stop().await;
    drop(client);
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    let job = wait_landing_phase(&mut client, &input, l::Phase::Checked).await;
    assert!(job.receipt.is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    assert_eq!(
        std::fs::read_to_string(scratch.join("executions"))
            .unwrap()
            .lines()
            .filter(|line| *line == "run")
            .count(),
        1
    );
    running.stop().await;
}

// The private journal is the durable consumption witness; verdict assertions
// remain on the public RPC/reference state, not on a fabricated callback.
async fn wait_checker_record(store: &Store, phases: &[&str]) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let jobs = store
                .external_checks_directory()
                .unwrap()
                .unwrap()
                .join("jobs");
            for entry in std::fs::read_dir(&jobs).unwrap() {
                let path = entry.unwrap().path().join("state.json");
                if let Ok(bytes) = std::fs::read(path) {
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        if value["record"]["phase"]
                            .as_str()
                            .is_some_and(|phase| phases.contains(&phase))
                        {
                            return value["record"].clone();
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn a_late_passing_receipt_after_transport_timeout_is_cleanup_only() {
    use tinytree_proto::landing_v1 as l;
    let (_temporary, store, mut config, input, _scratch) = checked_landing_setup("late_pass");
    let checker = config.service.landing.checker.as_mut().unwrap();
    checker.execution_timeout_seconds = 10;
    checker.max_attempts = 1;
    let running = Running::start_landing(
        store.clone(),
        config.service,
        landing_principals(true),
        true,
    )
    .await;
    let mut client = running.landing_client().await;
    client
        .start_landing(request(input.clone(), ALPHA))
        .await
        .unwrap();
    let record = wait_checker_record(&store, &["RetryReady", "Terminal"]).await;
    assert_eq!(record["verdict_eligible"], false);
    assert_eq!(record["receipt"]["outcome"], "passed");
    assert_eq!(record["receipt"]["cleanup_confirmed"], true);
    let job = wait_landing_phase(&mut client, &input, l::Phase::PreparedPendingChecks).await;
    assert!(job.receipt.is_none());
    assert_eq!(
        store.references("alpha", &["main".into()]).unwrap()[0].generation,
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn browser_http_and_native_grpc_share_the_listener() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (_temporary, store) = setup();
    let running = Running::start(store, ServiceConfig::default()).await;
    let mut client = running.client().await;
    let view = client
        .open_repository(request(
            p::OpenRepositoryRequest {
                repository_id: "alpha".into(),
                reference_names: Vec::new(),
            },
            READER,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(view.repository_id, "alpha");
    let body = "{}";
    let mut stream = tokio::net::TcpStream::connect(running.address)
        .await
        .unwrap();
    let request = format!(
        "POST /ui/api/repositories HTTP/1.1\r\nHost: {}\r\nOrigin: http://{}\r\nAuthorization: Bearer {READER}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        running.address,
        running.address,
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut remaining = body;
        let mut decoded = String::new();
        loop {
            let (size, rest) = remaining.split_once("\r\n").unwrap();
            let size = usize::from_str_radix(size.split(';').next().unwrap(), 16).unwrap();
            if size == 0 {
                break;
            }
            decoded.push_str(&rest[..size]);
            remaining = rest[size..].strip_prefix("\r\n").unwrap();
        }
        decoded
    } else {
        body.to_owned()
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"repositories":["alpha"]})
    );
    let mut stream = tokio::net::TcpStream::connect(running.address)
        .await
        .unwrap();
    stream
        .write_all(
            format!(
                "GET /ui/ HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                running.address
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("text/html"));
    assert!(response.contains("content-security-policy:"));
    drop(client);
    running.stop().await;
}
