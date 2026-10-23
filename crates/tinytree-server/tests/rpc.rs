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
            Principal::new("alpha", ALPHA, vec!["alpha".into()], true).unwrap(),
            Principal::new("beta", BETA, vec!["beta".into()], true).unwrap(),
            Principal::new("reader", READER, vec!["alpha".into()], false)
                .unwrap()
                .with_retention(true),
            Principal::new("peer", peer_token, vec!["alpha".into()], true)
                .unwrap()
                .with_retention(true),
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
    }];
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    config.principals.clear();
    config.repositories = vec!["con".into()];
    assert!(serve(config.clone(), std::future::pending()).await.is_err());
    assert!(!config.storage.exists());
    config.repositories = vec!["default".into()];
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
        Code::Unimplemented
    );
    assert_eq!(
        client
            .evaluate(request(
                q::EvaluateRequest {
                    scope: Some(key.clone()),
                    ..Default::default()
                },
                ALPHA
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
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
