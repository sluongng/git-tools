//! Real commands in separate processes, with no Git executable on PATH.

#[cfg(feature = "forge-cli")]
mod forge_product {
    use super::*;

    fn client() -> Client {
        let mut value = Client::new();
        value.binary = Path::new(env!("CARGO_BIN_EXE_tt")).canonicalize().unwrap();
        value.extra_env.push((
            "FORGE_TEST_TOKEN",
            "public-forge-fixture-token-not-a-secret",
        ));
        value
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forge_product_bootstrap_streams_verified_native_bytes_and_resumes_one_private_intent() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use std::sync::{Arc, Mutex};
        use tinytree_model as m;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Source is a real Store/native RPC service. Destination below is an
        // explicit transport/assertion fixture; actual owner/SQLite/R2 bootstrap
        // authority is independently exercised in the workerd runtime suite.
        let directory = tempfile::tempdir().unwrap();
        let store =
            tinytree_store::Store::open(directory.path().join("source"), Default::default()).unwrap();
        store.create_repository("source").unwrap();
        let body = vec![b'Z'; 1024 * 1024];
        let file = m::describe_file(&body);
        let file_receipt = store.put("source", file, &body, 0).unwrap();
        let tree = m::encode(
            &m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: "large.bin".into(),
                    value: m::TreeValue::File {
                        id: m::FileId(file.key.id.0),
                        executable: false,
                        size_bytes: file.size_bytes,
                        copy_id: None,
                    },
                }],
            }),
            Default::default(),
        )
        .unwrap();
        let tree_receipt = store
            .put("source", tree.descriptor, &tree.bytes, 0)
            .unwrap();
        let person = m::Signature {
            name: "Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: m::Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        };
        let commit = m::encode(
            &m::Object::Commit(m::Commit {
                parents: vec![m::CommitId::ROOT],
                predecessors: vec![],
                root_tree: m::TreeMerge {
                    terms: vec![m::TreeId(tree.descriptor.key.id.0)],
                    labels: vec![String::new()],
                },
                change_id: m::ChangeId([88; 16]),
                description: "Bootstrap fixture".into(),
                author: person.clone(),
                committer: person,
                secure_signature: None,
            }),
            Default::default(),
        )
        .unwrap();
        let commit_receipt = store
            .put("source", commit.descriptor, &commit.bytes, 0)
            .unwrap();
        store
            .publish(
                "source",
                &tinytree_store::PublishRequest {
                    idempotency_key: "source-seed".into(),
                    reference_name: "main".into(),
                    expected_generation: 0,
                    new_commit: Some(m::CommitId(commit.descriptor.key.id.0)),
                    staging_leases: vec![
                        file_receipt.staging_lease,
                        tree_receipt.staging_lease,
                        commit_receipt.staging_lease,
                    ],
                },
                1,
            )
            .unwrap();
        let source_control = store.clone();
        let source_commit = m::CommitId(commit.descriptor.key.id.0);
        let native = tinytree_server::RepositoryService::new(
            store,
            Default::default(),
            vec!["source".into()],
            vec![
                tinytree_server::Principal::new(
                    "fixture",
                    "public-forge-fixture-token-not-a-secret",
                    vec!["source".into()],
                    false,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_address = listener.local_addr().unwrap();
        let source_endpoint = format!("http://{source_address}");
        let source_restart = native.clone();
        let (source_stop, source_stopped) = tokio::sync::oneshot::channel();
        let stop_source_on_finish = Arc::new(Mutex::new(Some(source_stop)));
        let stop_source = stop_source_on_finish.clone();
        let source_task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, native, None, async {
                let _ = source_stopped.await;
            })
            .await
            .unwrap();
        });
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", http.local_addr().unwrap());
        let head = fixture_hex(&commit.descriptor.key.id.0);
        let objects = [
            (file, body),
            (tree.descriptor, tree.bytes),
            (commit.descriptor, commit.bytes),
        ]
        .into_iter()
        .map(|(d, b)| ((d.key.kind as u8, fixture_hex(&d.key.id.0)), (d, b)))
        .collect::<std::collections::HashMap<_, _>>();
        let state = Arc::new(Mutex::new(
            serde_json::json!({"inputs":[],"uploaded":[],"begin":null,"deny_status":true,"ready":false,"finished":false,"put_lost":false,"finish_lost":false,"finishes":0}),
        ));
        let observed = state.clone();
        let (http_stop, mut http_stopped) = tokio::sync::oneshot::channel();
        let http_task = tokio::spawn(async move {
            loop {
                let (mut socket, _) =
                    tokio::select! {value=http.accept()=>value.unwrap(),_=&mut http_stopped=>break};
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    if socket.read_exact(&mut byte).await.is_err() {
                        break;
                    }
                    headers.push(byte[0]);
                    assert!(headers.len() < 16384);
                }
                let headers = String::from_utf8(headers).unwrap().to_ascii_lowercase();
                assert!(headers.starts_with("post /api/v1/forge/call http/1.1\r\n"));
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!(length <= 2 * 1024 * 1024);
                let mut bytes = vec![0; length];
                socket.read_exact(&mut bytes).await.unwrap();
                let input: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                let payload: serde_json::Value =
                    serde_json::from_str(input["payload_json"].as_str().unwrap()).unwrap();
                let (status, result, drop_response) = {
                    let mut state = observed.lock().unwrap();
                    state["inputs"].as_array_mut().unwrap().push(input.clone());
                    let import = "12345678-1234-4234-8234-123456789abc";
                    let job = "22345678-1234-4234-8234-123456789abc";
                    let root = serde_json::json!({"kind":3,"id":head});
                    let source = serde_json::json!({"repository_id":"source","reference":"main","generation":1,"head":head});
                    let destination = serde_json::json!({"generation":1,"head":"aa".repeat(32)});
                    let ready = state["ready"] == true;
                    let progress = serde_json::json!({"job_id":job,"root":root,"phase":if ready {"ready"}else{"graph"},"generation":if ready {1}else{0},"complete":ready,"error":null,"counts":{"objects":if ready {3}else{0},"bytes":if ready {1048576}else{0},"edges":0,"contexts":0,"context_path_bytes":0,"obligations":0,"checked_objects":if ready {3}else{0},"checked_contexts":0,"checked_obligations":0}});
                    let receipt = serde_json::json!({"import_id":import,"closure_job_id":job,"root":root,"declared_source":source,"policy_epoch":1,"main":{"head":head,"generation":2},"bootstrap_sealed":true});
                    match input["operation"].as_u64().unwrap() {
                        2 => (
                            200,
                            serde_json::json!({"account_id":"example","monorepo_id":"forge-example","policy_epoch":1,"public_view":null,"main_revision":if state["finished"]==true {fixture_id::<32>(&head).to_vec()}else{vec![0xaau8;32]},"main_generation":if state["finished"]==true {2}else{1}}),
                            false,
                        ),
                        40 => {
                            assert_eq!(payload["source"], source);
                            assert_eq!(payload["destination"], destination);
                            if !state["begin"].is_null() {
                                assert_eq!(state["begin"], input);
                            }
                            state["begin"] = input.clone();
                            (
                                200,
                                serde_json::json!({"import_id":import,"closure_job_id":job,"root":root,"declared_source":source,"destination":destination,"policy_epoch":1,"status":progress,"limits":{"object_bytes":1048576,"metadata_bytes":262144,"account_objects":32768,"account_bytes":536870912,"attempts":4}}),
                                false,
                            )
                        }
                        41 => {
                            let kind = payload["descriptor"]["kind"].as_u64().unwrap() as u8;
                            let id = payload["descriptor"]["id"].as_str().unwrap().to_owned();
                            let (d, expected) = objects.get(&(kind, id.clone())).unwrap();
                            let bytes = STANDARD
                                .decode(payload["data_base64"].as_str().unwrap())
                                .unwrap();
                            assert_eq!(&bytes, expected);
                            m::verify(d, &bytes, Default::default()).unwrap();
                            assert_eq!(input["request_id"], format!("io-{import}-{kind}-{id}"));
                            let typed = serde_json::json!([kind, id]);
                            if !state["uploaded"].as_array().unwrap().contains(&typed) {
                                state["uploaded"].as_array_mut().unwrap().push(typed);
                            }
                            let lost = kind == 1 && state["put_lost"] == false;
                            if lost {
                                state["put_lost"] = serde_json::json!(true);
                            }
                            (
                                200,
                                serde_json::json!({"import_id":import,"kind":kind,"id":id,"size_bytes":d.size_bytes,"format_version":1,"state":"ready"}),
                                lost,
                            )
                        }
                        43 => {
                            if state["uploaded"].as_array().unwrap().len() == 3
                                && state["deny_status"] == true
                            {
                                state["deny_status"] = serde_json::json!(false);
                                (
                                    403,
                                    serde_json::json!({"version":1,"error":{"code":"permission_denied","message":"fixture interruption"}}),
                                    false,
                                )
                            } else if state["finished"] == true {
                                (
                                    200,
                                    serde_json::json!({"import_id":import,"state":"published","receipt":receipt}),
                                    false,
                                )
                            } else {
                                (
                                    200,
                                    serde_json::json!({"import_id":import,"root":root,"declared_source":source,"destination":destination,"policy_epoch":1,"obsolete":false,"bootstrap_sealed":false,"status":progress}),
                                    false,
                                )
                            }
                        }
                        42 => {
                            assert_eq!(input["request_id"], format!("ia-{import}-0"));
                            state["ready"] = serde_json::json!(true);
                            (
                                200,
                                serde_json::json!({"import_id":import,"status":progress}),
                                false,
                            )
                        }
                        44 => {
                            assert_eq!(payload["expected_generation"], 1);
                            if state["finished"] != true {
                                if let Some(stop) = stop_source.lock().unwrap().take() {
                                    let _ = stop.send(());
                                }
                                state["finished"] = serde_json::json!(true);
                                state["finishes"] =
                                    serde_json::json!(state["finishes"].as_u64().unwrap() + 1);
                            }
                            let lost = state["finish_lost"] == false;
                            state["finish_lost"] = serde_json::json!(true);
                            if lost {
                                (
                                    503,
                                    serde_json::json!({"version":1,"error":{"code":"unavailable","message":"fixture lost finish acknowledgement"}}),
                                    false,
                                )
                            } else {
                                (200, receipt, false)
                            }
                        }
                        other => panic!("unexpected bootstrap opcode {other}"),
                    }
                };
                if drop_response {
                    continue;
                }
                let reply = if status == 200 {
                    serde_json::json!({"version":1,"operation":input["operation"],"account_id":input["account_id"],"view":input["view"],"request_id":input["request_id"],"state":"complete","result_json":result.to_string()})
                } else {
                    result
                };
                let bytes = serde_json::to_vec(&reply).unwrap();
                socket.write_all(format!("HTTP/1.1 {status} Fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",bytes.len()).as_bytes()).await.unwrap();
                let _ = socket.write_all(&bytes).await;
            }
        });
        let client = client();
        let source_config = client.root.join("source.toml");
        let mut native_config = tinytree_client::Config {
            endpoint: source_endpoint,
            repository_id: "source".into(),
            token_env: Some("FORGE_TEST_TOKEN".into()),
            compression: false,
            ..Default::default()
        };
        native_config.rpc.operation_timeout_ms = 2000;
        native_config.rpc.rpc_timeout_ms = 1000;
        native_config.rpc.connect_timeout_ms = 500;
        fs::write(&source_config, toml::to_string(&native_config).unwrap()).unwrap();
        client.success(&[
            "context",
            "set",
            "--transport",
            "forge-https-v1",
            "--server",
            &endpoint,
            "--account",
            "example",
            "--token-env",
            "FORGE_TEST_TOKEN",
        ]);
        let args = [
            "account",
            "bootstrap-import",
            "--source-config",
            source_config.to_str().unwrap(),
            "--source-reference",
            "main",
            "--source-writes-paused",
            "--json",
        ];
        let first = client.run(&args);
        assert!(
            !first.status.success(),
            "first status read deliberately denies after uploads"
        );
        let path = client.home.join("config/tinytree/bootstrap-import.json");
        let saved = fs::read(&path).unwrap();
        let intent: serde_json::Value = serde_json::from_slice(&saved).unwrap();
        assert_eq!(intent["transfer_complete"], true);
        assert!(saved.len() < 128 * 1024);
        assert!(!String::from_utf8_lossy(&saved).contains("data_base64"));
        assert!(!client.home.join("config/tinytree/requests").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let mut resume = args.to_vec();
        resume.push("--resume");
        let unknown = client.run(&resume);
        assert!(!unknown.status.success());
        assert!(
            String::from_utf8_lossy(&unknown.stderr).contains("source_readback_unknown_after_publish")
                || String::from_utf8_lossy(&unknown.stdout)
                    .contains("source_readback_unknown_after_publish")
        );
        let uncertain: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(!uncertain["receipt"].is_null());
        assert_eq!(
            uncertain["source_changed_after_publish"], false,
            "unavailable is not observed main movement"
        );
        source_task.await.unwrap();
        let listener = tokio::net::TcpListener::bind(source_address).await.unwrap();
        let (restart_stop, restart_stopped) = tokio::sync::oneshot::channel();
        let restarted = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, source_restart, None, async {
                let _ = restart_stopped.await;
            })
            .await
            .unwrap();
        });
        let complete: serde_json::Value = serde_json::from_str(&client.success(&resume)).unwrap();
        assert_eq!(complete["state"], "complete");
        assert_eq!(
            complete["result"]["source_exclusivity"],
            "operator_asserted_not_verified"
        );
        // Trusted fixture independently advances the REAL source ref. Unlike
        // the prior unavailable read, this observed generation mismatch is a
        // terminal partial outcome after destination publication.
        source_control.publish("source", &tinytree_store::PublishRequest {
            idempotency_key: "source-advance-after-import".into(), reference_name: "main".into(),
            expected_generation: 1, new_commit: Some(source_commit), staging_leases: vec![],
        }, 2).unwrap();
        let changed = client.run(&resume);
        assert!(!changed.status.success());
        assert!(String::from_utf8_lossy(&changed.stderr).contains("source_changed_after_publish") || String::from_utf8_lossy(&changed.stdout).contains("source_changed_after_publish"));
        let changed_journal: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(changed_journal["source_changed_after_publish"], true);
        let seen = state.lock().unwrap();
        assert_eq!(seen["uploaded"].as_array().unwrap().len(), 3);
        assert_eq!(seen["finishes"], 1);
        let puts = seen["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|value| value["operation"] == 41)
            .collect::<Vec<_>>();
        assert_eq!(puts.len(), 4);
        let file_puts = puts
            .iter()
            .filter(|value| {
                serde_json::from_str::<serde_json::Value>(value["payload_json"].as_str().unwrap())
                    .unwrap()["descriptor"]["kind"]
                    == 1
            })
            .collect::<Vec<_>>();
        assert_eq!(file_puts.len(), 2);
        assert_eq!(
            file_puts[0], file_puts[1],
            "lost object ACK replays exact key and original1MiB bytes"
        );
        let finishes = seen["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|value| value["operation"] == 44)
            .collect::<Vec<_>>();
        assert_eq!(finishes.len(), 2);
        assert_eq!(finishes[0], finishes[1]);
        drop(seen);
        http_stop.send(()).unwrap();
        restart_stop.send(()).unwrap();
        http_task.await.unwrap();
        restarted.await.unwrap();
    }

    #[test]
    fn forge_product_context_is_private_machine_readable_and_never_a_login_claim() {
        let client = client();
        let help = client.success(&["--help"]);
        assert!(help.contains("Tinytree VCS and code review"));
        let result: serde_json::Value = serde_json::from_str(&client.success(&[
            "context",
            "set",
            "--server",
            "http://127.0.0.1:8093",
            "--account",
            "example",
            "--view",
            "public",
            "--token-env",
            "FORGE_TEST_TOKEN",
            "--json",
        ]))
        .unwrap();
        assert_eq!(result["schema_version"], 1);
        assert_eq!(result["state"], "configured");
        assert_eq!(result["result"]["account_id"], "example");
        assert_eq!(result["result"]["token_env"], "FORGE_TEST_TOKEN");
        let shown: serde_json::Value =
            serde_json::from_str(&client.success(&["--json", "context", "show"])).unwrap();
        assert_eq!(shown, result);
        let path = client.home.join("config/tinytree/context.json");
        let saved: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved, result["result"]);
        assert!(saved.get("transport").is_none(), "default gRPC encoding stays unchanged");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forge_product_explicit_http_context_binds_login_and_saved_requests() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Transport assertion fixture; canonical account semantics are covered
        // by the existing real Store/service workflow in this same suite.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let received = requests.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = tokio::select! {
                    result = listener.accept() => result.unwrap(),
                    _ = &mut stopped => break,
                };
                let received = received.clone();
                connections.spawn(async move {
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        socket.read_exact(&mut byte).await.unwrap();
                        headers.push(byte[0]);
                        assert!(headers.len() < 16384);
                    }
                    let headers = String::from_utf8(headers).unwrap().to_ascii_lowercase();
                    assert!(headers.starts_with("post /api/v1/forge/call http/1.1\r\n"));
                    assert!(headers.contains("authorization: bearer public-forge-fixture-token-not-a-secret\r\n"));
                    let size: usize = headers.lines().find_map(|line| line.strip_prefix("content-length: ")).unwrap().parse().unwrap();
                    assert!(size < 2 * 1024 * 1024);
                    let mut bytes = vec![0; size];
                    socket.read_exact(&mut bytes).await.unwrap();
                    let input: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(input["version"], 1);
                    assert_eq!(input["view"], "public");
                    assert_eq!(input["payload_json"], "{}");
                    received.lock().unwrap().push(input.clone());
                    let denied = input["account_id"] == "missing";
                    let result = match input["account_id"].as_str().unwrap() {
                        "malformed" => "not-json".to_owned(),
                        "invalid-shape" => "{}".to_owned(),
                        requested => {
                            let account = if requested == "wrong-account" { "other" } else { requested };
                            serde_json::json!({"account_id":account,"monorepo_id":format!("forge-{account}"),"policy_epoch":1,"public_view":null,"main_revision":vec![1u8;32],"main_generation":1}).to_string()
                        }
                    };
                    let output = if denied {
                        serde_json::json!({"version":1,"error":{"code":"permission_denied","message":"fixture account denied"}})
                    } else {
                        serde_json::json!({"version":1,"operation":input["operation"],"account_id":input["account_id"],"view":input["view"],"request_id":input["request_id"],"state":"complete","result_json":result})
                    };
                    let output = serde_json::to_vec(&output).unwrap();
                    socket.write_all(format!("HTTP/1.1 {} Fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",if denied {403}else{200},output.len()).as_bytes()).await.unwrap();
                    socket.write_all(&output).await.unwrap();
                });
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        let client = client();
        let logged_in = service_value(
            &client,
            &[
                "login",
                "--transport",
                "forge-https-v1",
                "--server",
                &endpoint,
                "--account",
                "example",
                "--token-env",
                "FORGE_TEST_TOKEN",
                "--json",
            ],
        );
        assert_eq!(logged_in["account_id"], "example");
        let context_path = client.home.join("config/tinytree/context.json");
        let context_bytes = fs::read(&context_path).unwrap();
        let context: serde_json::Value = serde_json::from_slice(&context_bytes).unwrap();
        assert_eq!(context["transport"], "forge_https_v1");
        assert_eq!(context["token_env"], "FORGE_TEST_TOKEN");
        assert!(!String::from_utf8_lossy(&context_bytes).contains("public-forge-fixture-token"));
        assert!(
            client
                .success(&["context", "show"])
                .contains("Transport: forge-https-v1")
        );
        assert_eq!(
            service_value(&client, &["account", "show", "--json"])["account_id"],
            "example"
        );
        let provision = service_value(
            &client,
            &[
                "account",
                "provision",
                "--request-id",
                "http-provision",
                "--json",
            ],
        );
        let pending_path = client
            .home
            .join("config/tinytree/requests/request-http-provision.json");
        let pending_bytes = fs::read(&pending_path).unwrap();
        let pending: serde_json::Value = serde_json::from_slice(&pending_bytes).unwrap();
        assert_eq!(pending["context"]["transport"], "forge_https_v1");
        assert_eq!(requests.lock().unwrap().len(), 3);
        // Same endpoint/account/key with another protocol is a different intent.
        client.success(&[
            "context",
            "set",
            "--transport",
            "grpc",
            "--server",
            &endpoint,
            "--account",
            "example",
            "--token-env",
            "FORGE_TEST_TOKEN",
        ]);
        let rebound = client.run(&["request", "resume", "http-provision", "--json"]);
        assert!(!rebound.status.success());
        assert_eq!(
            requests.lock().unwrap().len(),
            3,
            "context mismatch must fail before dispatch"
        );
        assert_eq!(fs::read(&pending_path).unwrap(), pending_bytes);
        let grpc: serde_json::Value =
            serde_json::from_slice(&fs::read(&context_path).unwrap()).unwrap();
        assert!(
            grpc.get("transport").is_none(),
            "retain existing default-gRPC scope hash encoding"
        );
        client.success(&[
            "context",
            "set",
            "--transport",
            "forge-https-v1",
            "--server",
            &endpoint,
            "--account",
            "example",
            "--token-env",
            "FORGE_TEST_TOKEN",
        ]);
        assert_eq!(
            service_value(&client, &["request", "resume", "http-provision", "--json"]),
            provision
        );
        assert_eq!(fs::read(&pending_path).unwrap(), pending_bytes);
        let before = fs::read(&context_path).unwrap();
        assert!(
            !client
                .run(&[
                    "login",
                    "--transport",
                    "forge-https-v1",
                    "--server",
                    &endpoint,
                    "--account",
                    "missing",
                    "--token-env",
                    "FORGE_TEST_TOKEN",
                    "--json"
                ])
                .status
                .success()
        );
        assert_eq!(
            fs::read(&context_path).unwrap(),
            before,
            "failed HTTP login preserves context"
        );
        assert!(
            !client
                .run(&[
                    "context",
                    "set",
                    "--transport",
                    "https",
                    "--server",
                    &endpoint,
                    "--account",
                    "example"
                ])
                .status
                .success()
        );
        assert_eq!(fs::read(&context_path).unwrap(), before);
        // Valid transport bindings cannot make malformed or another account's
        // semantic snapshot into a successfully persisted login.
        for account in ["malformed", "invalid-shape", "wrong-account"] {
            let rejected = client.run(&[
                "login",
                "--transport",
                "forge-https-v1",
                "--server",
                &endpoint,
                "--account",
                account,
                "--token-env",
                "FORGE_TEST_TOKEN",
                "--json",
            ]);
            assert!(
                !rejected.status.success(),
                "invalid login snapshot: {account}"
            );
            assert_eq!(
                fs::read(&context_path).unwrap(),
                before,
                "invalid snapshot must preserve the previous context: {account}"
            );
        }
        let received = requests.lock().unwrap();
        assert_eq!(received.len(), 8);
        assert_eq!(
            received[2], received[3],
            "resume uses the exact HTTP mutation envelope"
        );
        drop(received);
        stop.send(()).unwrap();
        server.await.unwrap();
    }

    #[test]
    fn forge_product_rejects_credential_urls_and_reports_workspace_failure_as_json() {
        let client = client();
        let failed = client.run(&[
            "context",
            "set",
            "--server",
            "https://user:secret@example.invalid",
            "--account",
            "example",
            "--json",
        ]);
        assert!(!failed.status.success());
        let report: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
        assert_eq!(report["state"], "failed");
        assert!(!String::from_utf8_lossy(&failed.stdout).contains("secret"));
        assert!(!client.home.join("config/tinytree/context.json").exists());
        let failed = client.run(&["status", "--json"]);
        assert!(!failed.status.success());
        let report: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
        assert_eq!(report["operation"], "status");
        assert_eq!(report["state"], "failed");
    }

    #[test]
    fn forge_product_failed_mutation_keeps_exact_replay_identity() {
        let client = client();
        // Reserve and close a loopback port: no fixture service or credentials.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        client.success(&[
            "context",
            "set",
            "--server",
            &endpoint,
            "--account",
            "example",
        ]);
        let failed = client.run(&[
            "account",
            "provision",
            "--request-id",
            "provision-example",
            "--json",
        ]);
        assert!(!failed.status.success());
        let report: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
        assert_eq!(report["state"], "uncertain");
        assert_eq!(report["request_id"], "provision-example");
        let path = client
            .home
            .join("config/tinytree/requests/request-provision-example.json");
        let original = fs::read(&path).unwrap();
        let shown: serde_json::Value = serde_json::from_str(&client.success(&[
            "request",
            "show",
            "provision-example",
            "--json",
        ]))
        .unwrap();
        assert_eq!(shown["state"], "saved");
        let resumed = client.run(&["request", "resume", "provision-example", "--json"]);
        assert!(!resumed.status.success());
        let report: serde_json::Value = serde_json::from_slice(&resumed.stdout).unwrap();
        assert_eq!(report["request_id"], "provision-example");
        assert_eq!(fs::read(&path).unwrap(), original);
        client.success(&[
            "context",
            "set",
            "--server",
            &endpoint,
            "--account",
            "different",
        ]);
        let rebound = client.run(&["request", "resume", "provision-example", "--json"]);
        assert!(!rebound.status.success());
        assert_eq!(fs::read(&path).unwrap(), original);
    }

    #[test]
    fn forge_product_push_saves_review_intent_and_rejects_changed_inputs() {
        let client = client();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        client.success(&[
            "context",
            "set",
            "--server",
            &endpoint,
            "--account",
            "example",
        ]);
        let base = "11".repeat(32);
        let head = "22".repeat(32);
        let view = "33".repeat(32);
        let pushed = client.run(&[
            "push",
            "--source-base",
            &base,
            "--source-head",
            &head,
            "--view-version",
            &view,
            "--request-id",
            "review-example",
            "--json",
        ]);
        assert!(!pushed.status.success());
        let output: serde_json::Value = serde_json::from_slice(&pushed.stdout).unwrap();
        assert_eq!(output["state"], "uncertain");
        let path = client
            .home
            .join("config/tinytree/requests/request-review-example.json");
        let original = fs::read(&path).unwrap();
        let intent: serde_json::Value = serde_json::from_slice(&original).unwrap();
        assert_eq!(intent["payload"]["source_head"], head);
        assert_eq!(intent["payload"]["source_base"], base);
        assert!(intent["payload"].get("actor").is_none());
        assert!(intent["payload"].get("check_profile").is_none());
        let changed = "44".repeat(32);
        let rebound = client.run(&[
            "push",
            "--source-base",
            &base,
            "--source-head",
            &changed,
            "--view-version",
            &view,
            "--request-id",
            "review-example",
            "--json",
        ]);
        assert!(!rebound.status.success());
        let output: serde_json::Value = serde_json::from_slice(&rebound.stdout).unwrap();
        assert_eq!(output["state"], "failed");
        assert_eq!(fs::read(&path).unwrap(), original);
        // Sending immutable review IDs never creates or changes a local workspace.
        assert!(!client.root.join(".jj").exists());
    }

    #[test]
    fn forge_product_rejects_authority_hints_before_saving_transform_requests() {
        let client = client();
        client.success(&[
            "context",
            "set",
            "--server",
            "http://127.0.0.1:8093",
            "--account",
            "example",
        ]);
        let path = client.home.join("plan.json");
        let plan = serde_json::json!({
            "source_account":"upstream", "source_view":"public",
            "source_before_view":"11".repeat(32), "source_after_view":"22".repeat(32),
            "spec":{"mappings":[{"source_prefix":"lib","destination_prefix":"vendor/lib"}]},
            "change_id":"33".repeat(16),
            "actor":"owner"
        });
        fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
        let failed = client.run(&[
            "transform",
            "start",
            "--plan",
            path.to_str().unwrap(),
            "--request-id",
            "transform-example",
            "--json",
        ]);
        assert!(!failed.status.success());
        let report: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
        assert_eq!(report["state"], "failed");
        assert!(
            !client
                .home
                .join("config/tinytree/requests/request-transform-example.json")
                .exists()
        );
        let failed = client.run(&[
            "pin",
            "resolve",
            &"11".repeat(32),
            "--max-bytes",
            "65537",
            "--path",
            "file",
            "--json",
        ]);
        assert!(!failed.status.success());
        let report: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
        assert_eq!(report["state"], "failed");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forge_product_real_service_auth_views_and_chunked_native_projection() {
        use futures::AsyncReadExt as _;
        use jj_native_backend::NativeObjectIo as _;
        use std::{os::unix::fs::PermissionsExt as _, sync::Arc};
        use tinytree_model as m;
        let directory = tempfile::tempdir().unwrap();
        let store =
            tinytree_store::Store::open(directory.path().join("server"), Default::default())
                .unwrap();
        let service = tinytree_server::RepositoryService::new(
            store.clone(),
            Default::default(),
            vec![],
            vec![
                tinytree_server::Principal::new_forge(
                    "owner",
                    "public-forge-fixture-token-not-a-secret",
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        let client = client();
        client.success(&[
            "context",
            "set",
            "--server",
            &endpoint,
            "--account",
            "example",
            "--token-env",
            "FORGE_TEST_TOKEN",
        ]);
        let provision: serde_json::Value = serde_json::from_str(&client.success(&[
            "account",
            "provision",
            "--request-id",
            "provision-service",
            "--json",
        ]))
        .unwrap();
        assert_eq!(provision["result"]["value"]["monorepo_id"], "forge-example");
        let epoch = provision["result"]["value"]["policy_epoch"]
            .as_u64()
            .unwrap();
        let repo = "forge-example";
        let original = store.references(repo, &["main".into()]).unwrap().remove(0);
        let file = |bytes: &[u8]| {
            let descriptor = m::describe_file(bytes);
            store.put(repo, descriptor, bytes, 0).unwrap();
            m::TreeValue::File {
                id: m::FileId(descriptor.key.id.0),
                executable: false,
                size_bytes: bytes.len() as u64,
                copy_id: None,
            }
        };
        let put = |object: m::Object| {
            let encoded = m::encode(&object, Default::default()).unwrap();
            store
                .put(repo, encoded.descriptor, &encoded.bytes, 0)
                .unwrap();
            encoded.descriptor
        };
        let large = vec![b'Z'; 1024 * 1024 + 37];
        let large_value = file(&large);
        let m::TreeValue::File { id: large_id, .. } = large_value else {
            unreachable!()
        };
        let private_value = file(b"private-sentinel-do-not-disclose");
        let m::TreeValue::File { id: private_id, .. } = private_value else {
            unreachable!()
        };
        let app = put(m::Object::Tree(m::Tree {
            entries: vec![
                m::TreeEntry {
                    name: "README.md".into(),
                    value: file(b"# Example\nhello public code"),
                },
                m::TreeEntry {
                    name: "large.txt".into(),
                    value: large_value,
                },
                m::TreeEntry {
                    name: "main.rs".into(),
                    value: file(b"fn main() { /* hello public */ }"),
                },
            ],
        }));
        let secrets = put(m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "token.txt".into(),
                value: private_value,
            }],
        }));
        let tree = put(m::Object::Tree(m::Tree {
            entries: vec![
                m::TreeEntry {
                    name: "app".into(),
                    value: m::TreeValue::Tree(m::TreeId(app.key.id.0)),
                },
                m::TreeEntry {
                    name: "secrets".into(),
                    value: m::TreeValue::Tree(m::TreeId(secrets.key.id.0)),
                },
            ],
        }));
        let signature = m::Signature {
            name: "Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: m::Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        };
        let head = put(m::Object::Commit(m::Commit {
            parents: vec![m::CommitId(original.commit.unwrap().key.id.0)],
            predecessors: vec![],
            root_tree: m::TreeMerge {
                terms: vec![m::TreeId(tree.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: m::ChangeId([7; 16]),
            description: "scoped fixture".into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }));
        // An imported immutable snapshot is selected by an authenticated view;
        // the fixture does not bypass checked account-main publication.
        let spec = client.home.join("view.json");
        fs::write(&spec,serde_json::to_vec(&serde_json::json!({"name":"public","root_prefix":"app","include_prefixes":[""],"exclude_prefixes":[],"visibility":{"kind":"public"},"revision":head.key.id.0,"export_allowed":true})).unwrap()).unwrap();
        client.success(&[
            "view",
            "publish",
            "--spec",
            spec.to_str().unwrap(),
            "--expected-epoch",
            &epoch.to_string(),
            "--make-default",
            "--request-id",
            "publish-service",
            "--json",
        ]);
        let shown = client.success(&["view", "show", "--json"]);
        let snapshot: serde_json::Value = serde_json::from_str(&shown).unwrap();
        assert!(shown.contains("hello public code"));
        assert!(!shown.contains("private-sentinel"));
        assert_eq!(
            snapshot["result"]["value"]["view_version"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        let search = client.success(&["search", "hello", "--json"]);
        assert!(search.contains("README.md"));
        let partial: serde_json::Value = serde_json::from_str(&search).unwrap();
        assert_eq!(partial["state"], "incomplete");
        assert_eq!(partial["result"]["value"]["complete"], false);
        assert!(!search.contains("private-sentinel"));
        assert_eq!(partial["result"]["value"]["skipped_files"]["oversized"], 1);
        let first_page: serde_json::Value = serde_json::from_str(&client.success(&["search", "hello", "--limit", "1", "--json"])).unwrap();
        assert_eq!(first_page["state"], "incomplete");
        let pinned_version = first_page["result"]["value"]["view_version"].as_str().unwrap().to_owned();
        let first_cursor = first_page["result"]["value"]["cursor"].as_str().expect("two matching files require continuation at limit1").to_owned();
        let missing_version = client.run(&["search", "hello", "--limit", "1", "--cursor", &first_cursor, "--json"]);
        assert!(!missing_version.status.success(), "continuation requires an explicit immutable version");
        let wrong_query = client.run(&["search", "public", "--limit", "1", "--version", &pinned_version, "--cursor", &first_cursor, "--json"]);
        assert!(!wrong_query.status.success(), "a cursor cannot restart/rebind a different query");
        let excessive_limit = client.run(&["search", "hello", "--limit", "101", "--json"]);
        assert!(!excessive_limit.status.success());
        let mut page = first_page;
        let mut hits = std::collections::BTreeSet::new();
        let mut finished = false;
        for _ in 0..8 {
            let value = &page["result"]["value"];
            assert_eq!(value["view_version"], pinned_version);
            for hit in value["matches"].as_array().unwrap() {
                assert!(hits.insert((hit["path"].as_str().unwrap().to_owned(), hit["line"].as_u64().unwrap(), hit["column"].as_u64().unwrap())), "continuation must not duplicate hits");
            }
            let Some(cursor) = value["cursor"].as_str() else {
                assert_eq!(value["complete"], false, "oversized file makes terminal page explicitly partial");
                assert_eq!(page["state"], "incomplete");
                assert_eq!(value["skipped_files"]["oversized"], 1);
                finished = true;
                break;
            };
            page = serde_json::from_str(&client.success(&["search", "hello", "--limit", "1", "--version", &pinned_version, "--cursor", cursor, "--json"])).unwrap();
        }
        assert!(finished, "bounded cursor walk must terminate");
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|hit| hit.0 == "README.md"));
        assert!(hits.iter().any(|hit| hit.0 == "main.rs"));
        let rejected = client.run(&["view", "read", "../secrets/token.txt", "--json"]);
        assert!(!rejected.status.success());
        assert!(!String::from_utf8_lossy(&rejected.stdout).contains("private-sentinel"));
        let context = fs::read(client.home.join("config/tinytree/context.json")).unwrap();
        let rejected = client.run(&[
            "login",
            "--server",
            &endpoint,
            "--account",
            "missing",
            "--token-env",
            "FORGE_TEST_TOKEN",
            "--json",
        ]);
        assert!(!rejected.status.success());
        assert_eq!(
            fs::read(client.home.join("config/tinytree/context.json")).unwrap(),
            context
        );
        let resolved = store
            .forge_resolve_view(
                &tinytree_store::AccountId("example".into()),
                Some(&tinytree_store::ForgeActor {
                    principal: "owner".into(),
                }),
                "public",
                None,
            )
            .unwrap();
        let path = directory.path().join("local");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let local =
            Arc::new(jj_native_backend::DirectObjectIo::init(&path, Default::default()).unwrap());
        let config = tinytree_client::Config {
            endpoint: endpoint.clone(),
            repository_id: repo.into(),
            token_env: Some("FORGE_TEST_TOKEN".into()),
            ..Default::default()
        };
        let provider = native_broker::ForgeObjectIo::open(
            local.clone(),
            native_broker::ForgeRemoteConfig {
                account_id: "example".into(),
                view: "public".into(),
                view_version: Some(resolved.version().0),
                client: config.clone(),
            },
        )
        .await
        .unwrap();
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(large_id.0),
        };
        assert_eq!(
            provider.descriptor(key).await.unwrap().unwrap().size_bytes,
            large.len() as u64
        );
        assert!(
            local.descriptor(key).await.unwrap().is_none(),
            "descriptor-only request must not fetch file bodies"
        );
        let (_, mut reader) = provider.read_file(large_id).await.unwrap().into_parts();
        let mut body = vec![];
        reader.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, large);
        assert!(provider.read_file(private_id).await.is_err());
        assert!(
            local
                .descriptor(m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(private_id.0)
                })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            provider.read_metadata(head.key).await.is_err(),
            "canonical ancestry must not be a projected object capability"
        );
        let raw = tinytree_client::NativeClient::connect(config)
            .await
            .unwrap();
        assert_eq!(
            raw.open(vec!["main".into()]).await.unwrap_err().code(),
            native_broker::ControlCode::PermissionDenied
        );
        use native_broker::NativeRepositoryControl as _;
        assert_eq!(
            provider
                .publication("not-a-raw-grant")
                .await
                .unwrap_err()
                .code(),
            native_broker::ControlCode::PermissionDenied
        );
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    fn service_value(client: &Client, args: &[&str]) -> serde_json::Value {
        let output = client.success(args);
        let envelope: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(envelope["schema_version"], 1);
        envelope["result"]["value"].clone()
    }
    fn fixture_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    fn fixture_id<const N: usize>(hex: &str) -> [u8; N] {
        assert_eq!(hex.len(), N * 2);
        std::array::from_fn(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap())
    }
    fn select_account(client: &Client, endpoint: &str, account: &str, view: &str, token: &str) {
        client.success(&[
            "context",
            "set",
            "--server",
            endpoint,
            "--account",
            account,
            "--view",
            view,
            "--token-env",
            token,
        ]);
    }
    fn publish_fixture_view(
        client: &Client,
        name: &str,
        revision: [u8; 32],
        include: &[&str],
        public: bool,
        key: &str,
    ) -> String {
        let account = service_value(client, &["account", "show", "--json"]);
        let spec = client.home.join("selected-view.json");
        fs::write(&spec,serde_json::to_vec(&serde_json::json!({"name":name,"root_prefix":"","include_prefixes":include,"exclude_prefixes":[],"visibility":{"kind":if public{"public"}else{"members"}},"revision":revision,"export_allowed":true})).unwrap()).unwrap();
        let mut args = vec![
            "view",
            "publish",
            "--spec",
            spec.to_str().unwrap(),
            "--expected-epoch",
        ];
        let epoch = account["policy_epoch"].as_u64().unwrap().to_string();
        args.extend([&epoch, "--request-id", key, "--json"]);
        if public {
            args.push("--make-default");
        }
        service_value(client, &args);
        service_value(client, &["view", "show", "--json"])["view_version"]
            .as_str()
            .unwrap()
            .into()
    }
    fn protocol_checker_config(
        directory: &Path,
        endpoint: &str,
    ) -> (tinytree_server::ServiceConfig, PathBuf) {
        use sha2::{Digest, Sha256};
        let operator = directory.join("operator");
        fs::create_dir(&operator).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&operator, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let executable = Path::new(env!("TINYTREE_CHECKER_FIXTURE"))
            .canonicalize()
            .unwrap();
        let checksum = format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()));
        let quoted = |p: &Path| serde_json::to_string(p.to_str().unwrap()).unwrap();
        let cache = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let git = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let daemon = operator.join("daemon.toml");
        // Declared, pinned protocol process only: this template is not mounted
        // and its fixture evidence must not be called a production RBE verdict.
        fs::write(
            &daemon,
            format!(
                r#"state_dir={}
[cache]
listen="{}"
upstream="grpc://127.0.0.1:9"
api_key_env="UNUSED_TWO_ACCOUNT_FIXTURE_KEY"
[git_proxy]
listen="{}"
[bes]
enabled=false
[bos]
enabled=false
[native.workspaces.capture]
policy="disabled"
[native.workspaces.upload]
policy="disabled"
[native.repositories.checker-template]
checker_forge_accounts=true
[native.repositories.checker-template.remote]
authorization_profile="trusted-two-account-checker-fixture"
[native.repositories.checker-template.remote.remote]
endpoint="{}"
repository_id="template"
token_env="FORGE_TWO_CHECKER_TOKEN"
"#,
                quoted(&operator.join("daemon-state")),
                cache.local_addr().unwrap(),
                git.local_addr().unwrap(),
                endpoint
            ),
        )
        .unwrap();
        drop((cache, git));
        let scratch = operator.join("checker-state");
        let policy = operator.join("checker.toml");
        fs::write(
            &policy,
            format!(
                r#"scratch={}
daemon_config={}
rbe_socket={}
runner_sha256="{}"
workspace_environment=["FORGE_TWO_CHECKER_TOKEN"]
[workspace]
path={}
sha256="{}"
[bazel]
path={}
sha256="{}"
[[profiles]]
name="checks"
repositories=[]
forge_accounts=true
store="checker-template"
mode="build"
targets=["//fixture:checks"]
flags=["--define=fixture_mode=pass"]
timeout_seconds=60
"#,
                quoted(&scratch),
                quoted(&daemon),
                quoted(&operator.join("rbe.sock")),
                checksum,
                quoted(&executable),
                checksum,
                quoted(&executable),
                checksum
            ),
        )
        .unwrap();
        let version = tinytree_check_runner::Config::load(&policy)
            .unwrap()
            .profile_version("checks")
            .unwrap();
        let mut config = tinytree_server::ServiceConfig::default();
        config.max_buffered_bytes = 256 * 1024 * 1024;
        config.landing.enabled = true;
        config.landing.poll_interval_ms = 10;
        config.landing.job_ttl_seconds = 120;
        config.landing.max_buffered_bytes = 6 * 1024 * 1024 * 1024;
        config.landing.forge_profile = Some("checks".into());
        config.landing.profiles = vec![tinytree_server::LandingProfile {
            name: "checks".into(),
            version,
            repositories: vec![],
        }];
        config.landing.checker = Some(tinytree_server::LandingCheckerConfig {
            enabled: true,
            executable,
            sha256: checksum,
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
        (config, scratch)
    }
    fn imported_source_snapshot(
        store: &tinytree_store::Store,
        repo: &str,
        base: [u8; 32],
        renamed: bool,
    ) -> ([u8; 32], tinytree_model::Descriptor) {
        use tinytree_model as m;
        // Trusted import fixture setup: independent immutable snapshots branch
        // from genesis so historic view retirement can genuinely free old bytes.
        // Actual native authoring/rename/WIP is separately covered by FUSE tests.
        let file = |bytes: &[u8]| {
            let d = m::describe_file(bytes);
            store.put(repo, d, bytes, 0).unwrap();
            (
                m::TreeValue::File {
                    id: m::FileId(d.key.id.0),
                    executable: false,
                    size_bytes: bytes.len() as u64,
                    copy_id: None,
                },
                d,
            )
        };
        let put = |object: m::Object| {
            let encoded = m::encode(&object, Default::default()).unwrap();
            store
                .put(repo, encoded.descriptor, &encoded.bytes, 0)
                .unwrap();
            encoded.descriptor
        };
        let (readme, descriptor) = file(if renamed {
            b"Renamed dependency README\nnew bytes v2\n"
        } else {
            b"Original dependency README\nfrozen bytes v1\n"
        });
        let (module, _) = file(b"pub fn dependency() {}\n");
        let (private, _) = file(b"private-origin-only\n");
        let mut package = vec![];
        if renamed {
            package.push(m::TreeEntry {
                name: "README-renamed.md".into(),
                value: readme.clone(),
            });
        }
        package.push(m::TreeEntry {
            name: "module.rs".into(),
            value: module,
        });
        let package = put(m::Object::Tree(m::Tree { entries: package }));
        let private = put(m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "secret.txt".into(),
                value: private,
            }],
        }));
        let mut entries = vec![];
        if !renamed {
            entries.push(m::TreeEntry {
                name: "README.md".into(),
                value: readme,
            });
        }
        entries.extend([
            m::TreeEntry {
                name: "pkg".into(),
                value: m::TreeValue::Tree(m::TreeId(package.key.id.0)),
            },
            m::TreeEntry {
                name: "private".into(),
                value: m::TreeValue::Tree(m::TreeId(private.key.id.0)),
            },
        ]);
        let tree = put(m::Object::Tree(m::Tree { entries }));
        let signature = m::Signature {
            name: "Import Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: m::Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        };
        let commit = put(m::Object::Commit(m::Commit {
            parents: vec![m::CommitId(base)],
            predecessors: vec![],
            root_tree: m::TreeMerge {
                terms: vec![m::TreeId(tree.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: m::ChangeId([if renamed { 32 } else { 31 }; 16]),
            description: "independent imported fixture snapshot".into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }));
        (commit.key.id.0, descriptor)
    }

    // HTTP/2 response-loss fixture: control frames pass, RPC response frames
    // are discarded until released. Requests still reach the actual service.
    async fn response_loss_proxy(
        upstream: std::net::SocketAddr,
    ) -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let held = Arc::new(AtomicBool::new(true));
        let gate = held.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (client, _) = listener.accept().await.unwrap();
                let gate = gate.clone();
                connections.spawn(async move {
                    let server=tokio::net::TcpStream::connect(upstream).await.unwrap();
                    let (mut cr,mut cw)=client.into_split();
                    let (mut sr,mut sw)=server.into_split();
                    tokio::select! {
                        _=tokio::io::copy(&mut cr,&mut sw)=>{},
                        _=async {
                            loop {
                                let mut header=[0;9];
                                if sr.read_exact(&mut header).await.is_err(){break;}
                                let len=((header[0] as usize)<<16)|((header[1] as usize)<<8)|header[2] as usize;
                                assert!(len<=1024*1024,"bounded fixture frame");
                                let mut body=vec![0;len];
                                if sr.read_exact(&mut body).await.is_err(){break;}
                                if gate.load(Ordering::SeqCst) && matches!(header[3],0|1|9){continue;}
                                if cw.write_all(&header).await.is_err() || cw.write_all(&body).await.is_err(){break;}
                            }
                        }=>{}
                    }
                });
            }
        });
        (endpoint, held, task)
    }

    // Trusted immutable-source construction/upload, not a product mutation:
    // review submission, retry, checking, approval and landing below use tt only.
    fn owned_fixture_patch(
        store: &tinytree_store::Store,
        view: &tinytree_store::ResolvedView,
        actor: &tinytree_store::ForgeActor,
        base: [u8; 32],
        change: u8,
        name: &str,
        content: &[u8],
        now: i64,
    ) -> [u8; 32] {
        use tinytree_model as m;
        let read = |kind, id| {
            let (_, bytes) = store
                .read_metadata(
                    view.repository(),
                    m::ObjectKey {
                        kind,
                        id: m::ObjectId(id),
                    },
                )
                .unwrap();
            m::decode(&bytes, Default::default()).unwrap()
        };
        let m::Object::Commit(mut commit) = read(m::Kind::Commit, base) else {
            panic!("commit")
        };
        let m::Object::Tree(mut root) = read(m::Kind::Tree, commit.root_tree.terms[0].0) else {
            panic!("root")
        };
        let vendor = root
            .entries
            .iter_mut()
            .find(|entry| entry.name == "vendor")
            .unwrap();
        let m::TreeValue::Tree(vendor_id) = vendor.value else {
            panic!("vendor")
        };
        let m::Object::Tree(mut tree) = read(m::Kind::Tree, vendor_id.0) else {
            panic!("vendor tree")
        };
        let file = m::describe_file(content);
        store
            .forge_upload_object(view, actor, &format!("vendor/{name}"), file, content, now)
            .unwrap();
        tree.entries.retain(|entry| entry.name != name);
        tree.entries.push(m::TreeEntry {
            name: name.into(),
            value: m::TreeValue::File {
                id: m::FileId(file.key.id.0),
                executable: false,
                size_bytes: content.len() as u64,
                copy_id: None,
            },
        });
        tree.entries.sort_by(|a, b| a.name.cmp(&b.name));
        let upload = |path: &str, object: m::Object| {
            let encoded = m::encode(&object, Default::default()).unwrap();
            store
                .forge_upload_object(view, actor, path, encoded.descriptor, &encoded.bytes, now)
                .unwrap();
            encoded.descriptor.key.id.0
        };
        vendor.value = m::TreeValue::Tree(m::TreeId(upload("vendor", m::Object::Tree(tree))));
        commit.parents = vec![m::CommitId(base)];
        commit.predecessors = vec![];
        commit.root_tree = m::TreeMerge {
            terms: vec![m::TreeId(upload("", m::Object::Tree(root)))],
            labels: vec![String::new()],
        };
        commit.change_id = m::ChangeId([change; 16]);
        commit.description = format!("owned immutable fixture patch {change}");
        upload("", m::Object::Commit(commit))
    }
    async fn await_prepared(client: &Client, change: &str) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let review = service_value(client, &["review", "show", change, "--json"]);
            if review["state"] == "prepared" {
                return review;
            }
            assert!(
                Instant::now() < deadline,
                "review did not prepare: {review}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn approve_and_land(
        client: &Client,
        review: &serde_json::Value,
        key: &str,
    ) -> serde_json::Value {
        let change = review["change_id"].as_str().unwrap();
        let patch = review["patchset_id"].as_str().unwrap();
        let candidate = review["candidate"]["id"].as_str().unwrap();
        service_value(
            client,
            &[
                "review",
                "approve",
                change,
                "--patchset",
                patch,
                "--candidate",
                candidate,
                "--request-id",
                &format!("approve-{key}"),
                "--json",
            ],
        );
        let landed = service_value(
            client,
            &[
                "review",
                "land",
                change,
                "--patchset",
                patch,
                "--candidate",
                candidate,
                "--request-id",
                &format!("land-{key}"),
                "--json",
            ],
        );
        assert_eq!(landed["state"], "landed");
        landed
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forge_product_two_account_transform_review_and_retained_pin_survive_origin_gc() {
        use tinytree_model as m;
        use tinytree_store::{AccountId, ForgeActor};
        const OTHER: &str = "public-two-account-beta-token-not-a-secret";
        const ORIGINAL: &[u8] = b"Original dependency README\nfrozen bytes v1\n";
        let directory = tempfile::tempdir().unwrap();
        let store_root = directory.path().join("store");
        let mut limits = tinytree_store::Config::default();
        limits.max_retention_validation_records = 4096;
        limits.max_retention_validation_bytes = 8 * 1024 * 1024;
        limits.max_closure_objects = 8192;
        limits.max_closure_edges = 32768;
        limits.max_path_contexts = 8192;
        limits.max_path_context_bytes = 8 * 1024 * 1024;
        let store = tinytree_store::Store::open(&store_root, limits).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (config, scratch) = protocol_checker_config(directory.path(), &endpoint);
        let service = tinytree_server::RepositoryService::new(
            store.clone(),
            config,
            vec![],
            vec![
                tinytree_server::Principal::new_forge(
                    "source-owner",
                    "public-forge-fixture-token-not-a-secret",
                )
                .unwrap(),
                tinytree_server::Principal::new_forge("destination-owner", OTHER).unwrap(),
                tinytree_server::Principal::new_forge_checker(
                    "checker",
                    "public-two-account-checker-token-not-a-secret",
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        let source = client();
        let mut destination = client();
        destination.extra_env.push(("FORGE_OTHER_TOKEN", OTHER));
        select_account(&source, &endpoint, "upstream", "public", "FORGE_TEST_TOKEN");
        select_account(
            &destination,
            &endpoint,
            "downstream",
            "workspace",
            "FORGE_OTHER_TOKEN",
        );
        for (client, key, repo) in [
            (&source, "create-upstream", "forge-upstream"),
            (&destination, "create-downstream", "forge-downstream"),
        ] {
            let first = service_value(
                client,
                &["account", "provision", "--request-id", key, "--json"],
            );
            let retried = service_value(
                client,
                &["account", "provision", "--request-id", key, "--json"],
            );
            assert_eq!(first, retried);
            assert_eq!(first["monorepo_id"], repo);
        }
        let mut repos = store.forge_account_repositories(10).unwrap();
        repos.sort();
        assert_eq!(repos, vec!["forge-downstream", "forge-upstream"]);
        let initial = service_value(&source, &["account", "show", "--json"]);
        let genesis = fixture_id::<32>(initial["main_revision"].as_str().unwrap());
        let before = publish_fixture_view(
            &source,
            "public",
            genesis,
            &["README.md", "pkg"],
            true,
            "view-before",
        );
        let (imported, original) =
            imported_source_snapshot(&store, "forge-upstream", genesis, false);
        let after = publish_fixture_view(
            &source,
            "public",
            imported,
            &["README.md", "pkg"],
            true,
            "view-imported",
        );
        let beta = service_value(&destination, &["account", "show", "--json"]);
        publish_fixture_view(
            &destination,
            "workspace",
            fixture_id::<32>(beta["main_revision"].as_str().unwrap()),
            &["vendor"],
            false,
            "view-destination",
        );
        let public = service_value(&source, &["view", "show", "--json"]);
        assert!(
            public["readme"]["content"]
                .as_str()
                .unwrap()
                .contains("Original dependency")
        );
        assert!(!public.to_string().contains("private-origin-only"));
        let matches = service_value(&source, &["search", "dependency", "--json"]);
        assert_eq!(matches["complete"], true);
        assert!(matches.to_string().contains("README.md"));
        select_account(
            &destination,
            &endpoint,
            "upstream",
            "public",
            "FORGE_OTHER_TOKEN",
        );
        for args in [
            vec!["account", "show", "--json"],
            vec!["view", "read", "private/secret.txt", "--json"],
        ] {
            let denied = destination.run(&args);
            assert!(!denied.status.success());
            assert!(!String::from_utf8_lossy(&denied.stdout).contains("private-origin-only"));
        }
        assert!(service_value(&destination, &["view", "show", "--json"])["readme"].is_object());
        select_account(
            &destination,
            &endpoint,
            "downstream",
            "workspace",
            "FORGE_OTHER_TOKEN",
        );
        let plan = destination.home.join("transform.json");
        fs::write(&plan,serde_json::to_vec(&serde_json::json!({"source_account":"upstream","source_view":"public","source_before_view":before,"source_after_view":after,"spec":{"mappings":[{"source_prefix":"","destination_prefix":"vendor"}]},"change_id":"44".repeat(16),"lineage":[]})).unwrap()).unwrap();
        let main_before = store
            .references("forge-downstream", &["main".into()])
            .unwrap();
        let proposed = service_value(
            &destination,
            &[
                "transform",
                "start",
                "--plan",
                plan.to_str().unwrap(),
                "--request-id",
                "dependency-import",
                "--json",
            ],
        );
        let retry = service_value(
            &destination,
            &[
                "transform",
                "start",
                "--plan",
                plan.to_str().unwrap(),
                "--request-id",
                "dependency-import",
                "--json",
            ],
        );
        assert_eq!(proposed["transform"]["id"], retry["transform"]["id"]);
        assert_eq!(
            proposed["review"]["patchset_id"],
            retry["review"]["patchset_id"]
        );
        assert_eq!(
            store
                .references("forge-downstream", &["main".into()])
                .unwrap(),
            main_before,
            "transform proposes a review, never updates main"
        );
        let run = proposed["transform"]["id"].as_str().unwrap().to_owned();
        let change = proposed["review"]["change_id"].as_str().unwrap().to_owned();
        let deadline = Instant::now() + Duration::from_secs(15);
        let review = loop {
            let review = service_value(&destination, &["review", "show", &change, "--json"]);
            if review["state"] == "prepared" {
                break review;
            }
            assert!(
                Instant::now() < deadline,
                "physical checker did not prepare review: {review}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let patchset = review["patchset_id"].as_str().unwrap();
        let candidate = review["candidate"]["id"].as_str().unwrap();
        service_value(
            &destination,
            &[
                "review",
                "approve",
                &change,
                "--patchset",
                patchset,
                "--candidate",
                candidate,
                "--request-id",
                "approve-import",
                "--json",
            ],
        );
        let landed = service_value(
            &destination,
            &[
                "review",
                "land",
                &change,
                "--patchset",
                patchset,
                "--candidate",
                candidate,
                "--request-id",
                "land-import",
                "--json",
            ],
        );
        assert_eq!(landed["state"], "landed");
        assert_eq!(
            fs::read_to_string(scratch.join("executions"))
                .unwrap()
                .lines()
                .filter(|line| *line == "run")
                .count(),
            1,
            "duplicate transform requests must not duplicate checker execution"
        );
        assert_ne!(
            store
                .references("forge-downstream", &["main".into()])
                .unwrap(),
            main_before
        );
        assert_eq!(
            service_value(
                &destination,
                &["view", "read", "vendor/README.md", "--json"]
            )["content"],
            std::str::from_utf8(ORIGINAL).unwrap()
        );
        // Freeze all authored inputs against the same older authorized projection.
        // No mounted alias is rebound; immutable bytes/receipts are fixture setup.
        let actor = ForgeActor {
            principal: "destination-owner".into(),
        };
        let account = AccountId("downstream".into());
        let frozen = store
            .forge_resolve_view(&account, Some(&actor), "workspace", None)
            .unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let projection = store.forge_project_view(&frozen, now).unwrap();
        let base = projection.projected_commit;
        let version = fixture_hex(&projection.view_version.0);
        let advance = owned_fixture_patch(
            &store,
            &frozen,
            &actor,
            base,
            61,
            "shared.txt",
            b"independent landed addition\n",
            now,
        );
        let stale = owned_fixture_patch(
            &store,
            &frozen,
            &actor,
            base,
            62,
            "stale.txt",
            b"stale base independent patch\n",
            now,
        );
        let conflicting = owned_fixture_patch(
            &store,
            &frozen,
            &actor,
            base,
            63,
            "shared.txt",
            b"competing add/add contents\n",
            now,
        );
        let corrected = owned_fixture_patch(
            &store,
            &frozen,
            &actor,
            base,
            63,
            "corrected.txt",
            b"explicit correction drops conflicting addition\n",
            now,
        );
        let interrupted = owned_fixture_patch(
            &store,
            &frozen,
            &actor,
            base,
            64,
            "interrupted.txt",
            b"committed before response loss\n",
            now,
        );
        let submit = |head: [u8; 32], key: &str, expected: Option<&str>| {
            let head = fixture_hex(&head);
            let base = fixture_hex(&base);
            let mut args = vec![
                "push",
                "--source-base",
                &base,
                "--source-head",
                &head,
                "--view-version",
                &version,
                "--request-id",
                key,
                "--json",
            ];
            if let Some(patch) = expected {
                args.extend(["--expected-patchset", patch]);
            }
            service_value(&destination, &args)
        };
        submit(advance, "independent-main", None);
        let independent = await_prepared(&destination, &fixture_hex(&[61; 16])).await;
        let independent = approve_and_land(&destination, &independent, "independent-main");
        let advanced_target = independent["candidate"]["head"].clone();
        // Actual product push from old base must target the independently advanced main.
        let first_stale = submit(stale, "stale-base-push", None);
        let prepared = await_prepared(&destination, &fixture_hex(&[62; 16])).await;
        assert_eq!(prepared["change_id"], fixture_hex(&[62; 16]));
        assert_eq!(prepared["source_base"], fixture_hex(&base));
        assert_eq!(prepared["source_head"], fixture_hex(&stale));
        assert_eq!(prepared["patchset_id"], first_stale["patchset_id"]);
        assert_eq!(prepared["candidate"]["target"], advanced_target);
        assert_ne!(prepared["candidate"]["head"], fixture_hex(&stale));
        assert_eq!(
            prepared["candidate"]["rewrites"],
            serde_json::json!([{"old":fixture_hex(&stale),"new":prepared["candidate"]["head"],"change_id":fixture_hex(&[62;16])}])
        );
        let replay = submit(stale, "stale-base-push", None);
        assert_eq!(replay["patchset_id"], prepared["patchset_id"]);
        let stale_landed = approve_and_land(&destination, &prepared, "stale-base");
        assert_eq!(
            stale_landed["projected_rewrites"],
            serde_json::json!([{"old":fixture_hex(&stale),"new":stale_landed["projected_head"],"change_id":fixture_hex(&[62;16])}])
        );
        assert_eq!(
            service_value(
                &destination,
                &["view", "read", "vendor/shared.txt", "--json"]
            )["content"],
            "independent landed addition\n"
        );
        assert_eq!(
            service_value(
                &destination,
                &["view", "read", "vendor/stale.txt", "--json"]
            )["content"],
            "stale base independent patch\n"
        );
        submit(conflicting, "conflicting-push", None);
        let conflict = service_value(
            &destination,
            &["review", "show", &fixture_hex(&[63; 16]), "--json"],
        );
        assert_eq!(conflict["state"], "conflict");
        assert_eq!(conflict["source_head"], fixture_hex(&conflicting));
        let old_patch = conflict["patchset_id"].as_str().unwrap();
        let retried = submit(conflicting, "conflicting-push", None);
        assert_eq!(retried["state"], "conflict");
        assert_eq!(retried["patchset_id"], old_patch);
        let replacement = submit(corrected, "corrected-push", Some(old_patch));
        assert_eq!(replacement["change_id"], conflict["change_id"]);
        assert_ne!(replacement["patchset_id"], old_patch);
        let corrected_review = await_prepared(&destination, &fixture_hex(&[63; 16])).await;
        assert_eq!(corrected_review["source_head"], fixture_hex(&corrected));
        assert_eq!(corrected_review["approvals"], 0);
        approve_and_land(&destination, &corrected_review, "corrected-push");
        let history = service_value(
            &destination,
            &[
                "review",
                "show",
                &fixture_hex(&[63; 16]),
                "--patchset",
                old_patch,
                "--json",
            ],
        );
        assert_eq!(history["source_head"], fixture_hex(&conflicting));
        assert_eq!(history["patchset_id"], old_patch);
        assert_eq!(
            service_value(
                &destination,
                &["view", "read", "vendor/corrected.txt", "--json"]
            )["content"],
            "explicit correction drops conflicting addition\n"
        );

        // Real post-dispatch interruption: the server records the mutation but
        // the response is withheld; terminate tt, then resume the exact journal.
        let (proxy, hold, proxy_task) =
            response_loss_proxy(endpoint.strip_prefix("http://").unwrap().parse().unwrap()).await;
        select_account(
            &destination,
            &proxy,
            "downstream",
            "workspace",
            "FORGE_OTHER_TOKEN",
        );
        let interrupted_head = fixture_hex(&interrupted);
        let base_hex = fixture_hex(&base);
        let mut killed = destination.spawn(&[
            "push",
            "--source-base",
            &base_hex,
            "--source-head",
            &interrupted_head,
            "--view-version",
            &version,
            "--request-id",
            "lost-response-push",
            "--json",
        ]);
        let deadline = Instant::now() + Duration::from_secs(10);
        let accepted = loop {
            if let Some(review) = store
                .forge_review_operation(&account, &actor, "lost-response-push")
                .unwrap()
            {
                break review;
            }
            assert!(
                killed.0.as_mut().unwrap().try_wait().unwrap().is_none(),
                "CLI must remain waiting for its withheld response"
            );
            assert!(
                Instant::now() < deadline,
                "mutation never dispatched to real service"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(accepted.source_head, interrupted);
        assert!(killed.0.as_mut().unwrap().try_wait().unwrap().is_none());
        let pending: serde_json::Value = serde_json::from_str(&destination.success(&[
            "request",
            "show",
            "lost-response-push",
            "--json",
        ]))
        .unwrap();
        assert_eq!(pending["result"]["request_id"], "lost-response-push");
        killed.0.as_mut().unwrap().kill().unwrap();
        killed.0.take().unwrap().wait().unwrap();
        hold.store(false, std::sync::atomic::Ordering::SeqCst);
        let resumed = service_value(
            &destination,
            &["request", "resume", "lost-response-push", "--json"],
        );
        assert_eq!(resumed["patchset_id"], fixture_hex(&accepted.patchset_id));
        assert_eq!(resumed["source_head"], interrupted_head);
        assert_eq!(resumed["change_id"], fixture_hex(&[64; 16]));
        let replay = service_value(
            &destination,
            &["request", "resume", "lost-response-push", "--json"],
        );
        assert_eq!(replay["patchset_id"], resumed["patchset_id"]);
        let completed = await_prepared(&destination, &fixture_hex(&[64; 16])).await;
        assert_eq!(completed["patchset_number"], 1);
        approve_and_land(&destination, &completed, "lost-response");
        select_account(
            &destination,
            &endpoint,
            "downstream",
            "workspace",
            "FORGE_OTHER_TOKEN",
        );
        proxy_task.abort();
        assert!(proxy_task.await.unwrap_err().is_cancelled());
        assert_eq!(
            fs::read_to_string(scratch.join("executions"))
                .unwrap()
                .lines()
                .filter(|line| *line == "run")
                .count(),
            5,
            "one checker execution per accepted candidate, none duplicated by retries or lost response"
        );

        let first = service_value(
            &destination,
            &[
                "pin",
                "resolve",
                &run,
                "--path",
                "vendor/README.md",
                "--max-bytes",
                "8",
                "--json",
            ],
        );
        let rest = service_value(
            &destination,
            &[
                "pin",
                "resolve",
                &run,
                "--path",
                "vendor/README.md",
                "--offset",
                "8",
                "--max-bytes",
                "65536",
                "--json",
            ],
        );
        let read_bytes = |value: &serde_json::Value| {
            value["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as u8)
                .collect::<Vec<_>>()
        };
        assert_eq!([read_bytes(&first), read_bytes(&rest)].concat(), ORIGINAL);
        assert_eq!(rest["eof"], true);
        let (renamed, _) = imported_source_snapshot(&store, "forge-upstream", genesis, true);
        publish_fixture_view(
            &source,
            "public",
            renamed,
            &["README.md", "pkg"],
            true,
            "view-renamed",
        );
        assert!(
            service_value(
                &source,
                &["view", "read", "pkg/README-renamed.md", "--json"]
            )["content"]
                .as_str()
                .unwrap()
                .contains("new bytes v2")
        );
        for (version, key) in [(&before, "retire-before"), (&after, "retire-imported")] {
            let account = service_value(&source, &["account", "show", "--json"]);
            service_value(
                &source,
                &[
                    "view",
                    "retire",
                    version,
                    "--expected-epoch",
                    &account["policy_epoch"].as_u64().unwrap().to_string(),
                    "--request-id",
                    key,
                    "--json",
                ],
            );
        }
        // Trusted server maintenance, not a new customer GC endpoint.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        store.gc("forge-upstream", now + 7 * 86400).unwrap();
        let original_path = store_root
            .join("repos/forge-upstream/1")
            .join(fixture_hex(&original.key.id.0));
        assert!(
            store
                .descriptor("forge-upstream", original.key)
                .unwrap()
                .is_none()
        );
        assert!(
            !original_path.exists(),
            "the original source payload must actually be collected"
        );
        let retained = service_value(
            &destination,
            &[
                "pin",
                "resolve",
                &run,
                "--path",
                "vendor/README.md",
                "--json",
            ],
        );
        assert_eq!(
            read_bytes(&retained),
            ORIGINAL,
            "old pin never follows the renamed/latest source"
        );
        let lineage = service_value(&destination, &["lineage", &run, "--json"]);
        assert!(lineage["origin"].is_null());
        let redacted = service_value(&destination, &["transform", "show", &run, "--json"]);
        assert!(redacted["origin"].is_null());
        assert!(!redacted.to_string().contains("upstream"));
        assert!(!redacted.to_string().contains(&after));
        for args in [
            vec![
                "pin",
                "retire",
                run.as_str(),
                "--request-id",
                "retire-protected",
                "--json",
            ],
            vec![
                "pin",
                "resolve",
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "--json",
            ],
            vec![
                "pin",
                "resolve",
                run.as_str(),
                "--path",
                "missing.txt",
                "--json",
            ],
        ] {
            let denied = destination.run(&args);
            assert!(!denied.status.success());
            let report: serde_json::Value = serde_json::from_slice(&denied.stdout).unwrap();
            assert_eq!(report["state"], "failed");
            assert!(report["result"].is_null());
        }
        // Temporary retained-body storage fault with exact RAII restore. It is
        // neither pin unprotection nor a product/source mutation.
        struct Restore {
            original: PathBuf,
            quarantine: PathBuf,
        }
        impl Drop for Restore {
            fn drop(&mut self) {
                if self.quarantine.exists() {
                    fs::rename(&self.quarantine, &self.original).unwrap();
                }
            }
        }
        let payload = store_root
            .join("repos/forge-downstream/1")
            .join(fixture_hex(&original.key.id.0));
        let quarantine = payload.with_extension("unavailable-fixture");
        fs::rename(&payload, &quarantine).unwrap();
        let restore = Restore {
            original: payload,
            quarantine,
        };
        let unavailable = destination.run(&[
            "pin",
            "resolve",
            &run,
            "--path",
            "vendor/README.md",
            "--json",
        ]);
        assert!(!unavailable.status.success());
        let report: serde_json::Value = serde_json::from_slice(&unavailable.stdout).unwrap();
        assert!(report["result"].is_null());
        drop(restore);
        assert_eq!(
            read_bytes(&service_value(
                &destination,
                &[
                    "pin",
                    "resolve",
                    &run,
                    "--path",
                    "vendor/README.md",
                    "--json"
                ]
            )),
            ORIGINAL
        );
        // Optional isolated retirement fixture: never reviewed or landed. Only
        // its exact transform-owned protection is released by maintenance after
        // proving no review exists. The real landed pin above stays protected.
        let actor = ForgeActor {
            principal: "destination-owner".into(),
        };
        let alpha = AccountId("upstream".into());
        let beta = AccountId("downstream".into());
        let current_source = store
            .forge_resolve_view(&alpha, Some(&actor), "public", None)
            .unwrap();
        let current_destination = store
            .forge_resolve_view(&beta, Some(&actor), "workspace", None)
            .unwrap();
        let signature = m::Signature {
            name: "Lifecycle Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: m::Timestamp {
                millis_since_epoch: now * 1000,
                tz_offset_minutes: 0,
            },
        };
        let standalone = store
            .forge_prepare_transform(
                &actor,
                &tinytree_store::forge_transforms::ForgeTransformRequest {
                    request_id: "unreferenced-lifecycle-fixture".into(),
                    before: current_source.clone(),
                    after: current_source,
                    destination: current_destination,
                    spec: tinytree_store::forge_transforms::ForgeTransformSpec {
                        mappings: vec![tinytree_store::forge_transforms::ForgePathMapping {
                            source_prefix: "".into(),
                            destination_prefix: "vendor".into(),
                        }],
                    },
                    change_id: m::ChangeId([91; 16]),
                    lineage: vec![],
                },
                &signature,
                now,
            )
            .unwrap();
        assert!(
            store
                .forge_review(&beta, &actor, m::ChangeId([91; 16]))
                .is_err()
        );
        let owner = format!("transform-{}", fixture_hex(&standalone.id));
        assert_eq!(
            standalone
                .pin
                .protected_roots
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![owner.clone()]
        );
        store
            .forge_protect_pin(&beta, &actor, standalone.id, &owner, false)
            .unwrap();
        let retired = fixture_hex(&standalone.id);
        service_value(
            &destination,
            &[
                "pin",
                "retire",
                &retired,
                "--request-id",
                "retire-unreferenced",
                "--json",
            ],
        );
        let rejected = destination.run(&["pin", "resolve", &retired, "--json"]);
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stdout).contains("dependency pin retired"));
        assert_eq!(
            read_bytes(&service_value(
                &destination,
                &[
                    "pin",
                    "resolve",
                    &run,
                    "--path",
                    "vendor/README.md",
                    "--json"
                ]
            )),
            ORIGINAL
        );
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn forge_product_context_does_not_follow_a_symlink() {
        let client = client();
        client.success(&[
            "context",
            "set",
            "--server",
            "http://127.0.0.1:8093",
            "--account",
            "example",
        ]);
        let path = client.home.join("config/tinytree/context.json");
        fs::remove_file(&path).unwrap();
        let secret = client.home.join("do-not-read");
        fs::write(&secret, b"private-token-value").unwrap();
        std::os::unix::fs::symlink(&secret, &path).unwrap();
        let failed = client.run(&["context", "show", "--json"]);
        assert!(!failed.status.success());
        assert!(!String::from_utf8_lossy(&failed.stdout).contains("private-token-value"));
        assert_eq!(fs::read(&secret).unwrap(), b"private-token-value");
    }
}

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

struct OwnedChild(Option<Child>);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Client {
    _directory: tempfile::TempDir,
    binary: PathBuf,
    root: PathBuf,
    home: PathBuf,
    config: PathBuf,
    empty_path: PathBuf,
    extra_env: Vec<(&'static str, &'static str)>,
}
impl Client {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("home");
        let root = directory.path().join("workspace");
        let empty_path = directory.path().join("no-programs");
        fs::create_dir(&home).unwrap();
        fs::create_dir(&root).unwrap();
        fs::create_dir(&empty_path).unwrap();
        let config = directory.path().join("jj.toml");
        fs::write(
            &config,
            "[user]\nname = 'Native Fixture'\nemail = 'fixture@example.invalid'\n\
             [operation]\nhostname = 'test'\nusername = 'test'\n\
             [ui]\npaginate = 'never'\n",
        )
        .unwrap();
        Self {
            _directory: directory,
            binary: Path::new(env!("CARGO_BIN_EXE_tinytree-jj"))
                .canonicalize()
                .unwrap(),
            root,
            home,
            config,
            empty_path,
            extra_env: vec![],
        }
    }
    fn spawn(&self, args: &[&str]) -> OwnedChild {
        let child = Command::new(&self.binary)
            .current_dir(&self.root)
            .args(["--no-pager", "--color=never"])
            .args(args)
            .env_clear()
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("JJ_CONFIG", &self.config)
            .env("PATH", &self.empty_path)
            .env("TERM", "dumb")
            .envs(self.extra_env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        OwnedChild(Some(child))
    }
    fn run(&self, args: &[&str]) -> Output {
        self.wait(self.spawn(args), args)
    }
    fn wait(&self, mut child: OwnedChild, args: &[&str]) -> Output {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if child.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                return child.0.take().unwrap().wait_with_output().unwrap();
            }
            assert!(Instant::now() < deadline, "command timed out: {args:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn success(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{args:?} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        String::from_utf8(output.stdout).unwrap()
    }
    fn revision(&self, revision: &str) -> String {
        let id = self.success(&["log", "--no-graph", "-r", revision, "-T", "commit_id"]);
        let id = id.trim().to_owned();
        assert_eq!(id.len(), 64, "native commit ID: {id}");
        assert!(id.bytes().all(|c| c.is_ascii_hexdigit()));
        id
    }
    fn roots(&self) -> serde_json::Value {
        let encoded = self.success(&["tinytree", "roots", "--json"]);
        let audit: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(audit["version"], 1);
        assert!(encoded.len() <= audit["encoded_bytes_upper_bound"].as_u64().unwrap() as usize);
        let roots = audit["object_roots"].as_array().unwrap();
        let mut previous = None;
        for root in roots {
            let kind = root["kind"].as_str().unwrap();
            let rank = match kind {
                "file" => 1,
                "tree" => 2,
                "commit" => 3,
                "symlink" => 4,
                "copy_history" => 5,
                _ => panic!("unexpected kind"),
            };
            let id = root["id"].as_str().unwrap();
            assert_eq!(id.len(), 64);
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            let key = (rank, id);
            assert!(
                previous.is_none_or(|old| old < key),
                "roots must be unique and sorted"
            );
            previous = Some(key);
        }
        audit
    }
    fn assert_native(&self) {
        assert_eq!(
            fs::read_to_string(self.root.join(".jj/repo/store/type")).unwrap(),
            "tinytree-native-v1",
        );
        assert!(!self.root.join(".git").exists());
        assert!(!self.root.join(".jj/repo/store/git").exists());
        assert!(self.root.join(".jj/repo/op_store").is_dir());
        assert!(self.root.join(".jj/repo/op_heads").is_dir());
        assert!(self.root.join(".jj/repo/index").is_dir());
    }
}

#[test]
fn stock_annotation_filters_unchanged_merge_parents_before_attribution() {
    let client = Client::new();
    client.success(&["tinytree", "init"]);
    let path = client.root.join("story.txt");
    fs::write(&path, "x\n").unwrap();
    client.success(&["describe", "-m", "A"]);
    client.success(&["bookmark", "set", "base", "-r", "@"]);

    client.success(&["new", "-r", "base"]);
    client.success(&["describe", "-m", "U"]);
    client.success(&["bookmark", "set", "unchanged", "-r", "@"]);

    client.success(&["new", "-r", "base"]);
    fs::remove_file(&path).unwrap();
    client.success(&["describe", "-m", "D"]);
    client.success(&["new"]);
    fs::write(&path, "x\n").unwrap();
    client.success(&["describe", "-m", "B"]);
    client.success(&["bookmark", "set", "readded", "-r", "@"]);

    client.success(&["new", "-r", "unchanged", "-r", "readded"]);
    client.success(&["describe", "-m", "M"]);
    // This is stock jj's local annotation engine, not the remote policy. The
    // unchanged U is filtered out, then the indirect M->A edge is removed as
    // transitive through B. Raw first-parent propagation would wrongly pick A.
    assert_eq!(
        client.success(&[
            "--ignore-working-copy",
            "file",
            "annotate",
            "story.txt",
            "-r",
            "@",
            "-T",
            "commit.description().first_line() ++ \":\" ++ content",
        ]),
        "B:x\n",
    );
}

#[test]
fn stock_annotation_resolves_recursive_bases_and_ordered_octopus_parents() {
    struct Case {
        commits: Vec<(&'static str, Vec<usize>, &'static str)>,
        annotation: &'static str,
    }
    let cases = [
        Case {
            commits: vec![
                ("base", vec![], "a\nstable\nb\n"),
                ("left", vec![0], "A\nstable\nb\n"),
                ("right", vec![0], "a\nstable\nB\n"),
                ("merge-left", vec![1, 2], "A\nstable\nB\n"),
                ("merge-right", vec![2, 1], "A\nstable\nB\n"),
                ("recursive", vec![3, 4], "A\nstable\nB\n"),
                ("unchanged-head", vec![5], "A\nstable\nB\n"),
            ],
            annotation: "left:A\nbase:stable\nright:B\n",
        },
        Case {
            commits: vec![
                ("base", vec![], "a\nanchor-a\nb\nanchor-b\nc\n"),
                ("first", vec![0], "A\nanchor-a\nb\nanchor-b\nc\n"),
                ("second", vec![0], "a\nanchor-a\nB\nanchor-b\nc\n"),
                ("third", vec![0], "a\nanchor-a\nb\nanchor-b\nC\n"),
                ("octopus", vec![1, 2, 3], "A\nanchor-a\nB\nanchor-b\nC\n"),
                ("unchanged-head", vec![4], "A\nanchor-a\nB\nanchor-b\nC\n"),
            ],
            annotation: "first:A\nbase:anchor-a\nsecond:B\nbase:anchor-b\nthird:C\n",
        },
    ];
    for case in cases {
        let client = Client::new();
        client.success(&["tinytree", "init"]);
        for (index, (description, parents, content)) in case.commits.iter().enumerate() {
            if index != 0 {
                let mut args = vec!["new".to_owned()];
                for parent in parents {
                    args.extend(["-r".to_owned(), format!("node{parent}")]);
                }
                client.success(&args.iter().map(String::as_str).collect::<Vec<_>>());
            }
            fs::write(client.root.join("story.txt"), content).unwrap();
            client.success(&["describe", "-m", description]);
            client.success(&["bookmark", "set", &format!("node{index}"), "-r", "@"]);
        }
        assert_eq!(
            client.success(&[
                "--ignore-working-copy",
                "file",
                "annotate",
                "story.txt",
                "-r",
                "@",
                "-T",
                "commit.description().first_line() ++ \":\" ++ content",
            ]),
            case.annotation
        );
    }
}

#[test]
fn search_validates_regex_before_repository_access_and_accepts_fixed_strings() {
    let client = Client::new(); // Deliberately not a repository.
    let malformed = client.run(&["tinytree", "search", "["]);
    assert!(!malformed.status.success());
    assert!(
        String::from_utf8_lossy(&malformed.stderr).contains("Native search failed:"),
        "malformed regex must be rejected before workspace discovery: {}",
        String::from_utf8_lossy(&malformed.stderr)
    );
    for flag in ["-F", "--fixed-strings"] {
        let literal = client.run(&["tinytree", "search", flag, "["]);
        assert!(!literal.status.success());
        let stderr = String::from_utf8_lossy(&literal.stderr);
        assert!(
            !stderr.contains("Native search failed:") && stderr.contains("repo"),
            "fixed strings must accept regex metacharacters and reach workspace discovery: {stderr}"
        );
    }
}

#[test]
fn local_native_commands_snapshot_edit_restore_and_undo_without_git() {
    let client = Client::new();
    client.success(&["tinytree", "init"]);
    client.assert_native();
    let initial_change = client.success(&["log", "--no-graph", "-r", "@", "-T", "change_id"]);
    fs::write(client.root.join("hello.txt"), "first native version\n").unwrap();
    client.success(&["status"]);
    assert_eq!(
        client.success(&["file", "show", "hello.txt"]),
        "first native version\n",
    );
    client.success(&["describe", "-m", "native base"]);
    assert_eq!(
        client.success(&["log", "--no-graph", "-r", "@", "-T", "change_id"]),
        initial_change,
    );
    let base = client.revision("@");
    client.success(&["new", "-m", "native child"]);
    fs::write(client.root.join("hello.txt"), "second native version\n").unwrap();
    client.success(&["status"]);
    let child = client.revision("@");
    assert_ne!(base, child);
    assert_eq!(client.revision("@-"), base);

    client.success(&["restore", "--from", "@-", "hello.txt"]);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "first native version\n",
    );
    client.success(&["undo"]);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "second native version\n",
    );
    assert_eq!(client.revision("@"), child);
    client.success(&["edit", &base]);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "first native version\n",
    );
    client.success(&["edit", &child]);
    assert_eq!(
        client.success(&["file", "show", "hello.txt"]),
        "second native version\n",
    );
    assert_eq!(
        client
            .success(&["log", "--no-graph", "-r", "@", "-T", "description"])
            .trim(),
        "native child",
    );
    // An abandoned operation remains physically retained until jj's operation
    // GC. Its old view/native commit must remain in a read-only root inventory.
    let old_operation = client.success(&["op", "log", "--no-graph", "-n", "1", "-T", "id"]);
    client.success(&["describe", "-m", "rewritten child"]);
    client.success(&["op", "abandon", old_operation.trim()]);
    let audit = client.roots();
    assert!(
        audit["retained_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id == old_operation.trim())
    );
    assert!(
        audit["object_roots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|root| root["kind"] == "commit" && root["id"] == child)
    );

    // Audit must not snapshot unsaved filesystem edits or create operations.
    let before = audit["operation_heads"].clone();
    fs::write(client.root.join("hello.txt"), "not snapshotted by audit\n").unwrap();
    let after = client.roots();
    assert_eq!(after["operation_heads"], before);
    assert_eq!(
        after["working_copy_operation"],
        audit["working_copy_operation"]
    );
    assert_eq!(after["object_roots"], audit["object_roots"]);
    let rejected = client.run(&["tinytree", "roots", "--json", "--max-operations", "1"]);
    assert!(!rejected.status.success());
    assert!(
        rejected.stdout.is_empty(),
        "no partial audit after a failed budget"
    );
    assert_eq!(client.roots()["operation_heads"], before);
    let oversized = client.run(&["tinytree", "roots", "--json", "--max-file-bytes", "16"]);
    assert!(!oversized.status.success());
    assert!(
        oversized.stdout.is_empty(),
        "no partial audit after a bounded read failure"
    );
    assert_eq!(client.roots()["operation_heads"], before);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "not snapshotted by audit\n"
    );
    client.assert_native();
}

#[test]
fn native_root_audit_keeps_negative_working_copy_terms_and_fails_closed() {
    use jj_native_backend::{DirectObjectIo, NativeObjectIo};
    use tinytree_model::{Kind, Object, ObjectId, ObjectKey};
    let client = Client::new();
    client.success(&["tinytree", "init"]);
    fs::write(client.root.join("conflict.txt"), "base\n").unwrap();
    client.success(&["describe", "-m", "base"]);
    let base = client.revision("@");
    client.success(&["new", &base, "-m", "left"]);
    fs::write(client.root.join("conflict.txt"), "left\n").unwrap();
    client.success(&["status"]);
    let left = client.revision("@");
    client.success(&["new", &base, "-m", "right"]);
    fs::write(client.root.join("conflict.txt"), "right\n").unwrap();
    client.success(&["status"]);
    let right = client.revision("@");
    client.success(&["new", &left, &right]);
    let merged = client.revision("@");
    let bytes: Vec<u8> = (0..merged.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&merged[offset..offset + 2], 16).unwrap())
        .collect();
    let provider = DirectObjectIo::load(
        &client.root.join(".jj/repo/store"),
        jj_native_backend::Config::default(),
    )
    .unwrap();
    let metadata = futures::executor::block_on(provider.read_metadata(ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(bytes.try_into().unwrap()),
    }))
    .unwrap();
    let Object::Commit(commit) = metadata.into_object() else {
        panic!("expected commit")
    };
    assert!(
        commit.root_tree.terms.len() >= 3,
        "fixture must have a negative tree term"
    );
    let negative = commit.root_tree.terms[1];
    drop(provider);
    let audit = client.roots();
    for term in &commit.root_tree.terms {
        let id: String = term.0.iter().map(|byte| format!("{byte:02x}")).collect();
        assert!(
            audit["object_roots"]
                .as_array()
                .unwrap()
                .iter()
                .any(|root| root["kind"] == "tree" && root["id"] == id)
        );
    }
    assert_ne!(negative, commit.root_tree.terms[0]);
    // Deliberately corrupt the retained operation fixture, including operations
    // outside current ancestry. No root subset may escape after a missing view.
    let views = client.root.join(".jj/repo/op_store/views");
    let victims: Vec<_> = fs::read_dir(views)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect();
    assert!(!victims.is_empty());
    let originals: Vec<_> = victims.iter().map(|path| fs::read(path).unwrap()).collect();
    // Malformed protobuf targets used to panic in jj's assumption-based view
    // conversion before the audit could verify the semantic object identity.
    for malformed in [&[0x4a, 0x00][..], &[0x4a, 0x02, 0x1a, 0x00][..]] {
        for path in &victims {
            fs::write(path, malformed).unwrap();
        }
        let rejected = client.run(&["tinytree", "roots", "--json"]);
        assert_eq!(
            rejected.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&rejected.stderr)
        );
        assert!(rejected.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&rejected.stderr).contains("panicked"));
        for path in &victims {
            assert_eq!(fs::read(path).unwrap(), malformed);
        }
        for (path, bytes) in victims.iter().zip(&originals) {
            fs::write(path, bytes).unwrap();
        }
    }
    let checkout = client.root.join(".jj/working_copy/checkout");
    let original_checkout = fs::read(&checkout).unwrap();
    let oversized = vec![0u8; 64 * 1024];
    fs::write(&checkout, &oversized).unwrap();
    let rejected = client.run(&["tinytree", "roots", "--json", "--max-file-bytes", "16"]);
    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&rejected.stderr).contains("panicked"));
    assert_eq!(fs::read(&checkout).unwrap(), oversized);
    fs::write(&checkout, original_checkout).unwrap();
    for victim in victims {
        fs::remove_file(victim).unwrap();
    }
    let rejected = client.run(&["tinytree", "roots", "--json"]);
    assert!(!rejected.status.success());
    assert!(rejected.stdout.is_empty());
}

#[test]
fn native_init_rejects_unsupported_overrides_and_existing_workspace() {
    let client = Client::new();
    let rejected = client.run(&["--ignore-working-copy", "tinytree", "init", "rejected"]);
    assert!(!rejected.status.success());
    assert!(!client.root.join("rejected").exists());
    client.success(&["tinytree", "init"]);
    fs::write(client.root.join("keep.txt"), "keep existing workspace\n").unwrap();
    client.success(&["status"]);
    let original = client.revision("@");
    let rejected = client.run(&["tinytree", "init"]);
    assert!(!rejected.status.success());
    assert_eq!(client.revision("@"), original);
    assert_eq!(
        fs::read_to_string(client.root.join("keep.txt")).unwrap(),
        "keep existing workspace\n",
    );
    client.assert_native();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_jj_processes_share_broker_objects_not_workspace_state() {
    use jj_native_backend::{DirectObjectIo, NativeObjectIo};
    use std::{collections::HashMap, os::unix::fs::PermissionsExt, sync::Arc};
    let host = tempfile::tempdir().unwrap();
    fs::set_permissions(host.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let host_path = host.path().canonicalize().unwrap();
    let repository = host_path.join("repository");
    fs::create_dir(&repository).unwrap();
    fs::set_permissions(&repository, fs::Permissions::from_mode(0o700)).unwrap();
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&repository, jj_native_backend::Config::default()).unwrap());
    let service = native_broker::BrokerService::new(
        HashMap::from([("example".into(), provider)]),
        native_broker::Config::default(),
    )
    .unwrap();
    let socket = host_path.join("broker.sock");
    let serve_path = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    // Keep the owned server task even if a child assertion fails.
    struct ServerGuard {
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
    }
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&serve_path, service, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let mut server = ServerGuard {
        stop: Some(stop),
        task: Some(task),
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                !server.task.as_ref().unwrap().is_finished(),
                "broker exited before ready"
            );
            if fs::metadata(&socket).is_ok_and(|m| m.permissions().mode() & 0o777 == 0o600) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let first = Client::new();
    let second = Client::new();
    let socket_argument = socket.to_str().unwrap();
    for client in [&first, &second] {
        client.success(&[
            "tinytree",
            "init",
            "--broker",
            socket_argument,
            "--store",
            "example",
        ]);
        client.assert_native();
        assert!(
            client
                .root
                .join(".jj/repo/store/native-broker.toml")
                .is_file()
        );
        assert!(!client.root.join(".jj/repo/store/native-objects").exists());
    }
    fs::write(first.root.join("shared.txt"), "first workspace\n").unwrap();
    fs::write(second.root.join("shared.txt"), "second workspace\n").unwrap();
    std::thread::scope(|scope| {
        let a = scope.spawn(|| first.success(&["describe", "-m", "first private operation"]));
        let b = scope.spawn(|| second.success(&["describe", "-m", "second private operation"]));
        a.join().unwrap();
        b.join().unwrap();
    });
    assert_ne!(first.revision("@"), second.revision("@"));
    assert_eq!(
        first.success(&["file", "show", "shared.txt"]),
        "first workspace\n"
    );
    assert_eq!(
        second.success(&["file", "show", "shared.txt"]),
        "second workspace\n"
    );
    // Every command loads the durable locator in a new process; no process opens
    // the authoritative store and no Git program is available on the empty PATH.
    first.success(&["new", "-m", "first private child"]);
    second.success(&["status"]);
    first.success(&["undo"]);
    assert_eq!(
        second.success(&["file", "show", "shared.txt"]),
        "second workspace\n"
    );
    let _ = server.stop.take().unwrap().send(());
    tokio::time::timeout(Duration::from_secs(5), server.task.take().unwrap())
        .await
        .unwrap()
        .unwrap();
}

#[cfg(unix)]
#[derive(Default)]
struct PublicationPause {
    enabled: std::sync::atomic::AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[cfg(unix)]
struct PausingControl {
    inner: jj_native_remote::RemoteRepositoryControl,
    pause: std::sync::Arc<PublicationPause>,
}
#[cfg(unix)]
impl std::fmt::Debug for PausingControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PausingControl")
            .finish_non_exhaustive()
    }
}
#[cfg(unix)]
#[async_trait::async_trait]
impl native_broker::NativeRepositoryControl for PausingControl {
    fn resources(&self) -> native_broker::ControlResources {
        native_broker::NativeRepositoryControl::resources(&self.inner)
    }
    async fn observe_reference(
        &self,
        name: &str,
    ) -> Result<native_broker::ReferenceSnapshot, native_broker::ControlStatus> {
        native_broker::NativeRepositoryControl::observe_reference(&self.inner, name).await
    }
    async fn admit_commit(
        &self,
        head: native_broker::CommitId,
    ) -> Result<native_broker::AdmittedCommit, native_broker::ControlStatus> {
        native_broker::NativeRepositoryControl::admit_commit(&self.inner, head).await
    }
    async fn publish(
        &self,
        request: native_broker::PublishRequest,
    ) -> Result<native_broker::ConfirmedPublication, native_broker::ControlStatus> {
        let confirmed =
            native_broker::NativeRepositoryControl::publish(&self.inner, request).await?;
        // The real remote provider/server have committed and acknowledged; only
        // this broker response is paused for actual child failure injection.
        if self
            .pause
            .enabled
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.pause.started.notify_one();
            self.pause.resume.notified().await;
        }
        Ok(confirmed)
    }
    async fn publication(
        &self,
        key: &str,
    ) -> Result<Option<native_broker::ConfirmedPublication>, native_broker::ControlStatus> {
        native_broker::NativeRepositoryControl::publication(&self.inner, key).await
    }
}

#[cfg(unix)]
struct NativeRemoteFixture {
    _root: tempfile::TempDir,
    socket: PathBuf,
    store: tinytree_store::Store,
    head: String,
    pause: std::sync::Arc<PublicationPause>,
    broker_stop: Option<tokio::sync::oneshot::Sender<()>>,
    server_stop: Option<tokio::sync::oneshot::Sender<()>>,
    broker_task: Option<tokio::task::JoinHandle<()>>,
    server_task: Option<tokio::task::JoinHandle<()>>,
}
#[cfg(unix)]
impl Drop for NativeRemoteFixture {
    fn drop(&mut self) {
        if let Some(stop) = self.broker_stop.take() {
            let _ = stop.send(());
        }
        if let Some(stop) = self.server_stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.broker_task.take() {
            task.abort();
        }
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }
}
#[cfg(unix)]
impl NativeRemoteFixture {
    async fn start() -> Self {
        use std::{collections::HashMap, os::unix::fs::PermissionsExt, sync::Arc};
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().canonicalize().unwrap();
        let server_path = path.join("server");
        fs::create_dir(&server_path).unwrap();
        let store = tinytree_store::Store::open(&server_path, Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let fixture = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
            unused_files: 2,
            bytes_per_file: 64,
        })
        .unwrap();
        let mut head = None;
        for object in fixture.objects() {
            store
                .put("example", object.descriptor, &object.bytes, 0)
                .unwrap();
            if object.descriptor.key.kind == tinytree_model::Kind::Commit {
                head = Some(tinytree_model::CommitId(object.descriptor.key.id.0));
            }
        }
        let head = head.unwrap();
        store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: "seed".into(),
                    reference_name: "main".into(),
                    expected_generation: 0,
                    new_commit: Some(head),
                    staging_leases: vec![],
                },
                0,
            )
            .unwrap();
        let service = tinytree_server::RepositoryService::new(
            store.clone(),
            Default::default(),
            vec!["example".into()],
            vec![
                tinytree_server::Principal::new(
                    "writer",
                    "public-cli-writer-token-not-a-secret",
                    vec!["example".into()],
                    true,
                )
                .unwrap()
                .with_retention(true),
                tinytree_server::Principal::new(
                    "reader",
                    "public-cli-reader-token-not-a-secret",
                    vec!["example".into()],
                    false,
                )
                .unwrap()
                .with_retention(true),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (server_stop, stopped) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        struct NoDefaultSource;
        #[async_trait::async_trait]
        impl local_cas::BlobSource for NoDefaultSource {
            async fn fetch(
                &self,
                _: &local_cas::Artifact,
                _: &mut std::fs::File,
            ) -> anyhow::Result<()> {
                anyhow::bail!("native test must use its scoped source")
            }
            async fn missing(
                &self,
                artifacts: &[local_cas::Artifact],
            ) -> anyhow::Result<Vec<local_cas::Artifact>> {
                Ok(artifacts.to_vec())
            }
        }
        let cache = local_cas::LocalCas::open(
            path.join("cache"),
            Arc::new(NoDefaultSource),
            Default::default(),
        )
        .unwrap();
        let local_path = path.join("local");
        fs::create_dir(&local_path).unwrap();
        fs::set_permissions(&local_path, fs::Permissions::from_mode(0o700)).unwrap();
        let direct = Arc::new(
            jj_native_backend::DirectObjectIo::init(&local_path, Default::default()).unwrap(),
        );
        let mut config = jj_native_remote::Config::default();
        config.remote.endpoint = format!("http://{address}");
        config.remote.repository_id = "example".into();
        config.remote.token_env = Some("TINYTREE_CLI_WRITER_TOKEN".into());
        config.authorization_profile = "cli-writer".into();
        let reader_config = {
            let mut config = config.clone();
            config.remote.token_env = Some("TINYTREE_CLI_READER_TOKEN".into());
            config.authorization_profile = "cli-reader".into();
            config
        };
        let remote = jj_native_remote::RemoteRepository::open(
            direct,
            cache.clone(),
            &path.join("ledger"),
            &path.join("staging"),
            config,
        )
        .unwrap();
        let reader_path = path.join("reader-local");
        fs::create_dir(&reader_path).unwrap();
        fs::set_permissions(&reader_path, fs::Permissions::from_mode(0o700)).unwrap();
        let reader = jj_native_remote::RemoteRepository::open(
            Arc::new(
                jj_native_backend::DirectObjectIo::init(&reader_path, Default::default()).unwrap(),
            ),
            cache,
            &path.join("reader-ledger"),
            &path.join("reader-staging"),
            reader_config,
        )
        .unwrap();
        let reader_provider: Arc<dyn jj_native_backend::NativeObjectIo> = reader.io;
        let reader_control: Arc<dyn native_broker::NativeRepositoryControl> =
            Arc::new(reader.control);
        let provider: Arc<dyn jj_native_backend::NativeObjectIo> = remote.io;
        let pause = Arc::new(PublicationPause::default());
        let control: Arc<dyn native_broker::NativeRepositoryControl> = Arc::new(PausingControl {
            inner: remote.control,
            pause: pause.clone(),
        });
        let broker = native_broker::BrokerService::new(
            HashMap::from([
                ("example".into(), provider),
                ("reader".into(), reader_provider),
            ]),
            Default::default(),
        )
        .unwrap()
        .with_controls(HashMap::from([
            ("example".into(), control),
            ("reader".into(), reader_control),
        ]))
        .unwrap();
        let socket = path.join("broker.sock");
        let serve_path = socket.clone();
        let (broker_stop, stopped) = tokio::sync::oneshot::channel();
        let broker_task = tokio::spawn(async move {
            native_broker::serve_unix(&serve_path, broker, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        let fixture = Self {
            _root: root,
            socket,
            store,
            head: head.0.iter().map(|b| format!("{b:02x}")).collect(),
            pause,
            broker_stop: Some(broker_stop),
            server_stop: Some(server_stop),
            broker_task: Some(broker_task),
            server_task: Some(server_task),
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    !fixture.broker_task.as_ref().unwrap().is_finished(),
                    "native broker startup failed"
                );
                if fs::metadata(&fixture.socket)
                    .is_ok_and(|metadata| metadata.permissions().mode() & 0o777 == 0o600)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        fixture
    }
    fn public(&self) -> tinytree_store::Reference {
        self.store
            .references("example", &["main".into()])
            .unwrap()
            .remove(0)
    }
    fn clone(&self, client: &Client) {
        self.clone_store(client, "example");
    }
    fn clone_store(&self, client: &Client, store: &str) {
        client.success(&[
            "tinytree",
            "clone",
            ".",
            "--broker",
            self.socket.to_str().unwrap(),
            "--store",
            store,
            "--reference",
            "main",
            "--bookmark",
            "main",
        ]);
    }
    async fn stop(mut self) {
        let _ = self.broker_stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.broker_task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        let _ = self.server_stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.server_task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_native_remote_cli_preserves_workspace_generations_conflicts_and_undo() {
    let fixture = NativeRemoteFixture::start().await;
    let first = Client::new();
    let second = Client::new();
    fixture.clone(&first);
    fixture.clone(&second);
    for client in [&first, &second] {
        client.assert_native();
        assert_eq!(client.revision("main"), fixture.head);
        assert_eq!(client.revision("@-"), fixture.head);
    }
    first.success(&["bookmark", "set", "created", "-r", "@"]);
    first.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "created",
        "--bookmark",
        "created",
    ]);
    first.success(&[
        "tinytree",
        "push",
        "--reference",
        "created",
        "--bookmark",
        "created",
    ]);
    assert_eq!(
        fixture
            .store
            .references("example", &["created".into()])
            .unwrap()[0]
            .generation,
        1,
        "an explicitly observed absent reference carries generation zero into its first publication"
    );
    fs::write(second.root.join("agent.txt"), "client B data\n").unwrap();
    second.success(&["describe", "-m", "client B change"]);
    second.success(&["bookmark", "set", "main", "-r", "@"]);
    let bhead = second.revision("@");
    second.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 2);
    assert_eq!(
        fixture
            .public()
            .commit
            .unwrap()
            .key
            .id
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        bhead
    );
    let sidecar = second.root.join(".jj/working_copy/native-remote.toml");
    let observed = fs::read(&sidecar).unwrap();
    second.success(&["undo"]);
    assert_eq!(
        fs::read(&sidecar).unwrap(),
        observed,
        "jj undo cannot roll back an external observed generation"
    );
    let rejected = second.run(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert!(!rejected.status.success());
    assert_eq!(fixture.public().generation, 2);
    second.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    second.success(&[
        "bookmark",
        "set",
        "main",
        "--allow-backwards",
        "-r",
        &fixture.head,
    ]);
    second.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 3);
    assert_eq!(first.revision("main"), fixture.head);
    let old = fs::read(first.root.join(".jj/working_copy/native-remote.toml")).unwrap();
    let stale = first.run(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert!(
        !stale.status.success(),
        "same head after ABA still requires the workspace's old generation"
    );
    assert_eq!(fixture.public().generation, 3);
    assert_eq!(
        fs::read(first.root.join(".jj/working_copy/native-remote.toml")).unwrap(),
        old
    );
    first.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    fs::write(first.root.join("agent.txt"), "client A data\n").unwrap();
    first.success(&["describe", "-m", "client A change"]);
    first.success(&["bookmark", "set", "main", "-r", "@"]);
    let ahead = first.revision("@");
    first.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 4);
    second.success(&["bookmark", "set", "main", "--allow-backwards", "-r", &bhead]);
    second.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(
        second.success(&["file", "show", "agent.txt"]),
        "client B data\n",
        "fetch must preserve the independent working copy"
    );
    let conflict = second.run(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert!(
        !conflict.status.success(),
        "divergent bookmarks must merge, not silently overwrite local work"
    );
    assert_eq!(fixture.public().generation, 4);
    second.success(&["rebase", "-r", "@", "-d", &ahead]);
    assert!(second.success(&["status"]).contains("conflict"));
    let rebased = second.revision("@");
    second.success(&["bookmark", "set", "main", "--allow-backwards", "-r", "@"]);
    second.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 5);
    let third = Client::new();
    fixture.clone(&third);
    assert_eq!(
        third.revision("main"),
        rebased,
        "native tree conflicts survive publication and another materialized clone"
    );
    assert!(
        third
            .success(&["file", "show", "agent.txt"])
            .contains("<<<<<<<")
    );
    assert_eq!(
        first.success(&["file", "show", "agent.txt"]),
        "client A data\n"
    );
    fixture.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_publication_recovery_reuses_one_intent_after_client_death_or_local_state_failure() {
    enum Failure {
        ClientDies,
        StateCommitCollision,
    }
    for failure in [Failure::ClientDies, Failure::StateCommitCollision] {
        let fixture = NativeRemoteFixture::start().await;
        let client = Client::new();
        let quoted_bookmark = "\"feature name\"";
        client.success(&[
            "tinytree",
            "clone",
            ".",
            "--broker",
            fixture.socket.to_str().unwrap(),
            "--store",
            "example",
            "--reference",
            "main",
            "--bookmark",
            quoted_bookmark,
        ]);
        fs::write(
            client.root.join("recovery.txt"),
            "durable publication data\n",
        )
        .unwrap();
        client.success(&["describe", "-m", "recover exact native intent"]);
        client.success(&["bookmark", "set", quoted_bookmark, "-r", "@"]);
        let head = client.revision("@");
        fixture
            .pause
            .enabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let args = [
            "tinytree",
            "push",
            "--reference",
            "main",
            "--bookmark",
            quoted_bookmark,
        ];
        let mut child = client.spawn(&args);
        tokio::time::timeout(Duration::from_secs(10), fixture.pause.started.notified())
            .await
            .unwrap();
        assert_eq!(fixture.public().generation, 2);
        let sidecar = client.root.join(".jj/working_copy/native-remote.toml");
        assert!(fs::read_to_string(&sidecar).unwrap().contains("request_id"));
        match failure {
            Failure::ClientDies => {
                assert!(child.0.as_mut().unwrap().try_wait().unwrap().is_none());
                child.0.as_mut().unwrap().kill().unwrap();
                assert!(!child.0.take().unwrap().wait().unwrap().success());
                fixture.pause.resume.notify_one();
            }
            Failure::StateCommitCollision => {
                let saved = sidecar.with_extension("saved");
                fs::rename(&sidecar, &saved).unwrap();
                fs::create_dir(&sidecar).unwrap();
                fixture.pause.resume.notify_one();
                let result = client.wait(child, &args);
                assert!(
                    !result.status.success(),
                    "local durable-state failure must not report completion"
                );
                fs::remove_dir(&sidecar).unwrap();
                fs::rename(saved, &sidecar).unwrap();
            }
        }
        client.success(&args);
        assert_eq!(
            fixture.public().generation,
            2,
            "recovery must not create a second publication generation"
        );
        assert_eq!(client.revision("\"feature name\"@tinytree"), head);
        assert!(!fs::read_to_string(&sidecar).unwrap().contains("request_id"));
        fixture.stop().await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readonly_failed_push_requires_explicit_bounded_local_replay_abandonment() {
    let fixture = NativeRemoteFixture::start().await;
    let client = Client::new();
    fixture.clone_store(&client, "reader");
    fs::write(
        client.root.join("local-only.txt"),
        "readonly clients can edit locally\n",
    )
    .unwrap();
    client.success(&["describe", "-m", "local readonly work"]);
    client.success(&["bookmark", "set", "main", "-r", "@"]);
    let push = [
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ];
    assert!(!client.run(&push).status.success());
    assert_eq!(fixture.public().generation, 1);
    let fetch = [
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ];
    assert!(
        !client.run(&fetch).status.success(),
        "default recovery must not assume a failed publication never committed"
    );
    let path = client.root.join(".jj/working_copy/native-remote.toml");
    let original = fs::read_to_string(&path).unwrap();
    let mut boundary: toml::Value = toml::from_str(&original).unwrap();
    let intent = boundary["pending"].clone();
    let active = client.roots();
    assert!(
        active["intent_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id.as_str() == intent["base_operation"].as_str())
    );
    assert!(
        active["object_roots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|root| root["kind"] == "commit" && root["id"].as_str() == intent["head"].as_str())
    );
    let mut missing_active = boundary.clone();
    missing_active["pending"]["base_operation"] = toml::Value::String("ff".repeat(64));
    fs::write(&path, toml::to_string(&missing_active).unwrap()).unwrap();
    let rejected = client.run(&["tinytree", "roots", "--json"]);
    assert!(
        !rejected.status.success(),
        "active recovery requires its base operation"
    );
    assert!(rejected.stdout.is_empty());
    fs::write(&path, &original).unwrap();
    let archived = (0..128)
        .map(|index| {
            let mut intent = intent.clone();
            intent["request_id"] = toml::Value::String(format!("{index:032x}"));
            intent
        })
        .collect();
    boundary["archived_pushes"] = toml::Value::Array(archived);
    fs::write(&path, toml::to_string(&boundary).unwrap()).unwrap();
    let full = fs::read(&path).unwrap();
    let abandon = [
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
        "--abandon-pending-push",
    ];
    assert!(!client.run(&abandon).status.success());
    assert_eq!(
        fs::read(&path).unwrap(),
        full,
        "a full archive must preserve the active replay intent without mutation"
    );
    fs::write(&path, &original).unwrap();
    client.success(&abandon);
    let state: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    assert!(state.get("pending").is_none());
    assert_eq!(state["archived_pushes"].as_array().unwrap().len(), 1);
    // Archival retains the publication head, not an indefinitely required jj
    // operation DAG. Operation GC may already have expired an archived base.
    let mut expired_archive = state.clone();
    expired_archive["archived_pushes"][0]["base_operation"] = toml::Value::String("ff".repeat(64));
    fs::write(&path, toml::to_string(&expired_archive).unwrap()).unwrap();
    let archived_audit = client.roots();
    assert!(
        archived_audit["intent_operations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        archived_audit["object_roots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|root| root["kind"] == "commit" && root["id"].as_str() == intent["head"].as_str())
    );
    assert_eq!(
        client.success(&["file", "show", "local-only.txt"]),
        "readonly clients can edit locally\n"
    );
    assert_eq!(
        fixture.public().generation,
        1,
        "abandoning local replay is not an upstream write or rollback"
    );
    fixture.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_fetch_preflights_observation_capacity_before_snapshot_or_pending_state() {
    let fixture = NativeRemoteFixture::start().await;
    let client = Client::new();
    fixture.clone(&client);
    let path = client.root.join(".jj/working_copy/native-remote.toml");
    let mut state: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let observations = state["observations"].as_array_mut().unwrap();
    for index in 0..127 {
        let mut entry = toml::map::Map::new();
        entry.insert(
            "reference".into(),
            toml::Value::String(format!("boundary-{index}")),
        );
        entry.insert(
            "bookmark".into(),
            toml::Value::String(format!("boundary-{index}")),
        );
        entry.insert("generation".into(), toml::Value::Integer(0));
        observations.push(toml::Value::Table(entry));
    }
    fs::write(&path, toml::to_string(&state).unwrap()).unwrap();
    let saved = fs::read(&path).unwrap();
    let before = client.revision("@");
    fs::write(
        client.root.join("must-not-snapshot.txt"),
        "pending ordinary edit\n",
    )
    .unwrap();
    let rejected = client.run(&[
        "tinytree",
        "fetch",
        "--reference",
        "one-too-many",
        "--bookmark",
        "extra",
    ]);
    assert!(!rejected.status.success());
    assert_eq!(
        fs::read(&path).unwrap(),
        saved,
        "admission failure must not leave a pending fetch"
    );
    assert_eq!(
        client
            .success(&[
                "--ignore-working-copy",
                "log",
                "--no-graph",
                "-r",
                "@",
                "-T",
                "commit_id"
            ])
            .trim(),
        before,
        "capacity must be checked before jj snapshots or commits local work"
    );
    assert_eq!(fixture.public().generation, 1);
    fixture.stop().await;
}
