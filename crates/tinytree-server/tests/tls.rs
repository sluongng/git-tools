//! Actual configured TLS listener with synthetic public test-only identities.
use std::{net::SocketAddr, path::PathBuf, time::Duration};

use tinytree_proto::v1::{OpenRepositoryRequest, repository_client::RepositoryClient};
use tinytree_server::{Config, PrincipalConfig, TlsConfig};
use tokio::sync::oneshot;
use tonic::{
    Code, Request,
    codec::CompressionEncoding,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

const TOKEN_ENV: &str = "TINYTREE_PUBLIC_TLS_TEST_TOKEN";
const TOKEN: &str = "public-tls-test-token-not-a-secret";

fn endpoint(address: SocketAddr, ca: &[u8], domain: &str) -> Endpoint {
    Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2))
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name(domain),
        )
        .unwrap()
}

struct Running {
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}
impl Running {
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .expect("TLS server did not drain")
            .unwrap()
            .unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn request(repository: &str, token: Option<&str>) -> Request<OpenRepositoryRequest> {
    let mut request = Request::new(OpenRepositoryRequest {
        repository_id: repository.into(),
        reference_names: vec!["main".into()],
    });
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_tls_authenticates_principals_and_verifies_server_identity() {
    // This value is injected at test process startup by the declared Bazel rule,
    // avoiding mutation of process environment while server threads are running.
    assert_eq!(std::env::var(TOKEN_ENV).unwrap(), TOKEN);
    let directory = tempfile::tempdir().unwrap();
    let port_reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = port_reservation.local_addr().unwrap();
    let config = Config {
        storage: directory.path().join("native-storage"),
        listen: address.to_string(),
        repositories: vec!["alpha".into(), "beta".into()],
        principals: vec![PrincipalConfig {
            id: "alpha-reader".into(),
            token_env: TOKEN_ENV.into(),
            repositories: vec!["alpha".into()],
            write: false,
            retain: false,
            cloud: false,
        }],
        tls: Some(TlsConfig {
            certificate: PathBuf::from(env!("TLS_SERVER_CERT"))
                .canonicalize()
                .unwrap(),
            private_key: PathBuf::from(env!("TLS_SERVER_KEY"))
                .canonicalize()
                .unwrap(),
        }),
        gc_interval_seconds: 0,
        ..Config::default()
    };
    let ca = std::fs::read(env!("TLS_CA_CERT")).unwrap();
    let wrong_ca = std::fs::read(env!("TLS_WRONG_CA_CERT")).unwrap();
    let trusted_endpoint = endpoint(address, &ca, "localhost");
    let (stop, stopped) = oneshot::channel();
    drop(port_reservation);
    let mut running = Running {
        stop: Some(stop),
        task: Some(tokio::spawn(tinytree_server::serve(config, async {
            let _ = stopped.await;
        }))),
    };
    let channel = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if running.task.as_ref().unwrap().is_finished() {
                let result = running.task.take().unwrap().await;
                panic!("configured TLS server rejected startup: {result:?}");
            }
            if let Ok(channel) = trusted_endpoint.clone().connect().await {
                break channel;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("configured TLS server did not become ready");
    let mut client: RepositoryClient<Channel> = RepositoryClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    let view = client
        .open_repository(request("alpha", Some(TOKEN)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(view.repository_id, "alpha");
    assert_eq!(view.protocol_major, 1);
    assert_eq!(view.references.len(), 1);
    assert!(view.references[0].commit.is_none());
    assert_eq!(
        client
            .open_repository(request("alpha", None))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated,
    );
    assert_eq!(
        client
            .open_repository(request("alpha", Some("wrong-public-test-token")))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated,
    );
    assert_eq!(
        client
            .open_repository(request("beta", Some(TOKEN)))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            endpoint(address, &wrong_ca, "localhost").connect(),
        )
        .await
        .expect("wrong-CA connection timed out instead of rejecting")
        .is_err(),
        "a client trusting an unrelated CA accepted the server",
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            endpoint(address, &ca, "wrong.example.invalid").connect(),
        )
        .await
        .expect("wrong-name connection timed out instead of rejecting")
        .is_err(),
        "a client requiring an unrelated DNS name accepted the server",
    );
    client
        .open_repository(request("alpha", Some(TOKEN)))
        .await
        .unwrap();
    drop(client);
    running.stop().await;
}
