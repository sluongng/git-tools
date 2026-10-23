use futures::{AsyncReadExt, executor::block_on};
use jj_lib::{
    backend::{Backend, CopyHistory},
    object_id::ObjectId,
    repo_path::{RepoPath, RepoPathBuf},
};
use jj_native_backend::{CopyCatalogLimits, NativeBackend, NativeObjectIo};
use native_broker::{BrokerObjectIo, Config};
use std::{
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
fn payload(salt: &str) -> Vec<u8> {
    (0..320 * 1024)
        .map(|n| ((n * 19 + n / 251 + salt.len() * 31) % 251) as u8)
        .collect()
}
fn parse(text: &str) -> [u8; 32] {
    let mut out = [0; 32];
    for (i, chunk) in text.as_bytes().chunks_exact(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
    }
    out
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
struct Never;
impl futures::AsyncRead for Never {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let socket = PathBuf::from(&args[1]);
    let spool = PathBuf::from(&args[2]);
    let mode = &args[3];
    let mut config = Config {
        max_metadata_bytes: 1024 * 1024,
        max_file_bytes: 16 * 1024 * 1024,
        max_catalog_bytes: 4 * 1024 * 1024,
        max_catalog_objects: 1000,
        max_memory_bytes: 32 * 1024 * 1024,
        max_temporary_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };
    if mode == "cancel" {
        config.policy.operation_timeout_ms = 100;
        config.policy.stream_idle_timeout_ms = 100;
    }
    block_on(async {
        let provider = Arc::new(
            BrokerObjectIo::connect(
                &socket,
                if mode == "unknown" {
                    "unknown"
                } else {
                    "example"
                },
                &spool,
                config.clone(),
            )
            .await
            .unwrap(),
        );
        if mode == "unknown" {
            assert!(
                provider
                    .descriptor(tinytree_model::empty_tree().descriptor.key)
                    .await
                    .is_err()
            );
            println!("UNKNOWN_DENIED");
            return;
        }
        if mode == "read" {
            let id = tinytree_model::FileId(parse(&args[4]));
            let (_, mut reader) = provider.read_file(id).await.unwrap().into_parts();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, payload(&args[5]));
            println!("READ_OK");
            return;
        }
        let provider = if mode == "cancel" {
            assert!(provider.write_file(&mut Never, 1024).await.is_err());
            drop(provider);
            config.policy.operation_timeout_ms = 10_000;
            config.policy.stream_idle_timeout_ms = 10_000;
            Arc::new(
                BrokerObjectIo::connect(&socket, "example", &spool, config)
                    .await
                    .unwrap(),
            )
        } else {
            provider
        };
        let mut backend_config = jj_native_backend::Config::default();
        backend_config.store.max_file_bytes = 16 * 1024 * 1024;
        backend_config.store.max_staging_bytes = 32 * 1024 * 1024;
        let backend = NativeBackend::from_io(provider.clone(), backend_config)
            .await
            .unwrap();
        let bytes = payload(mode);
        let file = backend
            .write_file(RepoPath::root(), &mut futures::io::Cursor::new(&bytes))
            .await
            .unwrap();
        let copy = backend
            .write_copy(&CopyHistory {
                current_path: RepoPathBuf::from_internal_string(format!("{mode}.txt")).unwrap(),
                parents: vec![],
                salt: mode.as_bytes().to_vec(),
            })
            .await
            .unwrap();
        let mut reader = backend.read_file(RepoPath::root(), &file).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(bytes, actual);
        let catalog = provider
            .copy_catalog_snapshot(CopyCatalogLimits {
                max_objects: 1000,
                max_metadata_bytes: 4 * 1024 * 1024,
                max_edges: 10000,
            })
            .await
            .unwrap()
            .into_entries();
        assert!(
            catalog
                .iter()
                .any(|entry| entry.descriptor().key.id.0.as_slice() == copy.as_bytes())
        );
        println!("OK {} {}", hex(file.as_bytes()), hex(copy.as_bytes()));
    });
}
