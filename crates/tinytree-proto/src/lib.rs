//! Experimental native repository transport, independent of Git serving.
//!
//! Protobuf messages describe requests and canonical object envelopes; protobuf
//! serialization is not the canonical native object encoding or hash domain.
//! These bindings provide identity/zstd negotiation, not server transport policy,
//! authentication, canonical-form validation, or durable publication by themselves.

pub mod v1 {
    tonic::include_proto!("tinytree.v1");
}

/// Descriptor set for the native-only protocol, including streaming cardinality.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("tinytree_descriptor");

#[cfg(test)]
mod tests {
    use super::{FILE_DESCRIPTOR_SET, v1};
    use prost::Message;
    use tonic::{codec::CompressionEncoding, transport::Channel};

    #[test]
    fn native_repository_contract_preserves_rpc_streaming_and_public_bindings() {
        let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
        assert_eq!(descriptors.file.len(), 1);
        let file = &descriptors.file[0];
        assert_eq!(file.package.as_deref(), Some("tinytree.v1"));
        assert!(file.dependency.is_empty());
        assert_eq!(file.service.len(), 1);
        let repository = &file.service[0];
        assert_eq!(repository.name.as_deref(), Some("Repository"));
        let methods: Vec<_> = repository
            .method
            .iter()
            .map(|method| {
                (
                    method.name.as_deref().unwrap(),
                    method.client_streaming.unwrap_or(false),
                    method.server_streaming.unwrap_or(false),
                )
            })
            .collect();
        assert_eq!(
            methods,
            [
                ("OpenRepository", false, false),
                ("GetCommitGraph", false, false),
                ("BatchReadObjects", false, true),
                ("FindMissingObjects", false, false),
                ("ReadFile", false, true),
                ("WriteObject", true, false),
                ("PublishReference", false, false),
                ("GetPublication", false, false),
            ]
        );

        // Check the public generated builders without constructing a mock
        // Repository implementation or a runtime/network connection.
        fn client_compression(
            client: v1::repository_client::RepositoryClient<Channel>,
        ) -> v1::repository_client::RepositoryClient<Channel> {
            client
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        #[allow(dead_code)]
        fn server_compression<T: v1::repository_server::Repository>(
            service: T,
        ) -> v1::repository_server::RepositoryServer<T> {
            v1::repository_server::RepositoryServer::new(service)
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        let _ = client_compression;
    }
}
