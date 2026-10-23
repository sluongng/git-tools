//! Experimental native repository transport, independent of Git serving.
//!
//! Protobuf messages describe requests and canonical object envelopes; protobuf
//! serialization is not the canonical native object encoding or hash domain.
//! These bindings provide identity/zstd negotiation, not server transport policy,
//! authentication, canonical-form validation, or durable publication by themselves.

// Preserve protobuf package nesting for generated cross-package type paths.
pub mod tinytree {
    pub mod cloud {
        pub mod v1 {
            tonic::include_proto!("tinytree.cloud.v1");
        }
    }
    pub mod v1 {
        tonic::include_proto!("tinytree.v1");
    }
    pub mod query {
        pub mod v1 {
            tonic::include_proto!("tinytree.query.v1");
        }
    }
}
pub use tinytree::{cloud::v1 as cloud_v1, query::v1 as query_v1, v1};

/// Descriptor set for the native-only protocol, including streaming cardinality.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("tinytree_descriptor");

#[cfg(test)]
mod tests {
    use super::{FILE_DESCRIPTOR_SET, query_v1, v1};
    use prost::Message;
    use tonic::{codec::CompressionEncoding, transport::Channel};

    #[test]
    fn native_repository_contract_preserves_rpc_streaming_and_public_bindings() {
        let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
        assert_eq!(descriptors.file.len(), 3);
        let file = descriptors
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("tinytree/v1/repository.proto"))
            .unwrap();
        assert_eq!(file.package.as_deref(), Some("tinytree.v1"));
        assert!(file.dependency.is_empty());
        assert_eq!(file.service.len(), 1);
        let repository = file
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("Repository"))
            .unwrap();
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

        let query_file = descriptors
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("tinytree/query/v1/query.proto"))
            .unwrap();
        assert_eq!(query_file.package.as_deref(), Some("tinytree.query.v1"));
        assert_eq!(query_file.dependency, ["tinytree/v1/repository.proto"]);
        assert_eq!(query_file.service.len(), 1);
        let query = query_file
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("RepositoryQuery"))
            .unwrap();
        let query_methods: Vec<_> = query
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
            query_methods,
            [
                ("OpenScope", true, false),
                ("ForkScope", true, false),
                ("CloseScope", false, false),
                ("QueryFacts", false, false),
                ("ResolveCommitPrefix", false, false),
                ("ResolveChangePrefix", false, false),
                ("ShortestPrefix", false, false),
                ("Evaluate", false, false),
                ("CloseQuery", false, false),
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
        fn query_client_compression(
            client: query_v1::repository_query_client::RepositoryQueryClient<Channel>,
        ) -> query_v1::repository_query_client::RepositoryQueryClient<Channel> {
            client
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        #[allow(dead_code)]
        fn query_server_compression<T: query_v1::repository_query_server::RepositoryQuery>(
            service: T,
        ) -> query_v1::repository_query_server::RepositoryQueryServer<T> {
            query_v1::repository_query_server::RepositoryQueryServer::new(service)
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        let _ = (client_compression, query_client_compression);
    }
    #[test]
    fn query_bindings_preserve_false_answers_and_explicit_stream_completion() {
        let answer = query_v1::GraphFactAnswer {
            answer: Some(query_v1::graph_fact_answer::Answer::HasId(false)),
        };
        let bytes = answer.encode_to_vec();
        assert!(!bytes.is_empty());
        let decoded = query_v1::GraphFactAnswer::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.answer, answer.answer);
        let page = query_v1::EvaluatePage {
            exhausted: Some(false),
            ..Default::default()
        };
        let decoded = query_v1::EvaluatePage::decode(page.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.exhausted, Some(false));
        assert_ne!(
            decoded.exhausted,
            query_v1::EvaluatePage::default().exhausted
        );
        let marker = query_v1::OpenScopeFrame {
            frame: Some(query_v1::open_scope_frame::Frame::ScopeEnd(
                query_v1::ScopeEnd::default(),
            )),
        };
        let decoded = query_v1::OpenScopeFrame::decode(marker.encode_to_vec().as_slice()).unwrap();
        assert!(matches!(
            decoded.frame,
            Some(query_v1::open_scope_frame::Frame::ScopeEnd(_))
        ));

        // Explicit empty traversal intervals and Latest(0) must not become
        // unbounded intervals/counts while crossing the wire.
        use query_v1::graph_expr::Expression;
        let range = query_v1::GraphExpr {
            version: 1,
            expression: Some(Expression::Range(Box::new(query_v1::RangeExpr {
                parents: Some(query_v1::ParentRange {
                    start: 0,
                    end_exclusive: Some(0),
                }),
                ..Default::default()
            }))),
        };
        let latest = query_v1::GraphExpr {
            version: 1,
            expression: Some(Expression::Latest(Box::new(query_v1::LatestExpr {
                candidates: Some(Box::new(range)),
                count: 0,
            }))),
        };
        assert_eq!(
            query_v1::GraphExpr::decode(latest.encode_to_vec().as_slice()).unwrap(),
            latest
        );
        let conflict = query_v1::GraphExpr {
            version: 1,
            expression: Some(Expression::HasConflict(Box::new(query_v1::UnaryExpr {
                input: Some(Box::new(latest)),
            }))),
        };
        assert_eq!(
            query_v1::GraphExpr::decode(conflict.encode_to_vec().as_slice()).unwrap(),
            conflict
        );
        let dag_range = query_v1::DagRangeExpr {
            generations_from_roots: Some(query_v1::GenerationRange {
                start: 0,
                end_exclusive: Some(0),
            }),
            ..Default::default()
        };
        let decoded = query_v1::DagRangeExpr::decode(dag_range.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, dag_range);
        assert_ne!(
            decoded.generations_from_roots,
            query_v1::DagRangeExpr::default().generations_from_roots
        );
    }
}
