//! Pinned Remote Execution API and ByteStream protocol bindings.
#[allow(clippy::doc_lazy_continuation)]
pub mod proto {
    pub mod google {
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }
        pub mod longrunning {
            tonic::include_proto!("google.longrunning");
        }
        pub mod bytestream {
            tonic::include_proto!("google.bytestream");
        }
    }
    pub mod build {
        pub mod bazel {
            pub mod semver {
                tonic::include_proto!("build.bazel.semver");
            }
            pub mod remote {
                pub mod execution {
                    pub mod v2 {
                        tonic::include_proto!("build.bazel.remote.execution.v2");
                    }
                }
            }
        }
    }
}
pub use proto::build::bazel::remote::execution::v2 as reapi;
pub use reapi::Digest;
