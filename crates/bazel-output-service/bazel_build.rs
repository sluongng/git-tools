// Bazel supplies protoc as a declared execution-platform tool. Keep the
// protocol inputs and extern type mappings identical to Cargo's build.rs.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(std::env::var_os("PROTOC").ok_or("PROTOC is required")?);
    let descriptor = std::path::PathBuf::from(
        std::env::var_os("PROTOC_DESCRIPTOR").ok_or("PROTOC_DESCRIPTOR is required")?,
    );
    let include = descriptor
        .parent()
        .and_then(std::path::Path::parent)
        .and_then(std::path::Path::parent)
        .ok_or("invalid protoc descriptor path")?;
    // Out-of-package data is not laid out as Cargo workspace siblings in the
    // build-script manifest directory. rules_rs expands every declared execpath
    // to an execution-root absolute path, including on remote workers.
    let imports = std::env::var("PROTO_IMPORT_FILES")?;
    let status = imports
        .split_whitespace()
        .find(|path| path.ends_with("/google/rpc/status.proto"))
        .ok_or("declared google/rpc/status.proto input is missing")?;
    let import_root = std::path::Path::new(status)
        .parent()
        .and_then(std::path::Path::parent)
        .and_then(std::path::Path::parent)
        .ok_or("invalid REAPI import path")?;
    tonic_prost_build::configure()
        .extern_path(".google.rpc", "::reapi::proto::google::rpc")
        .extern_path(".build.bazel.remote.execution.v2", "::reapi::reapi")
        .compile_with_config(
            prost,
            &[
                std::path::PathBuf::from("proto/bazel_output_service.proto"),
                std::path::PathBuf::from("proto/bazel_output_service_rev2.proto"),
            ],
            &[
                std::path::PathBuf::from("proto"),
                import_root.to_path_buf(),
                include.to_path_buf(),
            ],
        )?;
    Ok(())
}
