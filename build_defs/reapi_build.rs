// Bazel supplies protoc as a declared execution-platform tool. Cargo's build.rs
// instead selects protoc-bin-vendored; keep the protocol inputs identical.
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
    tonic_prost_build::configure().compile_with_config(
        prost,
        &[
            std::path::PathBuf::from(
                "proto/build/bazel/remote/execution/v2/remote_execution.proto",
            ),
            std::path::PathBuf::from("proto/google/bytestream/bytestream.proto"),
        ],
        &[std::path::PathBuf::from("proto"), include.to_path_buf()],
    )?;
    Ok(())
}
