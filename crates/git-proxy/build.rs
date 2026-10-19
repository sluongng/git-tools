fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        prost,
        &[
            std::path::PathBuf::from(
                "proto/build/bazel/remote/execution/v2/remote_execution.proto",
            ),
            std::path::PathBuf::from("proto/google/bytestream/bytestream.proto"),
        ],
        &[
            std::path::PathBuf::from("proto"),
            protoc_bin_vendored::include_path()?,
        ],
    )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
