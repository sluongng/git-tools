fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
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
                std::path::PathBuf::from("../reapi/proto"),
                protoc_bin_vendored::include_path()?,
            ],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
