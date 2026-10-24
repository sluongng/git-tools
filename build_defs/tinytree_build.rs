// Bazel supplies protoc as a declared execution-platform tool. No host PATH
// discovery or protobuf serialization is used to define native object IDs.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(std::env::var_os("PROTOC").ok_or("PROTOC is required")?);
    let output =
        std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is required")?);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(output.join("tinytree_descriptor.bin"))
        .compile_with_config(
            prost,
            &[
                "proto/tinytree/v1/repository.proto",
                "proto/tinytree/query/v1/query.proto",
                "proto/tinytree/cloud/v1/cloud.proto",
                "proto/tinytree/landing/v1/landing.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/tinytree/v1/repository.proto");
    println!("cargo:rerun-if-changed=proto/tinytree/query/v1/query.proto");
    println!("cargo:rerun-if-changed=proto/tinytree/cloud/v1/cloud.proto");
    println!("cargo:rerun-if-changed=proto/tinytree/landing/v1/landing.proto");
    Ok(())
}
