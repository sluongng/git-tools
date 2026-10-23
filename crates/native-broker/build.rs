fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(std::env::var_os("PROTOC").ok_or("PROTOC is required")?);
    // The build-script manifest directory is not the repository layout.
    // Bazel supplies this declared schema input through the action execroot.
    let query = std::path::PathBuf::from(
        std::env::var_os("TINYTREE_QUERY_PROTO").ok_or("TINYTREE_QUERY_PROTO is required")?,
    );
    let shared = query
        .ancestors()
        .nth(4)
        .ok_or("invalid shared schema input")?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".tinytree.query.v1", "::tinytree_proto::query_v1")
        .extern_path(".tinytree.v1", "::tinytree_proto::v1")
        .compile_with_config(
            prost,
            &[std::path::Path::new(
                "proto/native/broker/v1/object_io.proto",
            )],
            &[std::path::Path::new("proto"), shared],
        )?;
    println!("cargo:rerun-if-changed=proto/native/broker/v1/object_io.proto");
    Ok(())
}
