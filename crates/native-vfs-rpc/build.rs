fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(std::env::var_os("PROTOC").ok_or("PROTOC is required")?);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(
            prost,
            &["proto/native/vfs/v1/projection.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/native/vfs/v1/projection.proto");
    Ok(())
}
