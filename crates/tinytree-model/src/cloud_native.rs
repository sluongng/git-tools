//! Shared normalization for authenticated retained-version source bindings.
//! Hashing these values does not authenticate an owner or authorize an object.
pub fn cloud_native_binding_digest(
    repository: &str,
    owner_binding: [u8; 32],
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    manifest_blake3: [u8; 32],
    manifest_size: u64,
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-cloud-native-binding-v1\0");
    hash.update(&(repository.len() as u64).to_be_bytes());
    hash.update(repository.as_bytes());
    hash.update(&owner_binding);
    hash.update(&workspace);
    hash.update(&generation.to_be_bytes());
    hash.update(&manifest);
    hash.update(&manifest_blake3);
    hash.update(&manifest_size.to_be_bytes());
    *hash.finalize().as_bytes()
}
