//! Owner-scoped discovery using only a daemon-configured remote provider.
//! Summaries do not promise retention; restore re-verifies the selected capsule.
use crate::{control::*, native_capture_upload::Source};
use anyhow::{Result, ensure};
use std::sync::Arc;
use tinytree_client::{CloudRawDigest, CloudWorkspace, NativeClient};
use tokio::time::Instant;

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn bytes(text: &str, expected: usize) -> Result<Vec<u8>> {
    ensure!(
        text.len() == expected * 2
            && text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "cloud selectors require canonical lowercase hexadecimal data"
    );
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}
pub(crate) fn workspace(text: &str) -> Result<[u8; 32]> {
    let id: [u8; 32] = bytes(text, 32)?.try_into().unwrap();
    ensure!(id != [0; 32], "cloud workspace identity must be nonzero");
    Ok(id)
}
fn page(page_size: u16, continuation: &str) -> Result<Vec<u8>> {
    ensure!(
        (1..=128).contains(&page_size),
        "cloud page size must be 1..128"
    );
    if continuation.is_empty() {
        Ok(Vec::new())
    } else {
        bytes(continuation, 74)
    }
}
async fn client(source: &Arc<Source>, deadline: Instant) -> Result<NativeClient> {
    source
        .control
        .client_for_owned_work(source.clone(), deadline)
        .await
}
fn row(value: CloudWorkspace) -> NativeCloudWorkspace {
    NativeCloudWorkspace {
        workspace_id: hex(&value.workspace_id),
        generation: value.generation,
        manifest_id: value.manifest_id.map(|id| hex(&id.0)),
        local_destination: None,
    }
}
pub(crate) async fn workspaces(
    source: Arc<Source>,
    page_size: u16,
    continuation: &str,
    deadline: Instant,
) -> Result<NativeCloudWorkspaces> {
    let continuation = page(page_size, continuation)?;
    let value = client(&source, deadline)
        .await?
        .cloud_workspaces(page_size as usize, &continuation)
        .await?;
    Ok(NativeCloudWorkspaces {
        workspaces: value.workspaces.into_iter().map(row).collect(),
        continuation: hex(&value.continuation),
    })
}
pub(crate) async fn versions(
    source: Arc<Source>,
    id: &str,
    page_size: u16,
    continuation: &str,
    deadline: Instant,
) -> Result<NativeCloudVersions> {
    let id = workspace(id)?;
    let continuation = page(page_size, continuation)?;
    let value = client(&source, deadline)
        .await?
        .cloud_versions(id, page_size as usize, &continuation)
        .await?;
    Ok(NativeCloudVersions {
        workspace: row(value.workspace),
        versions: value
            .versions
            .into_iter()
            .map(|value| NativeCloudVersion {
                generation: value.generation,
                manifest_id: hex(&value.manifest_id.0),
                previous_manifest_id: value.previous_manifest_id.map(|id| hex(&id.0)),
            })
            .collect(),
        through_generation: value.through_generation,
        continuation: hex(&value.continuation),
    })
}
pub(crate) async fn select(
    source: Arc<Source>,
    id: &str,
    generation: Option<u64>,
    deadline: Instant,
) -> Result<NativeRestoreSelection> {
    let id = workspace(id)?;
    ensure!(generation != Some(0), "cloud generation must be positive");
    let client = client(&source, deadline).await?;
    let version = match generation {
        Some(generation) => client.cloud_version(id, generation).await?,
        None => client.cloud_latest_captured_version(id).await?,
    };
    let generation = version.generation;
    // This is an exact, hash-verified selector. The existing restore collector
    // verifies v3 structure, capture DATA and retained dependencies before writes.
    let raw = CloudRawDigest::of(version.manifest());
    ensure!(raw.size_bytes > 0, "cloud version manifest is empty");
    Ok(NativeRestoreSelection {
        workspace_id: id,
        generation,
        manifest_id: version.manifest_id.0,
        manifest_blake3: raw.blake3,
        manifest_size_bytes: raw.size_bytes,
    })
}
