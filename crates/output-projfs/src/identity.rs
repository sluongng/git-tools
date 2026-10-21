//! Stable placeholder identities. Remote content is independent of inode lifetime.
use anyhow::{Result, ensure};
use output_core::Artifact;
use serde::{Deserialize, Serialize};

pub const LEGACY_PROVIDER: &[u8; 16] = b"cas-output-v0001";
pub const LOCATOR_PROVIDER: &[u8; 16] = b"cas-output-v0002";
pub const MAX_ORIGINS: usize = 256;
pub const MAX_ORIGIN_TEXT_BYTES: usize = 4096;
const MAGIC: &[u8; 8] = b"CASLOC02";

/// A provider-private immutable origin key. Retain its full security scope and
/// instance in the durable manifest, never reconstruct them from a digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    pub key: [u8; 16],
    pub backend: String,
    pub instance: String,
}

pub fn validate_origins(origins: &[Origin]) -> Result<()> {
    ensure!(origins.len() <= MAX_ORIGINS, "too many placeholder origins");
    for (index, origin) in origins.iter().enumerate() {
        ensure!(origin.key != [0; 16], "empty placeholder origin key");
        ensure!(!origin.backend.is_empty(), "empty placeholder backend");
        ensure!(
            origin.backend.len() <= MAX_ORIGIN_TEXT_BYTES
                && origin.instance.len() <= MAX_ORIGIN_TEXT_BYTES,
            "placeholder origin is too long"
        );
        ensure!(
            !origins[..index].iter().any(|other| other.key == origin.key
                || (other.backend == origin.backend && other.instance == origin.instance)),
            "duplicate placeholder origin"
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Content {
    Legacy {
        node: u64,
        generation: u64,
    },
    Remote {
        node: u64,
        generation: u64,
        origin: [u8; 16],
        digest_function: i32,
        hash: String,
        size: u64,
    },
}

impl Content {
    pub fn remote(
        node: u64,
        generation: u64,
        origin: &Origin,
        artifact: &Artifact,
    ) -> Result<Self> {
        artifact.validate()?;
        ensure!(node > 1 && generation != 0, "invalid placeholder inode");
        ensure!(origin.key != [0; 16], "empty placeholder origin key");
        ensure!(
            artifact.backend == origin.backend && artifact.instance == origin.instance,
            "placeholder origin does not match artifact"
        );
        ensure!(artifact.size <= i64::MAX as u64, "placeholder is too large");
        Ok(Self::Remote {
            node,
            generation,
            origin: origin.key,
            digest_function: artifact.digest_function,
            hash: artifact.hash.clone(),
            size: artifact.size,
        })
    }

    pub fn encode(&self) -> Result<([u8; 128], [u8; 128])> {
        let mut provider = [0; 128];
        let mut content = [0; 128];
        match self {
            Self::Legacy { node, generation } => {
                ensure!(
                    *node != 0 && *generation != 0,
                    "invalid legacy placeholder inode"
                );
                provider[..16].copy_from_slice(LEGACY_PROVIDER);
                content[..8].copy_from_slice(&node.to_le_bytes());
                content[8..16].copy_from_slice(&generation.to_le_bytes());
            }
            Self::Remote {
                node,
                generation,
                origin,
                digest_function,
                hash,
                size,
            } => {
                let check = Artifact {
                    backend: String::new(),
                    instance: String::new(),
                    digest_function: *digest_function,
                    hash: hash.clone(),
                    size: *size,
                };
                check.validate()?;
                ensure!(
                    *node > 1 && *generation != 0 && *origin != [0; 16],
                    "invalid remote placeholder identity"
                );
                ensure!(*size <= i64::MAX as u64, "placeholder is too large");
                provider[..16].copy_from_slice(LOCATOR_PROVIDER);
                content[..8].copy_from_slice(MAGIC);
                content[8..16].copy_from_slice(&node.to_le_bytes());
                content[16..24].copy_from_slice(&generation.to_le_bytes());
                content[24..40].copy_from_slice(origin);
                content[40..44].copy_from_slice(&digest_function.to_le_bytes());
                content[44..52].copy_from_slice(&size.to_le_bytes());
                for (output, hex) in content[52..84]
                    .iter_mut()
                    .zip(hash.as_bytes().chunks_exact(2))
                {
                    fn nibble(byte: u8) -> u8 {
                        if byte <= b'9' {
                            byte - b'0'
                        } else {
                            byte - b'a' + 10
                        }
                    }
                    *output = nibble(hex[0]) * 16 + nibble(hex[1]);
                }
            }
        }
        Ok((provider, content))
    }

    pub fn decode(provider: &[u8; 128], content: &[u8; 128]) -> Result<Self> {
        ensure!(
            provider[16..].iter().all(|byte| *byte == 0),
            "unknown provider extension"
        );
        if &provider[..16] == LEGACY_PROVIDER {
            ensure!(
                content[16..].iter().all(|byte| *byte == 0),
                "unknown legacy identity extension"
            );
            let node = u64::from_le_bytes(content[..8].try_into().unwrap());
            let generation = u64::from_le_bytes(content[8..16].try_into().unwrap());
            ensure!(
                node != 0 && generation != 0,
                "invalid legacy placeholder inode"
            );
            return Ok(Self::Legacy { node, generation });
        }
        ensure!(
            &provider[..16] == LOCATOR_PROVIDER && &content[..8] == MAGIC,
            "unknown placeholder format"
        );
        ensure!(
            content[84..].iter().all(|byte| *byte == 0),
            "unknown locator extension"
        );
        let result = Self::Remote {
            node: u64::from_le_bytes(content[8..16].try_into().unwrap()),
            generation: u64::from_le_bytes(content[16..24].try_into().unwrap()),
            origin: content[24..40].try_into().unwrap(),
            digest_function: i32::from_le_bytes(content[40..44].try_into().unwrap()),
            size: u64::from_le_bytes(content[44..52].try_into().unwrap()),
            hash: content[52..84]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        };
        // One validation path rejects unsupported functions, invalid IDs and
        // file sizes before any cache request or native asynchronous admission.
        result.encode()?;
        Ok(result)
    }

    pub fn artifact(&self, origins: &[Origin]) -> Result<Artifact> {
        validate_origins(origins)?;
        let Self::Remote {
            origin,
            digest_function,
            hash,
            size,
            ..
        } = self
        else {
            anyhow::bail!("legacy identity requires retained inode metadata");
        };
        let origin = origins
            .iter()
            .find(|candidate| candidate.key == *origin)
            .ok_or_else(|| anyhow::anyhow!("unknown immutable placeholder origin"))?;
        let artifact = Artifact {
            backend: origin.backend.clone(),
            instance: origin.instance.clone(),
            digest_function: *digest_function,
            hash: hash.clone(),
            size: *size,
        };
        artifact.validate()?;
        Ok(artifact)
    }
}
