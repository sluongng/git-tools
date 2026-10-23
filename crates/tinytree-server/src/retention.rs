//! Wire names omit ownership; the authenticated stable principal supplies it.
use crate::auth::Identity;
use tinytree_model::CommitId;
use tinytree_store::{RetentionOwner, retention_reference};
use tonic::Status;

const WIRE_PREFIX: &str = "_tinytree/hold/v1/";

pub(crate) enum ReferenceScope {
    Public(String),
    Hold {
        owner: RetentionOwner,
        commit: CommitId,
    },
}
impl ReferenceScope {
    pub fn parse(name: &str, identity: &Identity) -> Result<Self, Status> {
        if let Some(suffix) = name.strip_prefix(WIRE_PREFIX) {
            let (client, commit) = suffix
                .split_once('/')
                .ok_or_else(|| Status::invalid_argument("invalid native retention reference"))?;
            let client = decode_hex::<16>(client)?;
            let commit = CommitId(decode_hex::<32>(commit)?);
            if commit == CommitId::ROOT {
                return Err(Status::invalid_argument(
                    "intrinsic root cannot be retained",
                ));
            }
            return Ok(Self::Hold {
                owner: RetentionOwner {
                    principal: owner_hash(identity),
                    client,
                },
                commit,
            });
        }
        if reserved(name) {
            return Err(Status::permission_denied(
                "native internal reference namespace is reserved",
            ));
        }
        Ok(Self::Public(name.to_owned()))
    }

    pub fn stored_name(&self) -> Result<String, Status> {
        match self {
            Self::Public(name) => Ok(name.clone()),
            Self::Hold { owner, commit } => retention_reference(*owner, *commit)
                .map_err(|_| Status::invalid_argument("invalid native retention reference")),
        }
    }
}

pub(crate) fn wire_name(name: &str, identity: &Identity) -> Result<String, Status> {
    if let Some((owner, commit)) = tinytree_store::parse_retention_reference(name) {
        if owner.principal != owner_hash(identity) {
            return Err(Status::permission_denied("native retention access denied"));
        }
        return Ok(format!(
            "{WIRE_PREFIX}{}/{}",
            hex(&owner.client),
            hex(&commit.0)
        ));
    }
    if reserved(name) {
        return Err(Status::data_loss(
            "invalid stored native retention reference",
        ));
    }
    Ok(name.to_owned())
}

pub(crate) fn owner_hash(identity: &Identity) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-retention-owner-v1\0");
    hash.update(&(identity.id.len() as u64).to_be_bytes());
    hash.update(identity.id.as_bytes());
    *hash.finalize().as_bytes()
}
fn reserved(name: &str) -> bool {
    name == "_tinytree" || name.starts_with("_tinytree/")
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn decode_hex<const N: usize>(text: &str) -> Result<[u8; N], Status> {
    if text.len() != 2 * N
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Status::invalid_argument(
            "invalid native retention reference",
        ));
    }
    let mut bytes = [0; N];
    for (out, pair) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *out = (digit(pair[0]) << 4) | digit(pair[1]);
    }
    Ok(bytes)
}
fn digit(byte: u8) -> u8 {
    if byte <= b'9' {
        byte - b'0'
    } else {
        byte - b'a' + 10
    }
}
