use anyhow::{Context, Result, bail, ensure};
use reqwest::{Url, blocking::Client, redirect::Policy};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::time::Duration;

#[derive(Deserialize)]
struct Objects {
    objects: Vec<Object>,
}

#[derive(Deserialize)]
struct Object {
    oid: String,
    size_bytes: u64,
    object_type: String,
    blake3: Option<Blake3>,
}

#[derive(Deserialize)]
struct Blake3 {
    hash: String,
    size_bytes: u64,
}

#[derive(Serialize)]
pub struct CasDigest {
    hash: String,
    size_bytes: i64,
    digest_function: &'static str,
    git_sha1: String,
}

/// Resolve a trusted proxy's content mapping, without accessing local blob bytes.
pub fn lookup(origin: &str, alias: &str, oid: &str) -> Result<CasDigest> {
    let mappings = lookup_batch(origin, alias, &[oid.to_owned()])?;
    let (size, hash) = mappings[oid];
    Ok(CasDigest {
        hash: hex::encode(hash),
        size_bytes: size as i64,
        digest_function: "BLAKE3",
        git_sha1: oid.to_owned(),
    })
}

pub(crate) fn endpoint(origin: &str, alias: &str) -> Result<Url> {
    ensure!(
        !alias.is_empty()
            && alias.len() <= 128
            && alias
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "proxy repository alias must be 1-128 ASCII letters, digits, '_' or '-'"
    );
    let mut url = Url::parse(origin).context("invalid proxy URL")?;
    // The shared daemon protects loopback HTTP Git/metadata routes with an
    // unguessable capability. General remote path prefixes remain unsupported.
    let capability = url.path().trim_matches('/');
    let local_capability = url.scheme() == "http"
        && url
            .host_str()
            .and_then(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .ok()
            })
            .is_some_and(|ip| ip.is_loopback())
        && capability.len() == 64
        && capability.bytes().all(|byte| byte.is_ascii_hexdigit())
        && matches!(url.path().strip_prefix('/'), Some(path) if path == capability || path == format!("{capability}/"));
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && (url.path() == "/" || local_capability)
            && !origin.chars().any(|c| c.is_whitespace() || c == '\\'),
        "proxy URL must be an HTTP(S) origin or a private daemon loopback capability"
    );
    // Reject paths that URL parsing would normalize back to the origin root.
    let authority = origin.split_once("://").context("invalid proxy origin")?.1;
    ensure!(
        !authority.contains('@')
            && authority
                .split_once('/')
                .is_none_or(|(_, suffix)| suffix.is_empty()
                    || (local_capability
                        && (suffix == capability || suffix == format!("{capability}/")))),
        "proxy URL must not contain credentials or a path prefix"
    );
    let prefix = if local_capability {
        format!("/{capability}")
    } else {
        String::new()
    };
    url.set_path(&format!("{prefix}/api/v1/repos/{alias}/objects"));
    Ok(url)
}

// Bound cold-start work per request independently of the server admission cap.
pub const BATCH_SIZE: usize = 1024;

pub fn lookup_batch(
    origin: &str,
    alias: &str,
    oids: &[String],
) -> Result<HashMap<String, (u64, [u8; 32])>> {
    ensure!(
        !oids.is_empty() && oids.len() <= BATCH_SIZE,
        "invalid proxy mapping batch size"
    );
    let requested: HashSet<_> = oids.iter().collect();
    ensure!(
        requested.len() == oids.len(),
        "duplicate requested object ID"
    );
    for oid in oids {
        ensure!(
            oid.len() == 40
                && oid
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "expected canonical SHA1 ID"
        );
    }
    let url = endpoint(origin, alias)?;
    let client = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building proxy HTTP client")?;
    let response = client
        .post(url)
        .json(&serde_json::json!({"oids": oids, "blake3": true}))
        .send()
        .context("querying proxy digest mapping")?;
    ensure!(
        response.status().is_success(),
        "proxy digest request returned HTTP {}",
        response.status()
    );
    const MAX_RESPONSE: u64 = 8 * 1024 * 1024;
    let mut bytes = Vec::new();
    response
        .take(MAX_RESPONSE + 1)
        .read_to_end(&mut bytes)
        .context("reading proxy digest response")?;
    ensure!(
        bytes.len() as u64 <= MAX_RESPONSE,
        "proxy response too large"
    );
    let objects: Objects =
        serde_json::from_slice(&bytes).context("invalid proxy digest response")?;
    ensure!(
        objects.objects.len() == oids.len(),
        "proxy must return exactly the requested objects"
    );
    let mut result = HashMap::new();
    for object in objects.objects {
        ensure!(
            requested.contains(&object.oid),
            "proxy returned a different Git object ID"
        );
        ensure!(object.object_type == "blob", "proxy object is not a blob");
        ensure!(
            object.size_bytes <= i64::MAX as u64,
            "blob exceeds REAPI size limit"
        );
        let Some(digest) = object.blake3 else {
            bail!("proxy returned no BLAKE3 mapping; enable --enable-blake3 on the proxy");
        };
        ensure!(
            digest.size_bytes == object.size_bytes,
            "BLAKE3 mapping size differs from Git blob size"
        );
        ensure!(
            digest.hash.len() == 64 && digest.hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "proxy BLAKE3 hash must contain 64 hexadecimal characters"
        );
        let mut hash = [0; 32];
        hex::decode_to_slice(&digest.hash, &mut hash)?;
        ensure!(
            result
                .insert(object.oid, (object.size_bytes, hash))
                .is_none(),
            "proxy returned a duplicate object ID"
        );
    }
    Ok(result)
}
