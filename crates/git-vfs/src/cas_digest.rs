use anyhow::{Context, Result, bail, ensure};
use reqwest::{Url, blocking::Client, redirect::Policy};
use serde::{Deserialize, Serialize};
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
    ensure!(
        !alias.is_empty()
            && alias.len() <= 128
            && alias
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
        "proxy repository alias must be 1-128 ASCII letters, digits, '_' or '-'"
    );
    let mut url = Url::parse(origin).context("invalid proxy URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/"
            && !origin.chars().any(|c| c.is_whitespace() || c == '\\'),
        "proxy URL must be an HTTP(S) origin without credentials, query, fragment, or path prefix"
    );
    // Reject paths that URL parsing would normalize back to the origin root.
    let authority = origin.split_once("://").context("invalid proxy origin")?.1;
    ensure!(
        !authority.contains('@')
            && authority
                .split_once('/')
                .is_none_or(|(_, suffix)| suffix.is_empty()),
        "proxy URL must not contain credentials or a path prefix"
    );
    url.set_path(&format!("/api/v1/repos/{alias}/objects"));
    let client = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building proxy HTTP client")?;
    let response = client
        .post(url)
        .json(&serde_json::json!({"oids": [oid], "blake3": true}))
        .send()
        .context("querying proxy digest mapping")?;
    ensure!(
        response.status().is_success(),
        "proxy digest request returned HTTP {}",
        response.status()
    );
    const MAX_RESPONSE: u64 = 64 * 1024;
    let mut bytes = Vec::new();
    response
        .take(MAX_RESPONSE + 1)
        .read_to_end(&mut bytes)
        .context("reading proxy digest response")?;
    ensure!(
        bytes.len() as u64 <= MAX_RESPONSE,
        "proxy response too large"
    );
    let mut objects: Objects =
        serde_json::from_slice(&bytes).context("invalid proxy digest response")?;
    ensure!(
        objects.objects.len() == 1,
        "proxy must return exactly the requested object"
    );
    let object = objects.objects.remove(0);
    ensure!(
        object.oid == oid,
        "proxy returned a different Git object ID"
    );
    ensure!(object.object_type == "blob", "proxy object is not a blob");
    let size_bytes = i64::try_from(object.size_bytes).context("blob exceeds REAPI size limit")?;
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
    Ok(CasDigest {
        hash: digest.hash.to_ascii_lowercase(),
        size_bytes,
        digest_function: "BLAKE3",
        git_sha1: oid.to_owned(),
    })
}
