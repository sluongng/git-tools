//! Same-origin, read-only browser facade. Native storage remains authoritative;
//! bounded previews report incomplete coverage instead of a false empty result.
use super::*;
use axum::{
    Router,
    body::to_bytes,
    extract::{Request as HttpRequest, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response as HttpResponse},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::net::IpAddr;
use tinytree_model::TreeId;
use tonic::service::Routes;

const MAX_BODY: usize = 16 * 1024;
const MAX_FILE: usize = 1024 * 1024;
const MAX_PATHS: usize = 10_000;
const MAX_PATH_BYTES: usize = 1024 * 1024;
const MAX_READ: usize = 8 * 1024 * 1024;
const MAX_HITS: usize = 100;
const MAX_DEPTH: usize = 64;

#[derive(Clone)]
struct WebState {
    service: RepositoryService,
    port: Option<u16>,
}

pub(crate) fn routes(service: RepositoryService, port: Option<u16>) -> Routes {
    let grpc = Routes::new(service.grpc_service())
        .add_service(service.query_grpc_service())
        .add_service(service.cloud_grpc_service())
        .add_service(service.landing_grpc_service())
        .into_axum_router();
    grpc.merge(browser_routes(service, port)).into()
}

fn browser_routes(service: RepositoryService, port: Option<u16>) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { axum::response::Redirect::temporary("/ui/") }),
        )
        .route(
            "/ui",
            get(|| async { axum::response::Redirect::temporary("/ui/") }),
        )
        .route("/ui/", get(index))
        .route("/ui/index.html", get(index))
        .route("/ui/app.js", get(script))
        .route("/ui/style.css", get(styles))
        .route("/ui/tinytree_ui.wasm", get(wasm))
        .route("/ui/api/repositories", post(repositories))
        .route("/ui/api/snapshot", post(snapshot))
        .route("/ui/api/file", post(file))
        .route("/ui/api/search", post(search))
        .with_state(WebState { service, port })
}

fn asset(content_type: &'static str, bytes: &'static [u8]) -> HttpResponse {
    let mut response = ([(header::CONTENT_TYPE, content_type)], bytes).into_response();
    security_headers(response.headers_mut());
    response
}
async fn index() -> HttpResponse {
    asset(
        "text/html; charset=utf-8",
        include_bytes!(env!("TINYTREE_UI_HTML")),
    )
}
async fn script() -> HttpResponse {
    asset(
        "text/javascript; charset=utf-8",
        include_bytes!(env!("TINYTREE_UI_JS")),
    )
}
async fn styles() -> HttpResponse {
    asset(
        "text/css; charset=utf-8",
        include_bytes!(env!("TINYTREE_UI_CSS")),
    )
}
async fn wasm() -> HttpResponse {
    asset("application/wasm", include_bytes!(env!("TINYTREE_UI_WASM")))
}
fn security_headers(headers: &mut HeaderMap) {
    headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    headers.insert("referrer-policy", "no-referrer".parse().unwrap());
    headers.insert("content-security-policy", "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; connect-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'".parse().unwrap());
}

fn error(status: Status) -> HttpResponse {
    let code = match status.code() {
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::Unimplemented | tonic::Code::FailedPrecondition => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = (code, axum::Json(json!({"error":status.message()}))).into_response();
    security_headers(response.headers_mut());
    response
}
fn json_response(value: Value) -> HttpResponse {
    let mut response = axum::Json(value).into_response();
    security_headers(response.headers_mut());
    response
}

// No cookies, ambient credentials, CORS or token-bearing URLs. Requiring JSON
// and checking Origin/Fetch Metadata blocks cross-site form and fetch reads.
// Anonymous local access additionally rejects rebinding through a foreign Host.
fn browser_identity(
    state: &WebState,
    headers: &HeaderMap,
    uri: &http::Uri,
) -> Result<Identity, Status> {
    let header_host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let uri_host = uri.authority().map(|a| a.as_str());
    if header_host
        .zip(uri_host)
        .is_some_and(|(header, uri)| header != uri)
    {
        return Err(Status::permission_denied("inconsistent browser authority"));
    }
    let host = uri_host
        .or(header_host)
        .ok_or_else(|| Status::permission_denied("browser Host required"))?;
    let authority: http::uri::Authority = host
        .parse()
        .map_err(|_| Status::permission_denied("invalid browser Host"))?;
    if !state.service.authentication_enabled() {
        let name = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']');
        let loopback = name.eq_ignore_ascii_case("localhost")
            || name.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
        if !loopback
            || state.port.is_some_and(|port| {
                authority.port_u16() != Some(port)
                    && !(authority.port_u16().is_none() && matches!(port, 80 | 443))
            })
        {
            return Err(Status::permission_denied("local browser Host required"));
        }
    }
    if headers
        .get("sec-fetch-site")
        .is_some_and(|v| v != "same-origin" && v != "none")
    {
        return Err(Status::permission_denied(
            "same-origin browser request required",
        ));
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin: http::Uri = origin
            .to_str()
            .ok()
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Status::permission_denied("invalid browser Origin"))?;
        if !matches!(origin.scheme_str(), Some("http" | "https"))
            || origin.authority().map(|a| a.as_str()) != Some(host)
            || origin.path_and_query().is_some_and(|v| v.as_str() != "/")
        {
            return Err(Status::permission_denied(
                "same-origin browser request required",
            ));
        }
    }
    let metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
    state.service.inner.auth.identify(&metadata)
}
async fn input<T: serde::de::DeserializeOwned>(
    state: &WebState,
    request: HttpRequest,
) -> Result<(Identity, Instant, T, OwnedSemaphorePermit), Status> {
    let identity = browser_identity(state, request.headers(), request.uri())?;
    if request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(str::trim)
        != Some("application/json")
    {
        return Err(Status::invalid_argument(
            "browser API requires application/json",
        ));
    }
    let deadline =
        Instant::now() + Duration::from_millis(state.service.inner.config.rpc.rpc_timeout_ms);
    // Admission precedes body polling/JSON allocation, including repository listing.
    let global = state.service.global(deadline).await?;
    let bytes = tokio::time::timeout_at(
        deadline,
        to_bytes(
            request.into_body(),
            MAX_BODY.min(state.service.inner.config.max_request_bytes),
        ),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("browser request deadline exceeded"))?
    .map_err(|_| Status::resource_exhausted("browser request exceeds byte budget"))?;
    let input = serde_json::from_slice(&bytes)
        .map_err(|_| Status::invalid_argument("invalid browser JSON request"))?;
    Ok((identity, deadline, input, global))
}

async fn repositories(State(state): State<WebState>, request: HttpRequest) -> HttpResponse {
    match input::<serde_json::Map<String, Value>>(&state, request).await {
        Ok((identity, deadline, _, global)) => {
            let listing_bytes = state
                .service
                .inner
                .repositories
                .keys()
                .map(|repository| repository.len().saturating_add(128))
                .sum::<usize>()
                .saturating_mul(4)
                .saturating_add(1024);
            let memory = match state.service.memory(listing_bytes, deadline).await {
                Ok(memory) => memory,
                Err(status) => return error(status),
            };
            let mut repositories = state
                .service
                .inner
                .repositories
                .keys()
                .filter(|repo| identity.authorize(repo, false).is_ok())
                .cloned()
                .collect::<Vec<_>>();
            repositories.sort();
            let mut response = json_response(json!({"repositories":repositories}));
            response
                .extensions_mut()
                .insert(crate::body_deadline::ResponseBudget::new((global, memory)));
            response
        }
        Err(status) => error(status),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotRequest {
    repository: String,
    #[serde(default = "default_reference")]
    reference: String,
    revision: Option<String>,
}
fn default_reference() -> String {
    "main".into()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRequest {
    repository: String,
    revision: String,
    path: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequest {
    repository: String,
    revision: String,
    query: String,
}

async fn work<F>(
    state: &WebState,
    identity: Identity,
    repository: String,
    deadline: Instant,
    global: OwnedSemaphorePermit,
    action: F,
) -> Result<HttpResponse, Status>
where
    F: FnOnce(tinytree_store::Store, String, Instant) -> Result<Value, Status> + Send + 'static,
{
    // Retain both permits in physical work, including after caller cancellation.
    identity.authorize(&repository, false)?;
    let repository_permit = state.service.repository(&repository, deadline).await?;
    let admission = Arc::new(Admission {
        _global: global,
        _repository: repository_permit,
    });
    let memory = state
        .service
        .memory(MAX_READ * 4 + 16 * 1024 * 1024, deadline)
        .await?;
    let store = state.service.inner.store.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _held = (admission, memory);
        check(deadline)?;
        let value = action(store, repository, deadline)?;
        let mut response = json_response(value);
        response
            .extensions_mut()
            .insert(crate::body_deadline::ResponseBudget::new(_held));
        Ok(response)
    });
    tokio::time::timeout_at(deadline, result)
        .await
        .map_err(|_| Status::deadline_exceeded("browser read deadline exceeded"))?
        .map_err(|_| Status::internal("browser read worker failed"))?
}
async fn snapshot(State(state): State<WebState>, request: HttpRequest) -> HttpResponse {
    let result = async {
        let (identity, deadline, request, global) = input::<SnapshotRequest>(&state, request).await?;
        work(&state, identity, request.repository, deadline, global, move |store, repository, deadline| {
            let revision = match request.revision {
                Some(revision) => parse_revision(&revision)?,
                None => {
                    // Browser browsing does not expose owner-private hold names.
                    if request.reference.starts_with("_tinytree") || request.reference.len() > 256 {
                        return Err(Status::invalid_argument("invalid public reference"));
                    }
                    store.references(&repository, &[request.reference.clone()]).map_err(store_status)?
                        .first().and_then(|r| r.commit).map(|d| CommitId(d.key.id.0))
                        .ok_or_else(|| Status::not_found("reference has no published revision"))?
                }
            };
            let view = View::open(store, repository.clone(), revision, deadline)?;
            // Read the root README before walking optional autocomplete paths.
            let readme_file = view.readme()?;
            let (readme, readme_error) = match readme_file {
                Some(file) => match view.text(&file) {
                    Ok(content) => (Some(json!({"path":file.path,"content":content})), None),
                    Err(status) if matches!(status.code(), tonic::Code::ResourceExhausted | tonic::Code::Unimplemented) => (None, Some(status.message().to_owned())),
                    Err(status) => return Err(status),
                },
                None => (None, None),
            };
            let (files, complete) = view.files()?;
            Ok(json!({"repository":repository,"reference":request.reference,"revision":hex(revision.0),
                "paths":files.iter().map(|f| &f.path).collect::<Vec<_>>(),"paths_complete":complete,"readme":readme,"readme_error":readme_error}))
        }).await
    }.await;
    match result {
        Ok(response) => response,
        Err(status) => error(status),
    }
}
async fn file(State(state): State<WebState>, request: HttpRequest) -> HttpResponse {
    let result = async {
        let (identity, deadline, request, global) = input::<FileRequest>(&state, request).await?;
        work(
            &state,
            identity,
            request.repository,
            deadline,
            global,
            move |store, repository, deadline| {
                let revision = parse_revision(&request.revision)?;
                let view = View::open(store, repository, revision, deadline)?;
                let file = view.path(&request.path)?;
                Ok(json!({"path":file.path,"content":view.text(&file)?}))
            },
        )
        .await
    }
    .await;
    match result {
        Ok(response) => response,
        Err(status) => error(status),
    }
}
async fn search(State(state): State<WebState>, request: HttpRequest) -> HttpResponse {
    let result = async {
        let (identity, deadline, request, global) = input::<SearchRequest>(&state, request).await?;
        work(&state, identity, request.repository, deadline, global, move |store, repository, deadline| {
            if request.query.is_empty() || request.query.len() > 256 || request.query.contains(['\n', '\r', '\0']) {
                return Err(Status::invalid_argument("search requires a single-line literal of 1–256 bytes"));
            }
            let revision = parse_revision(&request.revision)?;
            let view = View::open(store, repository, revision, deadline)?;
            let (files, mut complete) = view.files()?;
            let mut read = 0usize;
            let mut matches = Vec::new();
            'files: for file in files {
                check(deadline)?;
                if file.size > MAX_FILE as u64 || read.saturating_add(file.size as usize) > MAX_READ {
                    complete = false;
                    continue;
                }
                read += file.size as usize;
                let bytes = view.bytes(&file)?;
                // Binary/non-UTF-8 files are outside the browser text-search domain.
                let Ok(text) = std::str::from_utf8(&bytes) else { continue; };
                if text.contains('\0') { continue; }
                for (line_index, line) in text.lines().enumerate() {
                    check(deadline)?;
                    if let Some(column) = line.find(&request.query) {
                        let start = line[..column].char_indices().rev().nth(80).map(|(i,_)| i).unwrap_or(0);
                        let end = line[column..].char_indices().nth(240).map(|(i,_)| column+i).unwrap_or(line.len());
                        matches.push(json!({"path":file.path,"line":line_index+1,"column":column+1,"preview":&line[start..end]}));
                        if matches.len() == MAX_HITS { complete = false; break 'files; }
                    }
                }
            }
            Ok(json!({"matches":matches,"complete":complete}))
        }).await
    }.await;
    match result {
        Ok(response) => response,
        Err(status) => error(status),
    }
}

fn check(deadline: Instant) -> Result<(), Status> {
    if Instant::now() >= deadline {
        Err(Status::deadline_exceeded("browser read deadline exceeded"))
    } else {
        Ok(())
    }
}
fn hex(bytes: [u8; 32]) -> String {
    blake3::Hash::from(bytes).to_hex().to_string()
}
fn parse_revision(value: &str) -> Result<CommitId, Status> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Status::invalid_argument(
            "revision must be a lowercase 64-digit commit ID",
        ));
    }
    let hash =
        blake3::Hash::from_hex(value).map_err(|_| Status::invalid_argument("invalid revision"))?;
    Ok(CommitId(*hash.as_bytes()))
}
#[derive(Clone)]
struct BrowserFile {
    path: String,
    id: tinytree_model::FileId,
    size: u64,
}
struct View {
    store: tinytree_store::Store,
    repository: String,
    root: TreeId,
    _pin: tinytree_store::RootReadGuard,
    deadline: Instant,
}
impl View {
    fn open(
        store: tinytree_store::Store,
        repository: String,
        revision: CommitId,
        deadline: Instant,
    ) -> Result<Self, Status> {
        let key = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(revision.0),
        };
        let pin = store.pin_root(&repository, key).map_err(store_status)?;
        let object = metadata(&store, &repository, key)?;
        let Object::Commit(commit) = object else {
            return Err(Status::data_loss("invalid revision object"));
        };
        if commit.root_tree.terms.len() != 1 {
            return Err(Status::unimplemented(
                "browser preview requires a resolved root tree",
            ));
        }
        Ok(Self {
            store,
            repository,
            root: commit.root_tree.terms[0],
            _pin: pin,
            deadline,
        })
    }
    fn tree(&self, id: TreeId, read: &mut usize) -> Result<tinytree_model::Tree, Status> {
        check(self.deadline)?;
        let key = ObjectKey {
            kind: Kind::Tree,
            id: ObjectId(id.0),
        };
        let descriptor = self
            .store
            .descriptor(&self.repository, key)
            .map_err(store_status)?
            .ok_or_else(|| Status::not_found("tree unavailable"))?;
        *read = read.saturating_add(usize::try_from(descriptor.size_bytes).unwrap_or(usize::MAX));
        if *read > MAX_READ {
            return Err(Status::resource_exhausted(
                "browser tree read budget exceeded",
            ));
        }
        match metadata(&self.store, &self.repository, key)? {
            Object::Tree(tree) => Ok(tree),
            _ => Err(Status::data_loss("invalid tree object")),
        }
    }
    fn readme(&self) -> Result<Option<BrowserFile>, Status> {
        let tree = self.tree(self.root, &mut 0)?;
        for name in ["readme.md", "readme.markdown", "readme.txt", "readme"] {
            if let Some(entry) = tree
                .entries
                .iter()
                .find(|entry| entry.name.eq_ignore_ascii_case(name))
            {
                if let tinytree_model::TreeValue::File { id, size_bytes, .. } = &entry.value {
                    return Ok(Some(BrowserFile {
                        path: entry.name.clone(),
                        id: *id,
                        size: *size_bytes,
                    }));
                }
            }
        }
        Ok(None)
    }
    fn files(&self) -> Result<(Vec<BrowserFile>, bool), Status> {
        let mut stack = vec![(String::new(), self.root, 0usize)];
        let mut files = Vec::new();
        let mut path_bytes = 0usize;
        let mut read = 0usize;
        let mut complete = true;
        let mut entries = 0usize;
        while let Some((prefix, id, depth)) = stack.pop() {
            if depth > MAX_DEPTH {
                complete = false;
                continue;
            }
            let tree = match self.tree(id, &mut read) {
                Ok(tree) => tree,
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    complete = false;
                    break;
                }
                Err(status) => return Err(status),
            };
            for entry in tree.entries.into_iter().rev() {
                check(self.deadline)?;
                entries += 1;
                if entries > MAX_PATHS {
                    complete = false;
                    break;
                }
                let path = if prefix.is_empty() {
                    entry.name
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                path_bytes = path_bytes.saturating_add(path.len());
                if path.len() > 4096 || path_bytes > MAX_PATH_BYTES {
                    complete = false;
                    break;
                }
                match entry.value {
                    tinytree_model::TreeValue::File { id, size_bytes, .. } => {
                        files.push(BrowserFile {
                            path,
                            id,
                            size: size_bytes,
                        })
                    }
                    tinytree_model::TreeValue::Tree(id) => stack.push((path, id, depth + 1)),
                    tinytree_model::TreeValue::Symlink { .. } => {}
                }
            }
            if entries > MAX_PATHS || path_bytes > MAX_PATH_BYTES {
                break;
            }
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok((files, complete))
    }
    fn path(&self, path: &str) -> Result<BrowserFile, Status> {
        if path.is_empty()
            || path.len() > 4096
            || path.split('/').count() > MAX_DEPTH
            || path.split('/').any(|p| {
                p.is_empty() || p == "." || p == ".." || p.contains('\\') || p.contains('\0')
            })
        {
            return Err(Status::invalid_argument("invalid repository file path"));
        }
        let mut id = self.root;
        let mut read = 0usize;
        let mut parts = path.split('/').peekable();
        while let Some(part) = parts.next() {
            let tree = self.tree(id, &mut read)?;
            let entry = tree
                .entries
                .into_iter()
                .find(|e| e.name == part)
                .ok_or_else(|| Status::not_found("file path unavailable"))?;
            match entry.value {
                tinytree_model::TreeValue::Tree(child) if parts.peek().is_some() => id = child,
                tinytree_model::TreeValue::File { id, size_bytes, .. }
                    if parts.peek().is_none() =>
                {
                    return Ok(BrowserFile {
                        path: path.into(),
                        id,
                        size: size_bytes,
                    });
                }
                _ => return Err(Status::not_found("regular file path unavailable")),
            }
        }
        Err(Status::not_found("file path unavailable"))
    }
    fn bytes(&self, file: &BrowserFile) -> Result<Vec<u8>, Status> {
        check(self.deadline)?;
        if file.size > MAX_FILE as u64 {
            return Err(Status::resource_exhausted(
                "browser file exceeds 1 MiB preview limit",
            ));
        }
        let key = ObjectKey {
            kind: Kind::File,
            id: ObjectId(file.id.0),
        };
        let mut lease = self
            .store
            .read(&self.repository, key)
            .map_err(store_status)?;
        if lease.descriptor().size_bytes != file.size {
            return Err(Status::data_loss("file size mismatch"));
        }
        let mut bytes = Vec::with_capacity(file.size as usize);
        lease
            .read_to_end(&mut bytes)
            .map_err(|_| Status::internal("browser file read failed"))?;
        check(self.deadline)?;
        Ok(bytes)
    }
    fn text(&self, file: &BrowserFile) -> Result<String, Status> {
        let bytes = self.bytes(file)?;
        if bytes.contains(&0) {
            return Err(Status::unimplemented("binary file preview unavailable"));
        }
        String::from_utf8(bytes).map_err(|_| Status::unimplemented("file is not UTF-8 text"))
    }
}
fn metadata(
    store: &tinytree_store::Store,
    repository: &str,
    key: ObjectKey,
) -> Result<Object, Status> {
    let descriptor = store
        .descriptor(repository, key)
        .map_err(store_status)?
        .ok_or_else(|| Status::not_found("metadata unavailable"))?;
    if descriptor.size_bytes > MAX_READ as u64 {
        return Err(Status::resource_exhausted(
            "browser metadata exceeds read budget",
        ));
    }
    let (_, bytes) = store.read_metadata(repository, key).map_err(store_status)?;
    tinytree_model::decode(
        &bytes,
        tinytree_model::Limits {
            max_object_bytes: MAX_READ,
            max_items: 100_000,
            max_field_bytes: MAX_FILE,
            max_path_bytes: 4096,
        },
    )
    .map_err(|_| Status::resource_exhausted("metadata exceeds browser decoding limits"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    const TOKEN: &str = "browser-reader-token-not-a-secret";
    fn setup(
        readme: &[u8],
        large_file: bool,
        authenticated: bool,
    ) -> (tempfile::TempDir, RepositoryService, String) {
        let temporary = tempfile::tempdir().unwrap();
        let store = tinytree_store::Store::open(temporary.path(), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        store.create_repository("hidden").unwrap();
        let fixture = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
            unused_files: 0,
            bytes_per_file: 0,
        })
        .unwrap();
        let mut commit = fixture
            .objects()
            .iter()
            .find_map(|object| {
                if let Ok(Object::Commit(commit)) =
                    tinytree_model::decode(&object.bytes, Default::default())
                {
                    Some(commit)
                } else {
                    None
                }
            })
            .unwrap();
        let mut leases = Vec::new();
        for object in fixture.objects() {
            leases.push(
                store
                    .put("example", object.descriptor, &object.bytes, 0)
                    .unwrap()
                    .staging_lease,
            );
        }
        let mut entries = Vec::new();
        for (name, bytes) in [
            ("README.md", readme.to_vec()),
            ("source.rs", b"fn main() { /* needle */ }\n".to_vec()),
        ]
        .into_iter()
        .chain(large_file.then(|| ("large.txt", vec![b'x'; MAX_FILE + 1])))
        {
            let descriptor = tinytree_model::describe_file(&bytes);
            leases.push(
                store
                    .put("example", descriptor, &bytes, 0)
                    .unwrap()
                    .staging_lease,
            );
            entries.push(tinytree_model::TreeEntry {
                name: name.into(),
                value: tinytree_model::TreeValue::File {
                    id: tinytree_model::FileId(descriptor.key.id.0),
                    executable: false,
                    size_bytes: bytes.len() as u64,
                    copy_id: None,
                },
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let tree = tinytree_model::encode(
            &Object::Tree(tinytree_model::Tree { entries }),
            Default::default(),
        )
        .unwrap();
        leases.push(
            store
                .put("example", tree.descriptor, &tree.bytes, 0)
                .unwrap()
                .staging_lease,
        );
        commit.root_tree = tinytree_model::TreeMerge {
            terms: vec![TreeId(tree.descriptor.key.id.0)],
            labels: vec![String::new()],
        };
        let commit = tinytree_model::encode(&Object::Commit(commit), Default::default()).unwrap();
        leases.push(
            store
                .put("example", commit.descriptor, &commit.bytes, 0)
                .unwrap()
                .staging_lease,
        );
        store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: "browser-fixture".into(),
                    reference_name: "main".into(),
                    expected_generation: 0,
                    new_commit: Some(CommitId(commit.descriptor.key.id.0)),
                    staging_leases: leases,
                },
                0,
            )
            .unwrap();
        let principals = if authenticated {
            vec![Principal::new("reader", TOKEN, vec!["example".into()], false).unwrap()]
        } else {
            vec![]
        };
        let service = RepositoryService::new(
            store,
            ServiceConfig::default(),
            vec!["example".into(), "hidden".into()],
            principals,
        )
        .unwrap();
        (temporary, service, hex(commit.descriptor.key.id.0))
    }
    async fn api(
        service: &RepositoryService,
        action: &str,
        body: Value,
        token: bool,
    ) -> (StatusCode, Value) {
        let mut request = HttpRequest::builder()
            .method("POST")
            .uri(format!("/ui/api/{action}"))
            .header("host", "localhost:8093")
            .header("origin", "http://localhost:8093")
            .header("content-type", "application/json");
        if token {
            request = request.header("authorization", format!("Bearer {TOKEN}"));
        }
        let response = browser_routes(service.clone(), Some(8093))
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 4 * MAX_FILE).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    use axum::body::Body;

    #[tokio::test]
    async fn browser_reads_readme_paths_and_literal_matches_at_exact_revision() {
        let (_directory, service, revision) = setup(
            b"# Example\nA needle in the README.\n<script>alert('untrusted')</script>\n",
            false,
            true,
        );
        let (status, snapshot) =
            api(&service, "snapshot", json!({"repository":"example"}), true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(snapshot["revision"], revision);
        assert_eq!(snapshot["paths"], json!(["README.md", "source.rs"]));
        assert_eq!(snapshot["paths_complete"], true);
        assert!(
            snapshot["readme"]["content"]
                .as_str()
                .unwrap()
                .contains("<script>")
        );
        let (status, found) = api(
            &service,
            "search",
            json!({"repository":"example","revision":revision,"query":"needle"}),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(found["complete"], true);
        assert_eq!(found["matches"].as_array().unwrap().len(), 2);
        assert_eq!(found["matches"][0]["path"], "README.md");
        assert_eq!(found["matches"][0]["line"], 2);
        let (status, file) = api(
            &service,
            "file",
            json!({"repository":"example","revision":revision,"path":"source.rs"}),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(file["content"], "fn main() { /* needle */ }\n");
        // A later public ref deletion must not redirect an immutable file read.
        service
            .inner
            .store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: "delete-main".into(),
                    reference_name: "main".into(),
                    expected_generation: 1,
                    new_commit: None,
                    staging_leases: vec![],
                },
                0,
            )
            .unwrap();
        let (status, file) = api(
            &service,
            "file",
            json!({"repository":"example","revision":revision,"path":"source.rs"}),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(file["content"], "fn main() { /* needle */ }\n");
    }
    #[tokio::test]
    async fn browser_authorizes_before_existence_and_lists_only_granted_repositories() {
        let (_directory, service, revision) = setup(b"# Example\n", false, true);
        assert_eq!(
            api(&service, "snapshot", json!({"repository":"example"}), false)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            api(&service, "repositories", json!({}), true).await.1,
            json!({"repositories":["example"]})
        );
        for repository in ["hidden", "unknown"] {
            assert_eq!(
                api(&service, "snapshot", json!({"repository":repository}), true)
                    .await
                    .0,
                StatusCode::FORBIDDEN
            );
        }
        for path in [
            "../source.rs",
            "/source.rs",
            "source.rs/",
            "source.rs\\other",
        ] {
            assert_eq!(
                api(
                    &service,
                    "file",
                    json!({"repository":"example","revision":revision,"path":path}),
                    true
                )
                .await
                .0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            api(
                &service,
                "snapshot",
                json!({"repository":"example","reference":"_tinytree/hold/v1/private"}),
                true
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    #[tokio::test]
    async fn browser_reports_incomplete_search_and_readme_preview_limits() {
        let (_directory, service, revision) = setup(&vec![b'a'; MAX_FILE + 1], true, true);
        let (status, snapshot) =
            api(&service, "snapshot", json!({"repository":"example"}), true).await;
        assert_eq!(status, StatusCode::OK);
        assert!(snapshot["readme"].is_null());
        assert!(
            snapshot["readme_error"]
                .as_str()
                .unwrap()
                .contains("preview limit")
        );
        let (status, found) = api(
            &service,
            "search",
            json!({"repository":"example","revision":revision,"query":"needle"}),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(found["complete"], false);
        assert_eq!(found["matches"][0]["path"], "source.rs");
    }
    #[tokio::test]
    async fn browser_blocks_cross_site_and_rebinding_but_accepts_http2_authority() {
        let (_directory, service, _) = setup(b"# Example", false, false);
        for (host, origin, fetch) in [
            (
                "attacker.invalid:8093",
                "http://attacker.invalid:8093",
                "same-origin",
            ),
            ("localhost:8093", "http://attacker.invalid", "cross-site"),
            (
                "localhost.evil.invalid:8093",
                "http://localhost.evil.invalid:8093",
                "same-origin",
            ),
            ("localhost:8094", "http://localhost:8094", "same-origin"),
        ] {
            let request = HttpRequest::builder()
                .method("POST")
                .uri("/ui/api/repositories")
                .header("host", host)
                .header("origin", origin)
                .header("sec-fetch-site", fetch)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            let response = browser_routes(service.clone(), Some(8093))
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{host}, {origin}");
        }
        let request = HttpRequest::builder()
            .method("POST")
            .version(http::Version::HTTP_2)
            .uri("http://localhost:8093/ui/api/repositories")
            .header("origin", "http://localhost:8093")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let response = browser_routes(service.clone(), Some(8093))
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/ui/api/repositories")
            .header("host", "localhost:8093")
            .header("content-type", "application/json")
            .body(Body::from("x".repeat(MAX_BODY + 1)))
            .unwrap();
        let response = browser_routes(service, Some(8093))
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
    struct CountedBody {
        polls: Arc<std::sync::atomic::AtomicUsize>,
        bytes: Option<bytes::Bytes>,
    }
    impl http_body::Body for CountedBody {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::task::Poll::Ready(
                self.bytes
                    .take()
                    .map(|bytes| Ok(http_body::Frame::data(bytes))),
            )
        }
    }
    #[tokio::test]
    async fn browser_authenticates_and_admits_before_polling_request_bodies() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (_directory, service, _) = setup(b"# Example", false, true);
        let request = |polls: Arc<AtomicUsize>, authenticated: bool| {
            let mut request = HttpRequest::builder()
                .method("POST")
                .uri("/ui/api/repositories")
                .header("host", "localhost:8093")
                .header("content-type", "application/json");
            if authenticated {
                request = request.header("authorization", format!("Bearer {TOKEN}"));
            }
            request
                .body(Body::new(CountedBody {
                    polls,
                    bytes: Some(bytes::Bytes::from_static(b"{}")),
                }))
                .unwrap()
        };
        let polls = Arc::new(AtomicUsize::new(0));
        let response = browser_routes(service.clone(), Some(8093))
            .oneshot(request(polls.clone(), false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let all_slots = service
            .inner
            .global
            .clone()
            .acquire_many_owned(service.maximum_inflight() as u32)
            .await
            .unwrap();
        let router = browser_routes(service.clone(), Some(8093));
        let mut pending = tokio::spawn(router.oneshot(request(polls.clone(), true)));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut pending)
                .await
                .is_err()
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(all_slots);
        let response = pending.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(polls.load(Ordering::SeqCst) > 0);
    }
}
