//! Internal per-FILE matching over host-authorized bytes, not an ACL resolver.
//!
//! The host authenticates paths/current policy and binds cross-file cursors. This
//! adapter accepts no path, account, profile, principal or coverage claim. Its
//! continuation is a byte position only, never authority to read an object.
//! Binary exclusions and limits are NOT complete cross-file search coverage.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};
use serde_json::{Value, json};
use std::{
    fmt,
    io::{self, Write},
};
use tinytree_forge_core::{
    accounts::{AccountId, validate_name},
    search::{ForgeSearchRequest, query_fingerprint},
};
use tinytree_model::{self as model, Descriptor, Kind, ObjectId, ObjectKey};
use tinytree_search::{Error, MatchBudget, MatchLimits, MatchUsage, Pattern, SearchMode};

const MAX_FILE: usize = 1024 * 1024;
const MAX_INPUT: usize = 2 * 1024 * 1024;
const MAX_BASE64: usize = MAX_FILE.div_ceil(3) * 4;
const MAX_RESPONSE: usize = 512 * 1024;
const MAX_WORK: usize = 64 * 1024 * 1024;
const MAX_MATCH_OUTPUT: usize = 256 * 1024;
const MAX_HITS: usize = 100;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema: u32,
    descriptor: FileDescriptor,
    bytes_base64: Text<MAX_BASE64>,
    query: Text<{ tinytree_search::MAX_PATTERN_BYTES }>,
    #[serde(default)]
    regex: bool,
    limit: usize,
    #[serde(default)]
    start_offset: u64,
    /// Trusted host-only remaining page accounting. Never copied from the
    /// public request; the shared ForgeSearchRequest rejects this field.
    #[serde(default)]
    remaining_budget: Option<RemainingBudget>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemainingBudget {
    max_work: usize,
    max_output_bytes: usize,
}
impl RemainingBudget {
    fn limits(&self) -> Result<MatchLimits, String> {
        if self.max_work == 0
            || self.max_work > MAX_WORK
            || self.max_output_bytes == 0
            || self.max_output_bytes > MAX_MATCH_OUTPUT
        {
            return Err("invalid remaining search budget".into());
        }
        Ok(MatchLimits {
            max_input_bytes: MAX_FILE,
            max_work: self.max_work,
            max_output_bytes: self.max_output_bytes,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDescriptor {
    kind: u8,
    id: Text<64>,
    size_bytes: u64,
    format_version: u16,
}
struct Text<const MAX: usize>(String);
impl<'de, const MAX: usize> Deserialize<'de> for Text<MAX> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct TextVisitor<const MAX: usize>;
        impl<const MAX: usize> Visitor<'_> for TextVisitor<MAX> {
            type Value = Text<MAX>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "at most {MAX} string bytes")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.len() > MAX {
                    return Err(E::custom("search string byte limit"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        d.deserialize_str(TextVisitor::<MAX>)
    }
}
fn native_id(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid lowercase FILE identity".into());
    }
    let mut id = [0; 32];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "invalid FILE identity")?;
    }
    Ok(id)
}
fn match_limits() -> MatchLimits {
    MatchLimits {
        max_input_bytes: MAX_FILE,
        max_work: MAX_WORK,
        max_output_bytes: MAX_MATCH_OUTPUT,
    }
}
fn usage(value: MatchUsage) -> Value {
    // These are matcher counters, not total host IO/verification accounting.
    json!({"input_bytes":value.input_bytes,"work":value.work,"output_bytes":value.output_bytes})
}
struct Output(Vec<u8>);
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RESPONSE.saturating_sub(self.0.len()) {
            return Err(io::Error::other("search response limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded(value: Value, verified_bytes: usize, counters: MatchUsage) -> Value {
    let mut output = Output(Vec::new());
    if serde_json::to_writer(&mut output, &value).is_err() {
        return json!({"state":"limit","budget":"response_bytes","matches":[],"complete":false,"next_offset":null,"verified_bytes":verified_bytes,"usage":usage(counters)});
    }
    value
}

/// Verify one complete native FILE, then use exactly the native bounded matcher.
/// The full FILE is supplied even for continuation: the matcher reconstructs
/// one-based lines/byte columns from its original prefix. No wall clock is read;
/// fixed work/regex-memory/input/output budgets bound synchronous execution.
pub fn search_file(input: &[u8]) -> Result<Value, String> {
    if input.len() > MAX_INPUT {
        return Err("search input byte limit".into());
    }
    let input: Input = serde_json::from_slice(input).map_err(|_| "invalid bounded search input")?;
    if input.schema != 1 || input.limit == 0 || input.limit > MAX_HITS {
        return Err("invalid search schema or hit limit".into());
    }
    let chosen_limits = match &input.remaining_budget {
        Some(budget) => budget.limits()?,
        None => match_limits(),
    };
    let file = input.descriptor;
    if file.kind != Kind::File as u8
        || file.format_version != model::FORMAT_VERSION
        || file.size_bytes > MAX_FILE as u64
    {
        return Err("unsupported FILE descriptor or byte limit".into());
    }
    let size = file.size_bytes as usize;
    if input.start_offset > file.size_bytes {
        return Err("search offset exceeds FILE bytes".into());
    }
    if input.bytes_base64.0.len() != size.div_ceil(3) * 4 {
        return Err("FILE base64 length mismatch".into());
    }
    let descriptor = Descriptor {
        key: ObjectKey {
            kind: Kind::File,
            id: ObjectId(native_id(&file.id.0)?),
        },
        size_bytes: file.size_bytes,
        format_version: file.format_version,
    };
    let mut bytes = vec![0; size];
    let decoded = STANDARD
        .decode_slice(&input.bytes_base64.0, &mut bytes)
        .map_err(|_| "invalid FILE base64")?;
    if decoded != size || STANDARD.encode(&bytes) != input.bytes_base64.0 {
        return Err("noncanonical FILE base64".into());
    }
    model::verify(
        &descriptor,
        &bytes,
        model::Limits {
            max_object_bytes: MAX_FILE,
            ..Default::default()
        },
    )
    .map_err(|_| "FILE canonical identity mismatch")?;
    let mode = if input.regex {
        SearchMode::Regex
    } else {
        SearchMode::Literal
    };
    let pattern = Pattern::new(input.query.0.as_bytes(), mode)
        .map_err(|_| "invalid bounded search pattern")?;
    if std::str::from_utf8(&bytes).is_err() {
        return Ok(
            json!({"state":"binary","matches":[],"complete":false,"next_offset":null,"verified_bytes":size,"usage":usage(MatchUsage::default())}),
        );
    }
    let mut budget = MatchBudget::new(chosen_limits);
    let result = pattern.scan(&bytes, input.start_offset, input.limit, &mut budget, || {
        true
    });
    let counters = budget.usage();
    let page = match result {
        Ok(page) => page,
        Err(Error::Invalid) => return Err("invalid search offset or matcher input".into()),
        Err(Error::Limit) => {
            let reason = if counters.work > chosen_limits.max_work {
                "work"
            } else if counters.output_bytes > chosen_limits.max_output_bytes {
                "match_output"
            } else if counters.input_bytes > MAX_FILE {
                "input_bytes"
            } else {
                "matcher"
            };
            return Ok(
                json!({"state":"limit","budget":reason,"matches":[],"complete":false,"next_offset":null,"verified_bytes":size,"usage":usage(counters)}),
            );
        }
        Err(Error::Cancelled) => {
            return Ok(
                json!({"state":"unavailable","matches":[],"complete":false,"next_offset":null,"verified_bytes":size,"usage":usage(counters)}),
            );
        }
    };
    let matches = page.hits.into_iter().map(|hit| json!({"line":hit.line,"column":hit.column,"preview":String::from_utf8_lossy(&hit.snippet)})).collect::<Vec<_>>();
    Ok(bounded(
        json!({"state":"matches","matches":matches,"complete":page.next_offset.is_none(),"next_offset":page.next_offset,"verified_bytes":size,"usage":usage(counters)}),
        size,
        counters,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchCommand {
    account_id: String,
    operation: u32,
    view: String,
    payload_json: String,
}
/// Strict public operation normalization and REAL pattern compilation. This
/// rejects invalid/unsupported patterns even when the selected view is empty.
/// A successful normalization is not a serialized compiled plan or authority;
/// the host separately binds current actor/view/epoch/catalog to opaque cursors.
pub fn normalize_command(input: &[u8]) -> Result<Value, String> {
    const MAX_COMMAND: usize = 64 * 1024;
    if input.len() > MAX_COMMAND {
        return Err("search command byte limit".into());
    }
    let command: SearchCommand =
        serde_json::from_slice(input).map_err(|_| "invalid strict search command")?;
    if command.operation != 8 {
        return Err("unsupported search operation".into());
    }
    AccountId(command.account_id.clone())
        .validate()
        .map_err(|e| e.to_string())?;
    if !command.view.is_empty() {
        validate_name(&command.view).map_err(|e| e.to_string())?;
    }
    let request: ForgeSearchRequest = serde_json::from_slice(command.payload_json.as_bytes())
        .map_err(|_| "invalid strict search payload")?;
    request.validate().map_err(|e| e.to_string())?;
    let mode = if request.regex {
        SearchMode::Regex
    } else {
        SearchMode::Literal
    };
    let _pattern = Pattern::new(request.query.as_bytes(), mode)
        .map_err(|_| "invalid bounded search pattern")?;
    let fingerprint = query_fingerprint(&command.account_id, &command.view, &request)
        .map_err(|e| e.to_string())?;
    Ok(
        json!({"operation":8,"version":request.version,"query":request.query,"regex":request.regex,"limit":request.limit,"cursor":request.cursor,"fingerprint":fingerprint}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input(bytes: &[u8], query: &str, regex: bool, limit: usize, start_offset: u64) -> Value {
        let descriptor = model::describe_file(bytes);
        json!({"schema":1,"descriptor":{"kind":1,"id":descriptor.key.id.0.iter().map(|b|format!("{b:02x}")).collect::<String>(),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version},"bytes_base64":STANDARD.encode(bytes),"query":query,"regex":regex,"limit":limit,"start_offset":start_offset})
    }
    fn call(value: &Value) -> Result<Value, String> {
        search_file(&serde_json::to_vec(value).unwrap())
    }
    #[test]
    fn literal_regex_and_resumed_pages_match_native_line_column_and_lossy_preview() {
        let bytes = "αα start\r\nfirst needle needle\nlast neeeedle\n".as_bytes();
        for (name, query, regex, offset, limit) in [
            ("literal first page", "needle", false, 0, 1),
            ("literal resumed page", "needle", false, 23, 10),
            ("regex on LF lines", "ne+dl[e]", true, 0, 100),
            ("UTF8 byte column", "start", false, 0, 100),
            ("end of file", "needle", false, bytes.len() as u64, 100),
            ("empty regex matches omitted", "^", true, 0, 100),
        ] {
            let output = call(&input(bytes, query, regex, limit, offset)).unwrap();
            let pattern = Pattern::new(
                query.as_bytes(),
                if regex {
                    SearchMode::Regex
                } else {
                    SearchMode::Literal
                },
            )
            .unwrap();
            let mut budget = MatchBudget::new(match_limits());
            let expected = pattern
                .scan(bytes, offset, limit, &mut budget, || true)
                .unwrap();
            assert_eq!(output["matches"],json!(expected.hits.iter().map(|hit|json!({"line":hit.line,"column":hit.column,"preview":String::from_utf8_lossy(&hit.snippet)})).collect::<Vec<_>>()),"{name}");
            assert_eq!(output["next_offset"], json!(expected.next_offset), "{name}");
            assert_eq!(output["complete"], expected.next_offset.is_none(), "{name}");
            assert!(output.get("id").is_none());
            assert!(output.get("path").is_none());
        }
        // Snippet boundaries can split UTF-8, exactly as the native server's
        // from_utf8_lossy conversion does. Never reinterpret columns as chars.
        let text = format!("{}needle{}", "€".repeat(44), "β".repeat(65));
        let output = call(&input(text.as_bytes(), "needle", false, 1, 0)).unwrap();
        assert_eq!(output["matches"][0]["column"], 133);
        assert!(
            output["matches"][0]["preview"]
                .as_str()
                .unwrap()
                .contains('\u{fffd}')
        );
    }
    #[test]
    fn continuations_are_progress_not_authority_and_binary_is_honestly_excluded() {
        let bytes = b"x x x\n";
        let mut offset = 0;
        let mut all = Vec::new();
        loop {
            let page = call(&input(bytes, "x", false, 1, offset)).unwrap();
            all.extend(page["matches"].as_array().unwrap().iter().cloned());
            match page["next_offset"].as_u64() {
                Some(next) => {
                    assert!(next > offset);
                    offset = next
                }
                None => break,
            }
        }
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.iter()
                .map(|hit| hit["column"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 3, 5]
        );
        let binary = call(&input(b"x\xff", "x", false, 100, 0)).unwrap();
        assert_eq!(binary["state"], "binary");
        assert_eq!(binary["complete"], false);
        assert!(binary["matches"].as_array().unwrap().is_empty());
        let nul = call(&input(b"x\0x", "x", false, 100, 0)).unwrap();
        assert_eq!(nul["state"], "matches");
        assert_eq!(
            nul["matches"].as_array().unwrap().len(),
            2,
            "native UTF-8 gate does not classify NUL as binary"
        );
        let empty = call(&input(b"", "x", false, 1, 0)).unwrap();
        assert_eq!(empty["complete"], true);
    }
    #[test]
    fn rejects_forged_bytes_ambiguous_schema_and_invalid_matcher_inputs() {
        let valid = input(b"needle", "needle", false, 10, 0);
        let mut cases = Vec::new();
        let mut value = valid.clone();
        value["descriptor"]["id"] = json!("0".repeat(64));
        cases.push(("forged digest", value));
        let mut value = valid.clone();
        value["descriptor"]["kind"] = json!(2);
        cases.push(("metadata as FILE", value));
        let mut value = valid.clone();
        value["descriptor"]["size_bytes"] = json!(5);
        cases.push(("wrong length", value));
        let mut value = valid.clone();
        value["descriptor"]["format_version"] = json!(2);
        cases.push(("wrong format", value));
        let mut value = valid.clone();
        value["bytes_base64"] = json!("bmVlZGxl\n");
        cases.push(("base64 whitespace", value));
        let mut value = valid.clone();
        value["principal"] = json!("owner");
        cases.push(("identity smuggling", value));
        let mut value = valid.clone();
        value["start_offset"] = json!(7);
        cases.push(("out of bounds offset", value));
        let mut value = valid.clone();
        value["query"] = json!("x\ny");
        cases.push(("multiline query", value));
        let mut value = valid.clone();
        value["query"] = json!("");
        cases.push(("empty query", value));
        let mut value = valid.clone();
        value["query"] = json!("x".repeat(tinytree_search::MAX_PATTERN_BYTES + 1));
        cases.push(("query byte limit", value));
        let mut value = valid.clone();
        value["query"] = json!("(?=x)");
        value["regex"] = json!(true);
        cases.push(("unsupported lookaround", value));
        let mut value = valid.clone();
        value["limit"] = json!(101);
        cases.push(("hit limit", value));
        for (name, value) in cases {
            assert!(call(&value).is_err(), "{name}");
        }
        let duplicate = serde_json::to_string(&valid)
            .unwrap()
            .replace("\"schema\":1", "\"schema\":1,\"schema\":1");
        assert!(search_file(duplicate.as_bytes()).is_err());
        assert!(search_file(&vec![b' '; MAX_INPUT + 1]).is_err());
        let mut huge = valid;
        huge["descriptor"]["size_bytes"] = json!(MAX_FILE + 1);
        assert!(call(&huge).is_err());
    }
    #[test]
    fn whole_file_boundary_and_budget_failure_never_claim_partial_success() {
        let bytes = vec![b'x'; MAX_FILE];
        let page = call(&input(&bytes, "absent", false, 1, 0)).unwrap();
        assert_eq!(page["state"], "matches");
        assert_eq!(page["complete"], true);
        assert_eq!(page["verified_bytes"], MAX_FILE);
        // The native matcher refuses regex matches wider than its snippet
        // contract. Return an explicit failed page, never accumulated hits.
        let text = format!("ok\n{}", "x".repeat(1025));
        let page = call(&input(text.as_bytes(), "ok|x+", true, 100, 0)).unwrap();
        assert_eq!(page["state"], "limit");
        assert_eq!(page["complete"], false);
        assert!(page["matches"].as_array().unwrap().is_empty());
        assert!(page["next_offset"].is_null());
    }
    fn command(payload: Value) -> Value {
        json!({"account_id":"example","operation":8,"view":"public","payload_json":payload.to_string()})
    }
    fn normalize(value: &Value) -> Result<Value, String> {
        normalize_command(&serde_json::to_vec(value).unwrap())
    }
    #[test]
    fn empty_view_preflight_uses_real_native_compiler_and_shared_intent_fingerprint() {
        let payload = json!({"query":"needle","limit":10});
        let result = normalize(&command(payload.clone())).unwrap();
        assert_eq!(result["operation"], 8);
        assert_eq!(result["regex"], false);
        assert!(result["version"].is_null());
        assert!(result["cursor"].is_null());
        let request: ForgeSearchRequest = serde_json::from_value(payload).unwrap();
        assert_eq!(
            result["fingerprint"],
            query_fingerprint("example", "public", &request).unwrap()
        );
        assert!(
            result.get("compiled").is_none(),
            "normalization is not a persisted compiler or authorization proof"
        );
        for (name, query, regex) in [
            ("empty literal", "", false),
            ("unsupported lookaround", "(?=needle)", true),
            ("invalid regex", "(", true),
            ("newline pattern", "first\nsecond", false),
        ] {
            assert!(
                normalize(&command(json!({"query":query,"regex":regex,"limit":10}))).is_err(),
                "{name}"
            );
        }
        let literal = normalize(&command(json!({"query":"(","regex":false,"limit":10}))).unwrap();
        assert_eq!(literal["query"], "(");
        let continuation = normalize(&command(json!({"version":vec![1u8;32],"cursor":"page-one","query":"needle","regex":false,"limit":10}))).unwrap();
        assert_eq!(
            continuation["fingerprint"], result["fingerprint"],
            "selected version/cursor remain separate authoritative host bindings"
        );
        assert!(
            normalize(&command(
                json!({"cursor":"page-one","query":"needle","limit":10})
            ))
            .is_err()
        );
    }
    #[test]
    fn public_search_cannot_select_internal_budgets_or_extra_authority() {
        let mut public = command(
            json!({"query":"needle","limit":10,"remaining_budget":{"max_work":1,"max_output_bytes":1}}),
        );
        assert!(normalize(&public).is_err());
        public = command(json!({"query":"needle","limit":10}));
        public["principal"] = json!("owner");
        assert!(normalize(&public).is_err());
        let mut duplicate = command(json!({"query":"needle","limit":10}));
        duplicate["payload_json"] = json!(r#"{"query":"needle","limit":10,"limit":11}"#);
        assert!(normalize(&duplicate).is_err());
        let mut wrong = command(json!({"query":"needle","limit":10}));
        wrong["operation"] = json!(7);
        assert!(normalize(&wrong).is_err());
        assert!(normalize(&command(json!({"query":"needle","limit":101}))).is_err());
        assert!(normalize_command(&vec![b' '; 64 * 1024 + 1]).is_err());
    }
    #[test]
    fn remaining_page_budgets_are_enforced_by_matcher_and_never_return_partial_file_hits() {
        let valid = input(b"x x x\n", "x", false, 100, 0);
        let full = call(&valid).unwrap();
        assert_eq!(full["matches"].as_array().unwrap().len(), 3);
        let work = full["usage"]["work"].as_u64().unwrap() as usize;
        let output = full["usage"]["output_bytes"].as_u64().unwrap() as usize;
        assert!(work > 1 && output > 1);
        for (name, remaining_work, remaining_output, expected_budget) in [
            ("remaining work", work - 1, MAX_MATCH_OUTPUT, "work"),
            ("remaining output", MAX_WORK, output - 1, "match_output"),
        ] {
            let mut request = valid.clone();
            request["remaining_budget"] =
                json!({"max_work":remaining_work,"max_output_bytes":remaining_output});
            let result = call(&request).unwrap();
            assert_eq!(result["state"], "limit", "{name}");
            assert_eq!(result["budget"], expected_budget, "{name}");
            assert!(result["matches"].as_array().unwrap().is_empty(), "{name}");
            assert_eq!(result["complete"], false);
            assert!(result["next_offset"].is_null());
        }
        let mut exact = valid.clone();
        exact["remaining_budget"] = json!({"max_work":work,"max_output_bytes":output});
        assert_eq!(
            call(&exact).unwrap(),
            full,
            "exact remaining budgets preserve native hit semantics"
        );
        for (name, remaining_work, remaining_output) in [
            ("zero work", 0, MAX_MATCH_OUTPUT),
            ("zero output", MAX_WORK, 0),
            ("inflated work", MAX_WORK + 1, MAX_MATCH_OUTPUT),
            ("inflated output", MAX_WORK, MAX_MATCH_OUTPUT + 1),
        ] {
            let mut request = valid.clone();
            request["remaining_budget"] =
                json!({"max_work":remaining_work,"max_output_bytes":remaining_output});
            assert!(call(&request).is_err(), "{name}");
        }
        let mut extra = valid;
        extra["remaining_budget"] = json!({"max_work":1,"max_output_bytes":1,"grant":true});
        assert!(call(&extra).is_err());
    }
}
