//! Browser presentation and instant, revision-local completion for tinytree.
//!
//! The small exported ABI transports UTF-8 JSON. Browser glue owns DOM events
//! and networking; repository-controlled content is rendered here, never as HTML.
use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, TagEnd, html};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::HashMap};

thread_local! {
    static PATHS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Command {
    Manifest {
        paths: Vec<String>,
    },
    Suggest {
        query: String,
    },
    Markdown {
        content: String,
        path: String,
    },
    Results {
        query: String,
        matches: Vec<Match>,
        complete: bool,
    },
    Source {
        content: String,
        line: Option<usize>,
    },
}
#[derive(Serialize)]
struct Suggestion {
    kind: &'static str,
    value: String,
    label: String,
    detail: &'static str,
}
#[derive(Deserialize)]
struct Match {
    path: String,
    line: usize,
    column: usize,
    preview: String,
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// Resolve Markdown links inside the same immutable repository snapshot.
fn destination(url: &str, document: &str) -> Option<(String, bool)> {
    if url.starts_with("https://") || url.starts_with("http://") {
        return Some((url.into(), true));
    }
    if url.starts_with('#') {
        return Some((url.into(), false));
    }
    if url.starts_with('/')
        || url.contains(':')
        || url.contains('\\')
        || url.chars().any(char::is_control)
    {
        return None;
    }
    let mut parts: Vec<&str> = document
        .rsplit_once('/')
        .map(|(dir, _)| dir.split('/').collect())
        .unwrap_or_default();
    let path = url.split(['?', '#']).next().unwrap_or("");
    let mut decoded = Vec::new();
    let mut bytes = path.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let a = (bytes.next()? as char).to_digit(16)?;
            let b = (bytes.next()? as char).to_digit(16)?;
            decoded.push((a * 16 + b) as u8);
        } else {
            decoded.push(byte);
        }
    }
    let path = std::str::from_utf8(&decoded).ok()?;
    if path.starts_with('/') || path.contains('\\') || path.chars().any(char::is_control) {
        return None;
    }
    for part in path.split('/') {
        match part {
            "" | "." => (),
            ".." => {
                parts.pop()?;
            }
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        return None;
    }
    let fragment = url
        .split_once('#')
        .map(|(_, fragment)| format!("#{}", encode(fragment)))
        .unwrap_or_default();
    Some((
        format!("#file:{}{fragment}", encode(&parts.join("/"))),
        false,
    ))
}

/// CommonMark rendering with raw HTML displayed as text and images as alt text.
/// No repository content can introduce executable HTML or an automatic fetch.
pub fn render_markdown(content: &str, path: &str) -> String {
    const MAX_BYTES: usize = 256 * 1024;
    const MAX_EVENTS: usize = 20_000;
    let mut boundary = content.len().min(MAX_BYTES);
    while !content.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut truncated = boundary < content.len();
    let mut links = Vec::new();
    let mut headings: HashMap<String, usize> = HashMap::new();
    let mut parsed = Vec::new();
    let mut open = Vec::new();
    for event in Parser::new_ext(
        &content[..boundary],
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    ) {
        if parsed.len() == MAX_EVENTS {
            truncated = true;
            break;
        }
        match &event {
            Event::Start(tag) => open.push(tag.to_end()),
            Event::End(_) => {
                open.pop();
            }
            _ => (),
        }
        parsed.push(event);
    }
    // Closing the active structure preserves balanced HTML at the event budget.
    // It also keeps a truncated link/list from absorbing the following notice.
    parsed.extend(open.into_iter().rev().map(Event::End));
    let events = parsed
        .iter()
        .enumerate()
        .map(|(index, event)| match event.clone() {
            Event::Start(Tag::Heading { level, .. }) => {
                let mut text = String::new();
                for event in &parsed[index + 1..] {
                    match event {
                        Event::End(TagEnd::Heading(_)) => break,
                        Event::Text(value) | Event::Code(value) => text.push_str(value),
                        _ => (),
                    }
                }
                let slug: String = text
                    .to_lowercase()
                    .chars()
                    .filter_map(|c| {
                        if c.is_alphanumeric() || c == '-' || c == '_' {
                            Some(c)
                        } else if c.is_whitespace() {
                            Some('-')
                        } else {
                            None
                        }
                    })
                    .collect();
                let count = headings.entry(slug.clone()).or_default();
                let id = if *count == 0 {
                    slug
                } else {
                    format!("{slug}-{count}")
                };
                *count += 1;
                Event::Html(format!("<{level} id=\"{}\">", escape(&id)).into())
            }
            Event::End(TagEnd::Heading(level)) => Event::Html(format!("</{level}>\n").into()),
            Event::Html(value) | Event::InlineHtml(value) => Event::Text(value),
            Event::Start(Tag::Image { .. }) => Event::Text(CowStr::Borrowed("[image: ")),
            Event::End(TagEnd::Image) => Event::Text(CowStr::Borrowed("]")),
            Event::Start(Tag::Link { dest_url, .. }) => {
                if let Some((url, external)) = destination(&dest_url, path) {
                    links.push(true);
                    let attributes = if external {
                        " target=\"_blank\" rel=\"noopener noreferrer\""
                    } else {
                        ""
                    };
                    Event::Html(format!("<a href=\"{}\"{attributes}>", escape(&url)).into())
                } else {
                    links.push(false);
                    Event::Html(CowStr::Borrowed("<span>"))
                }
            }
            Event::End(TagEnd::Link) => {
                Event::Html(CowStr::Borrowed(if links.pop().unwrap_or(false) {
                    "</a>"
                } else {
                    "</span>"
                }))
            }
            other => other,
        });
    let mut output = if truncated {
        String::from(
            "<aside class=\"markdown-preview-note\" role=\"note\">This Markdown preview is truncated to keep the page responsive. The view is limited to 256 KiB and 20,000 formatting events.</aside>",
        )
    } else {
        String::new()
    };
    html::push_html(&mut output, events);
    output
}

fn suggestions(query: &str, paths: &[String]) -> Vec<Suggestion> {
    let needle = query.trim().to_lowercase();
    let mut ranked: Vec<_> = paths
        .iter()
        .filter_map(|path| {
            let lower = path.to_lowercase();
            let basename = lower.rsplit('/').next().unwrap_or(&lower);
            let rank = if needle.is_empty() {
                if basename == "readme.md" {
                    0
                } else if !path.contains('/') {
                    1
                } else {
                    2
                }
            } else if lower.starts_with(&needle) {
                0
            } else if basename.starts_with(&needle) {
                1
            } else if lower.contains(&needle) {
                2
            } else {
                return None;
            };
            Some((rank, path))
        })
        .collect();
    ranked.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.len().cmp(&b.1.len()))
            .then(a.1.cmp(b.1))
    });
    let mut result: Vec<_> = ranked
        .into_iter()
        .take(6)
        .map(|(_, path)| Suggestion {
            kind: "file",
            value: path.clone(),
            label: path.clone(),
            detail: "Open file",
        })
        .collect();
    if !needle.is_empty() {
        result.push(Suggestion {
            kind: "search",
            value: query.into(),
            label: format!("Search contents for “{query}”"),
            detail: "Literal search",
        });
    }
    result
}

fn render_results(query: &str, matches: &[Match], complete: bool) -> String {
    let coverage = if complete {
        "Search complete"
    } else {
        "Partial results · search limit reached"
    };
    let mut output = format!(
        "<header class=\"results-heading\"><div><p class=\"eyebrow\">SEARCH RESULTS</p><h1>{}</h1></div><p>{} match{}<br><span>{coverage}</span></p></header>",
        escape(query),
        matches.len(),
        if matches.len() == 1 { "" } else { "es" }
    );
    if matches.is_empty() {
        output.push_str(if complete { "<div class=\"empty-state\"><h2>No matches found</h2><p>Try a shorter phrase, or open a file from the search suggestions.</p></div>" } else { "<div class=\"empty-state\"><h2>No matches in the scanned files</h2><p>The search limit was reached. This is not proof that the repository has no matches.</p></div>" });
    }
    for item in matches {
        output.push_str(&format!("<button type=\"button\" class=\"search-match\" data-path=\"{}\" data-line=\"{}\"><span class=\"match-location\">{}<span>{}:{}</span></span><code>{}</code></button>", escape(&item.path), item.line, escape(&item.path), item.line, item.column, escape(&item.preview)));
    }
    output
}

fn source(content: &str, selected: Option<usize>) -> String {
    const MAX_LINES: usize = 3000;
    let total = content.split('\n').count();
    let start = selected
        .unwrap_or(1)
        .saturating_sub(1000)
        .min(total.saturating_sub(1));
    let end = start.saturating_add(MAX_LINES).min(total);
    let mut output = String::new();
    if start > 0 || end < total {
        output.push_str(&format!("<p class=\"source-truncated\">Showing lines {}–{end} of {total}. This file view is limited to {MAX_LINES} lines.</p>", start + 1));
    }
    output.push_str("<pre class=\"source\"><code>");
    for (index, line) in content.split('\n').enumerate().skip(start).take(MAX_LINES) {
        let number = index + 1;
        output.push_str(&format!("<span id=\"line-{number}\" class=\"source-line{}\"><span class=\"line-number\">{number}</span>{}</span>", if selected == Some(number) { " selected-line" } else { "" }, escape(line)));
    }
    output.push_str("</code></pre>");
    output
}

/// Public JSON boundary shared by the browser ABI and native regression tests.
pub fn command(input: &[u8]) -> Vec<u8> {
    let value = match serde_json::from_slice::<Command>(input) {
        Ok(Command::Manifest { mut paths }) => {
            paths.truncate(10_000);
            paths.retain(|path| path.len() <= 4096);
            PATHS.with(|cache| *cache.borrow_mut() = paths);
            serde_json::json!({"ok": true})
        }
        Ok(Command::Suggest { query }) => {
            PATHS.with(|paths| serde_json::json!({"options": suggestions(&query, &paths.borrow())}))
        }
        Ok(Command::Markdown { content, path }) => {
            serde_json::json!({"html": render_markdown(&content, &path)})
        }
        Ok(Command::Results {
            query,
            matches,
            complete,
        }) => serde_json::json!({"html": render_results(&query, &matches, complete)}),
        Ok(Command::Source { content, line }) => {
            serde_json::json!({"html": source(&content, line)})
        }
        Err(_) => serde_json::json!({"error": "Invalid presentation request"}),
    };
    serde_json::to_vec(&value).unwrap_or_default()
}

/// Allocate a zero-initialized input buffer. The caller must free exactly once.
#[unsafe(no_mangle)]
pub extern "C" fn tt_alloc(len: usize) -> *mut u8 {
    let buffer = vec![0u8; len].into_boxed_slice();
    Box::into_raw(buffer) as *mut u8
}

/// Release a buffer returned by `tt_alloc`, using the original length.
///
/// # Safety
/// `ptr` and `len` must describe a live allocation returned by `tt_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_free(ptr: *mut u8, len: usize) {
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)));
    }
}

/// Execute a JSON command. The result remains valid until the next command.
///
/// # Safety
/// `ptr` and `len` must describe readable bytes allocated by `tt_alloc`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_command(ptr: *const u8, len: usize) -> *const u8 {
    let input = unsafe { std::slice::from_raw_parts(ptr, len) };
    OUTPUT.with(|output| {
        let mut output = output.borrow_mut();
        *output = command(input);
        output.as_ptr()
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn tt_result_len() -> usize {
    OUTPUT.with(|output| output.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn markdown_preserves_reading_structure_without_active_content() {
        let rendered = render_markdown(
            "# Guide\n\n**Ready** [file](../src/lib.rs) [bad](javascript:alert%281%29)\n\n<img src=x onerror=alert(1)>\n\n![badge](https://example.invalid/track)\n\n| A | B |\n| - | - |\n| 1 | 2 |",
            "docs/README.md",
        );
        assert!(rendered.contains("<h1 id=\"guide\">Guide</h1>"));
        assert!(rendered.contains("<strong>Ready</strong>"));
        assert!(rendered.contains("href=\"#file:src/lib.rs\""));
        assert!(!rendered.contains("href=\"javascript:"));
        assert!(!rendered.contains("<img"));
        assert!(rendered.contains("&lt;img"));
        assert!(rendered.contains("[image: badge]"));
        assert!(rendered.contains("<table>"));
    }
    #[test]
    fn autocomplete_prefers_prefix_and_exposes_literal_search() {
        command(br#"{"op":"manifest","paths":["docs/server.md","server.rs","src/server.rs","README.md"]}"#);
        let result: serde_json::Value =
            serde_json::from_slice(&command(br#"{"op":"suggest","query":"server"}"#)).unwrap();
        assert_eq!(result["options"][0]["value"], "server.rs");
        assert_eq!(result["options"][1]["value"], "src/server.rs");
        assert_eq!(result["options"][3]["kind"], "search");
    }
    #[test]
    fn result_rendering_escapes_paths_and_reports_partial_coverage() {
        let result: serde_json::Value = serde_json::from_slice(&command(br#"{"op":"results","query":"<script>","matches":[{"path":"\"onfocus=bad","line":2,"column":1,"preview":"<img>"}],"complete":false}"#)).unwrap();
        let html = result["html"].as_str().unwrap();
        assert!(html.contains("Partial results"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("data-path=\"&quot;onfocus=bad\""));
        assert!(!html.contains("<img>"));
    }
    #[test]
    fn relative_links_cannot_escape_repository_or_use_protocol_relative_urls() {
        assert_eq!(destination("../../private", "README.md"), None);
        assert_eq!(destination("//example.invalid", "README.md"), None);
        assert_eq!(destination("data:text/html,bad", "README.md"), None);
        assert_eq!(
            destination("https://example.invalid", "README.md"),
            Some(("https://example.invalid".into(), true))
        );
        assert_eq!(
            destination("my%20file.md#overview", "docs/README.md"),
            Some(("#file:docs/my%20file.md#overview".into(), false))
        );
        assert_eq!(destination("%2e%2e/private", "README.md"), None);
    }
    #[test]
    fn source_limits_dom_growth_and_can_show_a_late_match() {
        let content = "line\n".repeat(20_000);
        let html = source(&content, Some(15_000));
        assert!(html.contains("Showing lines"));
        assert!(html.contains("id=\"line-15000\" class=\"source-line selected-line\""));
        assert_eq!(html.matches("class=\"line-number\"").count(), 3000);
    }
    #[test]
    fn markdown_bounds_large_previews_and_closes_truncated_structure() {
        let html = render_markdown(&"- [ ] task\n".repeat(30_000), "README.md");
        assert!(html.contains("Markdown preview is truncated"));
        assert_eq!(html.matches("<ul>").count(), html.matches("</ul>").count());
        assert_eq!(html.matches("<li>").count(), html.matches("</li>").count());
        assert!(html.len() < 2 * 1024 * 1024);
        let unicode = render_markdown(&"é".repeat(150_000), "README.md");
        assert!(unicode.contains("Markdown preview is truncated"));
        assert!(!unicode.contains('\u{fffd}'));
    }
}
