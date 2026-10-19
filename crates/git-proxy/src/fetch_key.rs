//! Conservative identity for complete protocol-v2 fetch responses.

const MAX_WANTS: usize = 16_384;

/// Unknown or incomplete requests bypass caching and remain native Git's concern.
/// Repository epochs invalidate responses whose contents depend on changed refs
/// (notably include-tag), even though wants themselves are full object IDs.
pub(crate) fn cache_key(
    protocol: &str,
    body: &[u8],
    repository_identity: &str,
    epoch: u64,
) -> Option<String> {
    if protocol != "version=2" {
        return None;
    }
    let mut input = body;
    let command = data(&mut input)?;
    if line(command)? != b"command=fetch" {
        return None;
    }
    let mut hash = blake3::Hasher::new();
    frame(&mut hash, b"git-proxy/fetch-response/v1");
    frame(&mut hash, repository_identity.as_bytes());
    frame(&mut hash, &epoch.to_be_bytes());
    frame(&mut hash, protocol.as_bytes());
    frame(&mut hash, command);
    let (mut agent, mut session, mut format) = (false, false, false);
    loop {
        match packet(&mut input)? {
            Packet::Delimiter => break,
            Packet::Data(raw) => {
                let cap = line(raw)?;
                if let Some(value) = cap.strip_prefix(b"agent=") {
                    if agent || value.is_empty() {
                        return None;
                    }
                    agent = true;
                } else if let Some(value) = cap.strip_prefix(b"session-id=") {
                    if session || value.is_empty() || value.contains(&b' ') {
                        return None;
                    }
                    session = true;
                } else if cap == b"object-format=sha1" && !format {
                    format = true;
                    frame(&mut hash, raw);
                } else {
                    return None;
                }
            }
            Packet::Flush => return None,
        }
    }
    // Separate command capabilities and fetch arguments in the hash schema.
    frame(&mut hash, b"arguments");
    let mut wants = 0;
    let mut done = false;
    loop {
        match packet(&mut input)? {
            Packet::Flush => {
                return (done && wants > 0 && input.is_empty())
                    .then(|| hash.finalize().to_hex().to_string());
            }
            Packet::Delimiter => return None,
            Packet::Data(raw) => {
                if done {
                    return None;
                }
                let arg = line(raw)?;
                if let Some(oid) = arg.strip_prefix(b"want ") {
                    if !sha1(oid) || wants == MAX_WANTS {
                        return None;
                    }
                    wants += 1;
                } else if let Some(oid) = arg
                    .strip_prefix(b"have ")
                    .or_else(|| arg.strip_prefix(b"shallow "))
                {
                    if !sha1(oid) {
                        return None;
                    }
                } else if let Some(depth) = arg.strip_prefix(b"deepen ") {
                    let value = decimal(depth)?;
                    if value == 0 || value > i32::MAX as u64 {
                        return None;
                    }
                } else if let Some(limit) = arg.strip_prefix(b"filter blob:limit=") {
                    decimal(limit)?;
                } else if arg == b"done" {
                    done = true;
                } else if !matches!(
                    arg,
                    b"thin-pack"
                        | b"no-progress"
                        | b"include-tag"
                        | b"ofs-delta"
                        | b"deepen-relative"
                        | b"sideband-all"
                        | b"wait-for-done"
                        | b"filter blob:none"
                ) {
                    return None;
                }
                // Retain exact bytes and ordering, including optional LF.
                frame(&mut hash, raw);
            }
        }
    }
}

fn frame(hash: &mut blake3::Hasher, value: &[u8]) {
    hash.update(&(value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn sha1(value: &[u8]) -> bool {
    value.len() == 40 && value.iter().all(u8::is_ascii_hexdigit)
}

fn decimal(value: &[u8]) -> Option<u64> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn line(raw: &[u8]) -> Option<&[u8]> {
    let value = raw.strip_suffix(b"\n").unwrap_or(raw);
    (!value.is_empty() && value.iter().all(|c| (b' '..=b'~').contains(c))).then_some(value)
}

enum Packet<'a> {
    Data(&'a [u8]),
    Delimiter,
    Flush,
}

fn data<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    match packet(input)? {
        Packet::Data(value) => Some(value),
        _ => None,
    }
}

fn packet<'a>(input: &mut &'a [u8]) -> Option<Packet<'a>> {
    let header = input.get(..4)?;
    if !header.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let length = usize::from_str_radix(std::str::from_utf8(header).ok()?, 16).ok()?;
    let (packet, consumed) = match length {
        0 => (Packet::Flush, 4),
        1 => (Packet::Delimiter, 4),
        4..=65_520 => (Packet::Data(input.get(4..length)?), length),
        _ => return None,
    };
    *input = &input[consumed..];
    Some(packet)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WANT: &str = "want 1111111111111111111111111111111111111111\n";
    const HAVE: &str = "have 2222222222222222222222222222222222222222\n";

    fn pkt(body: &mut Vec<u8>, value: &str) {
        body.extend_from_slice(format!("{:04x}{value}", value.len() + 4).as_bytes());
    }

    fn request(caps: &[&str], args: &[&str]) -> Vec<u8> {
        let mut body = Vec::new();
        pkt(&mut body, "command=fetch\n");
        for cap in caps {
            pkt(&mut body, cap);
        }
        body.extend_from_slice(b"0001");
        for arg in args {
            pkt(&mut body, arg);
        }
        body.extend_from_slice(b"0000");
        body
    }

    fn key(body: &[u8]) -> Option<String> {
        cache_key("version=2", body, "repo", 1)
    }

    #[test]
    fn native_shallow_partial_clone_and_final_fetch_are_cacheable() {
        let caps = ["agent=git/2.55.0\n", "object-format=sha1\n"];
        for args in [
            vec![
                "thin-pack\n",
                "no-progress\n",
                "ofs-delta\n",
                "deepen 1\n",
                "filter blob:none\n",
                WANT,
                "done\n",
            ],
            vec![
                "include-tag\n",
                "sideband-all\n",
                "wait-for-done\n",
                "shallow 3333333333333333333333333333333333333333\n",
                "deepen-relative\n",
                "deepen 2\n",
                "filter blob:limit=1024\n",
                WANT,
                HAVE,
                "done\n",
            ],
        ] {
            assert!(key(&request(&caps, &args)).is_some());
        }
    }

    #[test]
    fn only_agent_and_session_capabilities_are_ignored() {
        let a = request(
            &["agent=git/a\n", "session-id=one\n", "object-format=sha1\n"],
            &[WANT, "done\n"],
        );
        let b = request(
            &["object-format=sha1\n", "session-id=two\n", "agent=git/b\n"],
            &[WANT, "done\n"],
        );
        let absent = request(&["object-format=sha1\n"], &[WANT, "done\n"]);
        assert_eq!(key(&a), key(&b));
        assert_eq!(key(&a), key(&absent));
        assert_ne!(key(&a), key(&request(&[], &[WANT, "done\n"])));
    }

    #[test]
    fn pack_inputs_and_repository_generation_are_distinct() {
        let base = request(&[], &[WANT, "done\n"]);
        for args in [
            vec![WANT, HAVE, "done\n"],
            vec![WANT, "deepen 1\n", "done\n"],
            vec![WANT, "filter blob:none\n", "done\n"],
            vec![WANT, "include-tag\n", "done\n"],
            vec![WANT, "thin-pack\n", "done\n"],
        ] {
            assert_ne!(key(&base), key(&request(&[], &args)));
        }
        assert_ne!(key(&base), cache_key("version=2", &base, "other", 1));
        assert_ne!(key(&base), cache_key("version=2", &base, "repo", 2));
        assert_ne!(
            key(&request(&[], &[WANT, HAVE, "done\n"])),
            key(&request(&[], &[HAVE, WANT, "done\n"]))
        );
    }

    #[test]
    fn negotiation_ref_inputs_and_unknown_extensions_bypass() {
        for args in [
            vec![WANT],
            vec!["done\n"],
            vec![WANT, "done\n", "done\n"],
            vec![WANT, "done\n", HAVE],
            vec!["want HEAD\n", "done\n"],
            vec![WANT, "want-ref refs/heads/main\n", "done\n"],
            vec![WANT, "deepen-not main\n", "done\n"],
            vec![WANT, "deepen-since 123\n", "done\n"],
            vec![WANT, "packfile-uris https\n", "done\n"],
            vec![WANT, "filter tree:1\n", "done\n"],
            vec![WANT, "filter blob:limit=1k\n", "done\n"],
            vec![WANT, "deepen 0\n", "done\n"],
            vec![WANT, "deepen 2147483648\n", "done\n"],
            vec![WANT, "filter blob:limit=18446744073709551616\n", "done\n"],
            vec![WANT, "unknown\n", "done\n"],
        ] {
            assert!(key(&request(&[], &args)).is_none(), "{args:?}");
        }
        for cap in [
            "object-format=sha256\n",
            "server-option=anything\n",
            "command=fetch\n",
            "unknown\n",
        ] {
            assert!(key(&request(&[cap], &[WANT, "done\n"])).is_none());
        }
        let body = request(&[], &[WANT, "done\n"]);
        for protocol in ["", "version=1", "version=2:agent=x", "version=2\n"] {
            assert!(cache_key(protocol, &body, "repo", 1).is_none());
        }
    }

    #[test]
    fn malformed_and_truncated_packets_never_get_keys() {
        let valid = request(&[], &[WANT, "done\n"]);
        for end in 0..valid.len() {
            assert!(key(&valid[..end]).is_none());
        }
        for tail in [
            b"0000".as_slice(),
            b"0001",
            b"0002",
            b"0003",
            b"0004",
            b"fff1",
            b"zzzz",
            b"0005x",
        ] {
            let mut body = valid.clone();
            body.extend_from_slice(tail);
            assert!(key(&body).is_none());
        }
        for value in ["done\nextra", "done\0", "done\r\n", "", "\n"] {
            assert!(key(&request(&[], &[WANT, value])).is_none());
        }
        for control in [
            b"0000".as_slice(),
            b"0002",
            b"0003",
            b"0004",
            b"fff1",
            b"zzzz",
        ] {
            let mut body = Vec::new();
            pkt(&mut body, "command=fetch\n");
            body.extend_from_slice(control);
            assert!(key(&body).is_none());
        }
    }

    #[test]
    fn excessive_wants_bypass() {
        let mut args = vec![WANT; MAX_WANTS];
        args.push("done\n");
        assert!(key(&request(&[], &args)).is_some());
        args.insert(0, WANT);
        assert!(key(&request(&[], &args)).is_none());
    }
}
