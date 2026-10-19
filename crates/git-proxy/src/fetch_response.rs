//! Bounded framing checks before publishing a completed protocol-v2 fetch response.
//!
//! This is not a pack/index verifier: native Git remains responsible for pack
//! generation. It prevents protocol errors and incomplete HTTP/CGI output from
//! becoming reusable successful cache entries.
use anyhow::{Result, bail, ensure};

// Git's pkt-line.h: LARGE_PACKET_MAX includes the four-byte length prefix.
const MAX_PACKET: usize = 65_520;

pub(crate) struct FetchResponseValidator {
    packet: Vec<u8>,
    in_pack: bool,
    terminal: bool,
    response_end: bool,
    pack_prefix: Vec<u8>,
    pack_bytes: u64,
}

impl FetchResponseValidator {
    pub(crate) fn new() -> Self {
        Self {
            packet: Vec::new(),
            in_pack: false,
            terminal: false,
            response_end: false,
            pack_prefix: Vec::with_capacity(12),
            pack_bytes: 0,
        }
    }

    pub(crate) fn feed(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            if self.packet.len() < 4 {
                let take = (4 - self.packet.len()).min(bytes.len());
                self.packet.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                if self.packet.len() < 4 {
                    break;
                }
            }
            let mut length = 0usize;
            for &byte in &self.packet[..4] {
                let digit = match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    b'A'..=b'F' => byte - b'A' + 10,
                    _ => bail!("invalid fetch pkt-line length"),
                };
                length = length * 16 + usize::from(digit);
            }
            ensure!(
                length != 3 && length <= MAX_PACKET,
                "invalid fetch pkt-line size"
            );
            if length <= 2 {
                self.control(length)?;
                self.packet.clear();
                continue;
            }
            ensure!(!self.terminal, "fetch data after terminal packet");
            let take = (length - self.packet.len()).min(bytes.len());
            self.packet.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.packet.len() == length {
                let mut packet = std::mem::take(&mut self.packet);
                self.payload(&packet[4..])?;
                packet.clear();
                self.packet = packet;
            }
        }
        Ok(())
    }

    fn control(&mut self, length: usize) -> Result<()> {
        match length {
            0 | 2 => {
                ensure!(self.in_pack, "fetch ended without packfile section");
                if self.terminal {
                    ensure!(
                        length == 2 && !self.response_end,
                        "duplicate fetch terminator"
                    );
                }
                self.terminal = true;
                self.response_end = length == 2;
            }
            1 => ensure!(
                !self.in_pack && !self.terminal,
                "delimiter inside or after packfile"
            ),
            _ => unreachable!(),
        }
        Ok(())
    }

    fn payload(&mut self, payload: &[u8]) -> Result<()> {
        ensure!(!payload.is_empty(), "empty fetch response packet");
        let data = match payload[0] {
            3 => bail!("fatal sideband in fetch response"),
            2 => return Ok(()), // Progress and keepalives never contain pack data.
            1 => &payload[1..],
            _ if self.in_pack => bail!("packfile data is not multiplexed"),
            _ => payload,
        };
        if self.in_pack {
            let take = (12 - self.pack_prefix.len()).min(data.len());
            self.pack_prefix.extend_from_slice(&data[..take]);
            self.pack_bytes = self
                .pack_bytes
                .checked_add(data.len() as u64)
                .ok_or_else(|| anyhow::anyhow!("fetch pack size overflow"))?;
            if self.pack_prefix.len() >= 4 {
                ensure!(&self.pack_prefix[..4] == b"PACK", "missing PACK signature");
            }
            if self.pack_prefix.len() >= 8 {
                let version = u32::from_be_bytes(self.pack_prefix[4..8].try_into().unwrap());
                ensure!(version == 2 || version == 3, "unsupported pack version");
            }
        } else {
            ensure!(
                !data.starts_with(b"ERR ") && data != b"ERR" && data != b"ERR\n",
                "Git fetch error response"
            );
            if data == b"packfile\n" || data == b"packfile" {
                self.in_pack = true;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<()> {
        ensure!(self.packet.is_empty(), "truncated fetch pkt-line");
        ensure!(self.in_pack && self.terminal, "incomplete fetch response");
        // A SHA-1 pack has a 12-byte header and a 20-byte trailer, even with
        // zero objects. Request eligibility restricts this cache to SHA-1.
        ensure!(
            self.pack_prefix.len() == 12 && self.pack_bytes >= 32,
            "truncated packfile"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pkt(payload: &[u8]) -> Vec<u8> {
        let mut result = format!("{:04x}", payload.len() + 4).into_bytes();
        result.extend_from_slice(payload);
        result
    }

    fn pack() -> Vec<u8> {
        let mut result = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
        result.extend_from_slice(&[0; 20]);
        result
    }

    fn response(sideband_all: bool) -> Vec<u8> {
        let mut bytes = pkt(if sideband_all {
            b"\x01shallow-info\n"
        } else {
            b"shallow-info\n"
        });
        bytes.extend(pkt(if sideband_all {
            b"\x01shallow abc\n"
        } else {
            b"shallow abc\n"
        }));
        bytes.extend_from_slice(b"0001");
        bytes.extend(pkt(if sideband_all {
            b"\x01packfile\n"
        } else {
            b"packfile\n"
        }));
        bytes.extend(pkt(b"\x02"));
        bytes.extend(pkt(&[&b"\x01"[..], &pack()].concat()));
        bytes.extend_from_slice(b"00000002");
        bytes
    }

    fn validate(bytes: &[u8], chunk: usize) -> Result<()> {
        let mut validator = FetchResponseValidator::new();
        for bytes in bytes.chunks(chunk) {
            validator.feed(bytes)?;
        }
        validator.finish()
    }

    #[test]
    fn accepts_multiplexing_sections_and_every_chunk_boundary() {
        for sideband_all in [false, true] {
            let bytes = response(sideband_all);
            for chunk in 1..=bytes.len() {
                validate(&bytes, chunk).unwrap();
            }
        }
    }

    #[test]
    fn rejects_errors_before_and_inside_pack_transfer() {
        for payload in [
            &b"ERR upload failure\n"[..],
            b"\x01ERR upload failure\n",
            b"\x03fatal failure\n",
        ] {
            assert!(validate(&pkt(payload), 1).is_err());
            let mut bytes = pkt(b"packfile\n");
            bytes.extend(pkt(payload));
            bytes.extend_from_slice(b"0000");
            assert!(validate(&bytes, 1).is_err());
        }
        let mut bytes = response(false);
        bytes.truncate(bytes.len() - 8);
        bytes.extend(pkt(b"\x03fatal after pack\n"));
        bytes.extend_from_slice(b"0000");
        assert!(validate(&bytes, 1).is_err());
    }

    #[test]
    fn does_not_interpret_pack_data_as_error_text() {
        let mut bytes = pkt(b"packfile\n");
        let mut data = pack();
        data.extend_from_slice(b"ERR inside pack is binary data\x03");
        for chunk in data.chunks(2) {
            bytes.extend(pkt(&[&b"\x01"[..], chunk].concat()));
        }
        bytes.extend_from_slice(b"0000");
        validate(&bytes, 1).unwrap();
    }

    #[test]
    fn rejects_truncation_invalid_framing_and_trailing_data() {
        let complete = response(false);
        // The optional response-end follows an already complete flush.
        for end in 0..complete.len() - 4 {
            assert!(validate(&complete[..end], 3).is_err(), "prefix {end}");
        }
        for invalid in [&b"zzzz"[..], b"0003", b"fff1", b"0004", b"0000", b"0002"] {
            assert!(validate(invalid, 2).is_err());
        }
        for tail in [&b"0001"[..], b"0000", b"0002", b"0008data"] {
            let mut bytes = complete.clone();
            bytes.extend_from_slice(tail);
            assert!(validate(&bytes, 2).is_err());
        }
    }

    #[test]
    fn accepts_native_git_final_fetch_and_empty_pack() {
        use std::{
            io::Write,
            process::{Command, Stdio},
        };
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository.git");
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        git(&["init", "--bare", repository.to_str().unwrap()]);
        let mut import = Command::new("git")
            .arg("--git-dir")
            .arg(&repository)
            .arg("fast-import")
            .arg("--quiet")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        import.stdin.take().unwrap().write_all(
            b"blob\nmark :1\ndata 5\nhello\ncommit refs/heads/main\ncommitter Test <test@example.invalid> 1 +0000\ndata 4\ntest\nM 100644 :1 hello.txt\n\ndone\n"
        ).unwrap();
        let output = import.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let output = git(&[
            "--git-dir",
            repository.to_str().unwrap(),
            "rev-parse",
            "refs/heads/main",
        ]);
        let oid = String::from_utf8(output.stdout).unwrap().trim().to_owned();
        for (sideband_all, empty_pack) in [(false, false), (true, false), (false, true)] {
            let mut request = pkt(b"command=fetch\n");
            request.extend_from_slice(b"0001");
            request.extend(pkt(format!("want {oid}\n").as_bytes()));
            if sideband_all {
                request.extend(pkt(b"sideband-all\n"));
            }
            if empty_pack {
                request.extend(pkt(format!("have {oid}\n").as_bytes()));
            }
            request.extend(pkt(b"done\n"));
            request.extend_from_slice(b"0000");
            let mut child = Command::new("git")
                .env("GIT_PROTOCOL", "version=2")
                .args([
                    "-c",
                    "uploadpack.allowSidebandAll=true",
                    "upload-pack",
                    "--stateless-rpc",
                ])
                .arg(&repository)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(&request).unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            validate(&output.stdout, 1).unwrap();
        }
    }

    #[test]
    fn requires_pack_signature_and_minimum_size() {
        for data in [
            &b"not a pack but a sufficiently long stream"[..],
            b"PACK\0\0\0\x02",
            b"PACK\0\0\0\x04\0\0\0\0abcdefghijklmnopqrst",
        ] {
            let mut bytes = pkt(b"packfile\n");
            bytes.extend(pkt(&[&b"\x01"[..], data].concat()));
            bytes.extend_from_slice(b"0000");
            assert!(validate(&bytes, 3).is_err());
        }
    }
}
