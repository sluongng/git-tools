//! Versioned native-byte overlay records shared by the persistent filesystem.
use super::{Entry, Kind, Payload, parse_time, timestamp};
use anyhow::{Context, Result, ensure};
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};

// Each entry is a versioned binary record. Length prefixes preserve native names
// and store file data verbatim rather than expanding each byte into JSON digits.
fn put_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        None => out.extend_from_slice(&u64::MAX.to_be_bytes()),
    }
}
pub(super) fn encode(e: &Entry) -> Vec<u8> {
    let mut out = vec![3];
    for n in [e.inode, e.parent, e.kind.code() as u64, e.mode as u64] {
        out.extend_from_slice(&n.to_be_bytes());
    }
    out.push(u8::from(e.linked));
    out.push(u8::from(e.control));
    put_bytes(&mut out, Some(e.name.as_bytes()));
    put_bytes(&mut out, e.base_oid.as_deref().map(str::as_bytes));
    match &e.payload {
        Payload::Base => out.push(0),
        Payload::None => out.push(1),
        Payload::Inline(bytes) => {
            out.push(2);
            put_bytes(&mut out, Some(bytes));
        }
        Payload::File { size, generation } => {
            out.push(3);
            out.extend_from_slice(&size.to_be_bytes());
            out.extend_from_slice(&generation.to_be_bytes());
        }
    }
    put_bytes(&mut out, Some(timestamp(e.mtime).as_bytes()));
    put_bytes(&mut out, Some(timestamp(e.ctime).as_bytes()));
    put_bytes(&mut out, Some(timestamp(e.atime).as_bytes()));
    out
}
struct Decoder<'a>(&'a [u8]);
impl<'a> Decoder<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(n <= self.0.len(), "truncated overlay entry");
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn number(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }
    fn bytes(&mut self) -> Result<Option<&'a [u8]>> {
        let len = self.number()?;
        if len == u64::MAX {
            Ok(None)
        } else {
            Ok(Some(self.take(usize::try_from(len)?)?))
        }
    }
    fn required(&mut self) -> Result<&'a [u8]> {
        self.bytes()?.context("missing overlay entry field")
    }
    fn boolean(&mut self) -> Result<bool> {
        let b = self.take(1)?[0];
        ensure!(b <= 1, "invalid overlay boolean");
        Ok(b == 1)
    }
}
// Borrow and validate dead payload bytes without copying them into the live map.
pub(super) fn decode_for_open(bytes: &[u8]) -> Result<(Entry, Option<u64>)> {
    decode(bytes, true)
}
pub(super) fn decode_exact(bytes: &[u8]) -> Result<Entry> {
    Ok(decode(bytes, false)?.0)
}
fn decode(bytes: &[u8], prune: bool) -> Result<(Entry, Option<u64>)> {
    let mut d = Decoder(bytes);
    let version = d.take(1)?[0];
    ensure!(
        matches!(version, 1 | 2 | 3),
        "unsupported overlay entry version"
    );
    let inode = d.number()?;
    let parent = d.number()?;
    let kind = Kind::decode(i64::try_from(d.number()?)?)?;
    let mode = u32::try_from(d.number()?)?;
    let linked = d.boolean()?;
    let control = d.boolean()?;
    let name = OsString::from_vec(d.required()?.to_vec());
    let base_oid = d
        .bytes()?
        .map(|bytes| String::from_utf8(bytes.to_vec()))
        .transpose()?;
    let mut pruned_inline = None;
    let mut payload = if version == 1 {
        match d.bytes()? {
            Some(bytes) if prune && !linked && !control => {
                // Validate the remaining record before returning the pruned state.
                let len = bytes.len() as u64;
                let mtime = parse_time(std::str::from_utf8(d.required()?)?.to_owned())?;
                let ctime = parse_time(std::str::from_utf8(d.required()?)?.to_owned())?;
                ensure!(d.0.is_empty(), "trailing overlay entry data");
                return Ok((
                    Entry {
                        inode,
                        parent,
                        name,
                        kind,
                        mode,
                        base_oid,
                        payload: Payload::None,
                        atime: mtime,
                        mtime,
                        ctime,
                        linked,
                        control,
                    },
                    Some(len),
                ));
            }
            Some(bytes) => Payload::Inline(bytes.to_vec()),
            None if base_oid.is_some() => Payload::Base,
            None => Payload::None,
        }
    } else {
        match d.take(1)?[0] {
            0 => {
                ensure!(base_oid.is_some(), "base payload lacks identity");
                Payload::Base
            }
            1 => Payload::None,
            2 => {
                let bytes = d.required()?;
                if prune && !linked && !control {
                    pruned_inline = Some(bytes.len() as u64);
                    Payload::None
                } else {
                    Payload::Inline(bytes.to_vec())
                }
            }
            3 => Payload::File {
                size: d.number()?,
                generation: d.number()?,
            },
            _ => anyhow::bail!("unsupported payload state"),
        }
    };
    let reclaimed = if prune && !linked && !control {
        let size = pruned_inline.or_else(|| payload.size());
        payload = Payload::None;
        size
    } else {
        None
    };
    let mtime = parse_time(std::str::from_utf8(d.required()?)?.to_owned())?;
    let ctime = parse_time(std::str::from_utf8(d.required()?)?.to_owned())?;
    let atime = if version >= 3 {
        parse_time(std::str::from_utf8(d.required()?)?.to_owned())?
    } else {
        mtime
    };
    ensure!(d.0.is_empty(), "trailing overlay entry data");
    Ok((
        Entry {
            inode,
            parent,
            name,
            kind,
            mode,
            base_oid,
            payload,
            atime,
            mtime,
            ctime,
            linked,
            control,
        },
        reclaimed,
    ))
}
