//! Versioned native-byte overlay records shared by the persistent filesystem.
use super::{Entry, Kind, parse_time, timestamp};
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
    let mut out = vec![1];
    for n in [e.inode, e.parent, e.kind.code() as u64, e.mode as u64] {
        out.extend_from_slice(&n.to_be_bytes());
    }
    out.push(u8::from(e.linked));
    out.push(u8::from(e.control));
    put_bytes(&mut out, Some(e.name.as_bytes()));
    put_bytes(&mut out, e.base_oid.as_deref().map(str::as_bytes));
    put_bytes(&mut out, e.data.as_deref());
    put_bytes(&mut out, Some(timestamp(e.mtime).as_bytes()));
    put_bytes(&mut out, Some(timestamp(e.ctime).as_bytes()));
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
    let mut d = Decoder(bytes);
    ensure!(d.take(1)? == [1], "unsupported overlay entry version");
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
    let payload = d.bytes()?;
    let reclaimed = payload
        .filter(|_| !linked && !control)
        .map(|p| p.len() as u64);
    let data = if !linked && !control {
        None
    } else {
        payload.map(<[u8]>::to_vec)
    };
    let mtime = parse_time(std::str::from_utf8(d.required()?)?.to_owned())?;
    let ctime = parse_time(std::str::from_utf8(d.required()?)?.to_owned())?;
    ensure!(d.0.is_empty(), "trailing overlay entry data");
    Ok((
        Entry {
            inode,
            parent,
            name,
            kind,
            mode,
            base_oid,
            data,
            mtime,
            ctime,
            linked,
            control,
        },
        reclaimed,
    ))
}
