use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{ChildStdin, ChildStdout, Stdio};

use super::background_process::{BackgroundCancellation, NativeChild};

use anyhow::{Context, Result, ensure};

/// One serialized native reader per repository. Only immutable, full blob OIDs
/// enter this protocol; metadata commands deliberately use separate no-fetch Git.
pub(super) struct BlobReader {
    _child: NativeChild,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl BlobReader {
    pub(super) fn start(
        repository: &Path,
        binary: &Path,
        cancellation: Option<&BackgroundCancellation>,
    ) -> Result<Self> {
        Self::start_with_fetch(repository, binary, cancellation, true)
    }

    pub(super) fn start_local(
        repository: &Path,
        binary: &Path,
        cancellation: Option<&BackgroundCancellation>,
    ) -> Result<Self> {
        Self::start_with_fetch(repository, binary, cancellation, false)
    }

    fn start_with_fetch(
        repository: &Path,
        binary: &Path,
        cancellation: Option<&BackgroundCancellation>,
        allow_fetch: bool,
    ) -> Result<Self> {
        let mut command =
            super::git_command(repository, binary, &["cat-file", "--batch"], allow_fetch);
        // A private process group also owns any native promisor-fetch children.
        // Discard stderr directly: no pipe can fill, and credential-bearing URLs
        // never enter errors or an unbounded diagnostic buffer.
        command.stdin(Stdio::piped()).stderr(Stdio::null());
        let mut child =
            NativeChild::spawn(&mut command, cancellation).context("start Git blob reader")?;
        let input = child
            .child
            .stdin
            .take()
            .expect("piped Git blob-reader stdin");
        let output = BufReader::new(
            child
                .child
                .stdout
                .take()
                .expect("piped Git blob-reader stdout"),
        );
        Ok(Self {
            _child: child,
            input,
            output,
        })
    }

    /// Stream a local object through fixed-size buffers. Missing objects leave
    /// the batch protocol aligned; callback errors require discarding this reader.
    pub(super) fn stream_local(
        &mut self,
        oid: &str,
        mut consume: impl FnMut(u64, u64, &[u8]) -> Result<()>,
    ) -> Result<Option<u64>> {
        writeln!(self.input, "{oid}")?;
        self.input.flush()?;
        let mut header = Vec::new();
        self.output
            .by_ref()
            .take(129)
            .read_until(b'\n', &mut header)?;
        ensure!(
            header.len() <= 128 && header.last() == Some(&b'\n'),
            "invalid bounded blob header"
        );
        let header = std::str::from_utf8(&header[..header.len() - 1])?;
        let fields: Vec<_> = header.split(' ').collect();
        if fields == [oid, "missing"] {
            return Ok(None);
        }
        ensure!(
            fields.len() == 3 && fields[0] == oid && fields[1] == "blob",
            "invalid local blob response"
        );
        ensure!(
            !fields[2].is_empty() && fields[2].bytes().all(|b| b.is_ascii_digit()),
            "invalid local blob size"
        );
        let size: u64 = fields[2].parse()?;
        consume(size, 0, &[])?;
        let mut buffer = [0u8; 65536];
        let mut offset = 0;
        while offset < size {
            let count = (size - offset).min(buffer.len() as u64) as usize;
            self.output.read_exact(&mut buffer[..count])?;
            consume(size, offset, &buffer[..count])?;
            offset += count as u64;
        }
        let mut newline = [0];
        self.output.read_exact(&mut newline)?;
        ensure!(newline == [b'\n'], "invalid local blob terminator");
        Ok(Some(size))
    }

    pub(super) fn read(&mut self, oid: &str) -> Result<Vec<u8>> {
        writeln!(self.input, "{oid}").context("request Git blob")?;
        self.input.flush().context("flush Git blob request")?;
        // SHA1, type and a decimal usize fit comfortably; a broken/wrapped Git
        // cannot make header parsing allocate without bound.
        let mut header = Vec::new();
        self.output
            .by_ref()
            .take(129)
            .read_until(b'\n', &mut header)
            .context("read Git blob header")?;
        ensure!(
            header.len() <= 128 && header.last() == Some(&b'\n'),
            "Git returned no bounded blob header"
        );
        let header = std::str::from_utf8(&header[..header.len() - 1])
            .context("Git returned non-UTF8 blob header")?;
        let fields: Vec<_> = header.split(' ').collect();
        ensure!(
            fields.len() == 3 && fields[0] == oid && fields[1] == "blob",
            "requested object is missing or is not a Git blob"
        );
        ensure!(
            !fields[2].is_empty() && fields[2].bytes().all(|b| b.is_ascii_digit()),
            "invalid Git blob length"
        );
        let size: usize = fields[2].parse().context("Git blob length exceeds usize")?;
        let mut data = Vec::new();
        data.try_reserve_exact(size)
            .context("allocate Git blob contents")?;
        data.resize(size, 0);
        self.output
            .read_exact(&mut data)
            .context("Git returned incomplete blob contents")?;
        let mut newline = [0];
        self.output
            .read_exact(&mut newline)
            .context("Git returned no blob terminator")?;
        ensure!(newline == [b'\n'], "Git returned invalid blob terminator");
        Ok(data)
    }
}
