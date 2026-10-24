//! Bounded literal matching and versioned candidate filters. A filter is not a
//! content-integrity or repository-coverage proof; callers verify those sources.
pub const ALGORITHM_VERSION: u32 = 1;
pub const FILTER_BYTES: usize = 512;
pub const MAX_PATTERN_BYTES: usize = 1024;
pub const MAX_SNIPPET_BYTES: usize = MAX_PATTERN_BYTES + 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid,
    Limit,
    Cancelled,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid literal-search input",
            Self::Limit => "literal-search budget exceeded",
            Self::Cancelled => "literal search cancelled",
        })
    }
}
impl std::error::Error for Error {}

pub fn validate_pattern(pattern: &[u8]) -> Result<(), Error> {
    if pattern.is_empty() || pattern.len() > MAX_PATTERN_BYTES || pattern.contains(&b'\n') {
        Err(Error::Invalid)
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileFilter([u8; FILTER_BYTES]);
impl FileFilter {
    pub fn from_bytes(bytes: [u8; FILTER_BYTES]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; FILTER_BYTES] {
        &self.0
    }
    /// False excludes a literal; true requires exact content verification.
    /// Short patterns have no trigram exclusion proof and always return true.
    pub fn may_match(&self, pattern: &[u8]) -> bool {
        pattern.windows(3).all(|gram| {
            positions(gram)
                .into_iter()
                .all(|bit| self.0[bit / 8] & (1 << (bit % 8)) != 0)
        })
    }
}
fn positions(gram: &[u8]) -> [usize; 3] {
    let value = u32::from(gram[0]) | (u32::from(gram[1]) << 8) | (u32::from(gram[2]) << 16);
    let mut mixed = value.wrapping_mul(0x9e37_79b1);
    mixed ^= mixed >> 16;
    let second = mixed.rotate_left(13).wrapping_mul(0x85eb_ca6b);
    let mask = (FILTER_BYTES * 8 - 1) as u32;
    [
        (mixed & mask) as usize,
        (second & mask) as usize,
        ((mixed ^ second.rotate_left(7)) & mask) as usize,
    ]
}
pub struct FilterBuilder {
    filter: FileFilter,
    tail: [u8; 2],
    tail_len: usize,
}
impl Default for FilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl FilterBuilder {
    pub fn new() -> Self {
        Self {
            filter: FileFilter([0; FILTER_BYTES]),
            tail: [0; 2],
            tail_len: 0,
        }
    }
    pub fn update(&mut self, bytes: &[u8]) {
        self.update_checked(bytes, || true)
            .expect("noncancelling filter update");
    }
    pub fn update_checked(
        &mut self,
        bytes: &[u8],
        mut check: impl FnMut() -> bool,
    ) -> Result<(), Error> {
        for (index, &byte) in bytes.iter().enumerate() {
            if index % 1024 == 0 && !check() {
                return Err(Error::Cancelled);
            }
            if self.tail_len == 2 {
                for bit in positions(&[self.tail[0], self.tail[1], byte]) {
                    self.filter.0[bit / 8] |= 1 << (bit % 8);
                }
                self.tail[0] = self.tail[1];
                self.tail[1] = byte;
            } else {
                self.tail[self.tail_len] = byte;
                self.tail_len += 1;
            }
        }
        Ok(())
    }
    pub fn finish(self) -> FileFilter {
        self.filter
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MatchLimits {
    pub max_input_bytes: usize,
    pub max_work: usize,
    pub max_output_bytes: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MatchUsage {
    pub input_bytes: usize,
    pub work: usize,
    pub output_bytes: usize,
}
#[derive(Clone, Debug)]
pub struct MatchBudget {
    limits: MatchLimits,
    usage: MatchUsage,
}
impl MatchBudget {
    pub fn new(limits: MatchLimits) -> Self {
        Self {
            limits,
            usage: MatchUsage::default(),
        }
    }
    pub fn usage(&self) -> MatchUsage {
        self.usage
    }
    fn add(value: &mut usize, amount: usize, maximum: usize) -> Result<(), Error> {
        *value = value.checked_add(amount).ok_or(Error::Limit)?;
        if *value > maximum {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
    fn work(&mut self, amount: usize, check: &mut impl FnMut() -> bool) -> Result<(), Error> {
        if !check() {
            return Err(Error::Cancelled);
        }
        Self::add(&mut self.usage.work, amount, self.limits.max_work)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hit {
    pub offset: u64,
    pub length: u32,
    pub line: u64,
    /// One-based byte column. Wire protocols choosing zero-based columns must
    /// subtract one explicitly; offsets remain zero-based absolute byte offsets.
    pub column: u64,
    pub snippet_offset: u64,
    pub snippet: Vec<u8>,
    pub truncated_prefix: bool,
    pub truncated_suffix: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchPage {
    pub hits: Vec<Hit>,
    /// Resume immediately after the last nonoverlapping match. None means the
    /// complete file was considered; a continuation may yield an empty last page.
    pub next_offset: Option<u64>,
}

pub fn scan(
    bytes: &[u8],
    pattern: &[u8],
    start_offset: u64,
    max_hits: usize,
    budget: &mut MatchBudget,
    mut check: impl FnMut() -> bool,
) -> Result<MatchPage, Error> {
    validate_pattern(pattern)?;
    let start = usize::try_from(start_offset).map_err(|_| Error::Invalid)?;
    if start > bytes.len() || max_hits == 0 || max_hits > 65_536 {
        return Err(Error::Invalid);
    }
    budget.work(1, &mut check)?;
    MatchBudget::add(
        &mut budget.usage.input_bytes,
        bytes.len(),
        budget.limits.max_input_bytes,
    )?;
    // Prefix construction and comparisons are linear; no unbounded regex engine.
    let mut prefix = vec![0; pattern.len()];
    let mut matched = 0;
    for index in 1..pattern.len() {
        budget.work(1, &mut check)?;
        while matched > 0 && pattern[index] != pattern[matched] {
            budget.work(1, &mut check)?;
            matched = prefix[matched - 1];
        }
        if pattern[index] == pattern[matched] {
            matched += 1;
        }
        prefix[index] = matched;
    }
    let mut line = 1u64;
    let mut line_start = 0usize;
    for (index, &byte) in bytes[..start].iter().enumerate() {
        budget.work(1, &mut check)?;
        if byte == b'\n' {
            line += 1;
            line_start = index + 1;
        }
    }
    let mut hits = Vec::new();
    matched = 0;
    for index in start..bytes.len() {
        budget.work(1, &mut check)?;
        let byte = bytes[index];
        while matched > 0 && byte != pattern[matched] {
            budget.work(1, &mut check)?;
            matched = prefix[matched - 1];
        }
        if byte == pattern[matched] {
            matched += 1;
        }
        if byte == b'\n' {
            line += 1;
            line_start = index + 1;
        }
        if matched == pattern.len() {
            let offset = index + 1 - pattern.len();
            let snippet_start = line_start.max(offset.saturating_sub(128));
            let maximum = bytes
                .len()
                .min(snippet_start.saturating_add(MAX_SNIPPET_BYTES))
                .min(index.saturating_add(129));
            let mut end = snippet_start;
            while end < maximum && bytes[end] != b'\n' {
                budget.work(1, &mut check)?;
                end += 1;
            }
            let charge = std::mem::size_of::<Hit>()
                .saturating_add(end - snippet_start)
                .saturating_mul(2);
            MatchBudget::add(
                &mut budget.usage.output_bytes,
                charge,
                budget.limits.max_output_bytes,
            )?;
            // Avoid Vec's initial four-item allocation exceeding the first
            // hit's charged 2x growth window for tiny snippets.
            if hits.capacity() == 0 {
                hits.reserve_exact(1);
            }
            hits.push(Hit {
                offset: offset as u64,
                length: pattern.len() as u32,
                line,
                column: (offset - line_start + 1) as u64,
                snippet_offset: snippet_start as u64,
                snippet: bytes[snippet_start..end].to_vec(),
                truncated_prefix: snippet_start > line_start,
                truncated_suffix: end < bytes.len() && bytes[end] != b'\n',
            });
            matched = 0;
            if hits.len() == max_hits {
                return Ok(MatchPage {
                    hits,
                    next_offset: (index + 1 < bytes.len()).then_some((index + 1) as u64),
                });
            }
        }
    }
    Ok(MatchPage {
        hits,
        next_offset: None,
    })
}

#[cfg(test)]
mod tests;
