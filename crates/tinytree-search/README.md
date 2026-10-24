# tinytree-search · library

Bounded regex and case-sensitive byte-literal matching shared by remote search
and local workspace overlays. Regex syntax is UTF-8 with ASCII byte semantics
by default, including `\b` and case folding. Inline flags such as `(?i:foo)` and
`(?u:é+)` are supported. Backreferences, lookaround, Unicode word boundaries,
and expressions exceeding the compiled-program limits are rejected.

Regex matches are nonoverlapping and confined to individual LF-delimited lines,
even with dot-all enabled. Zero-width matches are skipped. Matches have byte
offsets, actual byte lengths and bounded line snippets containing the complete
match. A match wider than 1024 bytes exhausts the search budget rather than
returning an incomplete snippet. Paging preserves the original line and word
boundary context.

The regex plan derives conservative AND/OR conditions from required literal
trigrams. Existing per-file Bloom filters exclude impossible candidates;
optional text, short alternatives and unindexable classes fall back to scanning.
Positive candidates always require matching against verified content. The
filter format is unchanged and is not an integrity or coverage proof.

Forward and reverse deterministic automata have bounded compilation memory.
Every search transition is charged to the work budget and checks cancellation;
output and input have separate limits. Exhaustion returns an error, never a
partial page presented as complete.
