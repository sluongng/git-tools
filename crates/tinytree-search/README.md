# tinytree-search · library

Bounded, case-sensitive byte-literal matching shared by remote search and local
workspace overlays. Matches are nonoverlapping, with byte offsets and bounded
line snippets. The reusable trigram Bloom filter excludes impossible files;
positive candidates still require exact matching against verified content.
