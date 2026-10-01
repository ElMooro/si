# Symbol-directory generation integrity

This contract concerns the server search index. It does not qualify the meaning,
freshness, coverage or security identity of its underlying catalog data.

New builds write the complete compressed docs and index under content-derived
generation keys before changing legacy aliases. Only after those writes succeed
does the existing manifest name the exact pair, including sizes and SHA-256
digests. The index also records its document digest, version and build clock.
Legacy keys and fields remain for existing consumers. Immutable generation
retention currently has no automatic pruning; capacity/lifecycle management is a
separate operational concern. Concurrent publishers cannot mix one reader's pair,
but last-writer ordering of the manifest is not a compare-and-swap guarantee.

A reader pins one manifest and retrieves only that pair, even if another build
publishes while it downloads. It verifies complete transport, compressed hashes,
gzip trailers, a restricted pickle class set, document/popularity alignment,
sorted token and row mappings, posting ordinals and UTC clocks. Arbitrary pickle
classes, trailing data and invalid identity mappings are rejected. No remote
module or account object is fetched by these helpers. A hash identifies received
bytes; it is not proof of their economic correctness or source independence.

Older manifests have no binding between their two mutable files. During that
migration, the reader uses the complete docs object and reconstructs its derived
index using the same token/identity builder as normal publication. It reports
`legacy_reconstructed_from_single_docs_object`; it never certifies the old index
head. New hash-bound pairs report `hash_bound_generation`. Both states retain the
document digest and deny investment authority. Search and warm responses expose
this separately from their existing build time.

Refreshes stage the candidate files and a streamed compressed rollback of the
current cache in a unique temporary directory. Downloads and rollback writes
require 64 MiB of remaining disk headroom; a full disk rejects the refresh before
the old graph is released. The rollback gzip is checked in bounded chunks.
Candidate decoding then replaces the old graph without deliberately retaining
two decoded generations. An ordinary parse/validation failure releases candidate
references and tracebacks before reconstructing the old cache. The failed refresh
still raises an error; it does not claim a new successful load. An older UTC build
cannot replace a newer cached build, and a future build clock is rejected.
Warm checks compare generation identity even when build timestamps are equal. A
failed manifest read or invalid/regressing head raises an error while preserving
the working cache; it no longer reports a successful check through an empty fallback.

This is not recovery from process termination, host loss, disk corruption after
checkpoint validation or an unrecoverable memory kill. The separate SQLite provider-warehouse refresh has its own candidate contract
in [provider-search-cache.md](provider-search-cache.md) and requires separate
release acceptance. Temporary-space contention can defer
a refresh, and actual AWS population capacity/latency has not been measured.
The reproducible optional synthetic benchmark is
`python tests/benchmark_directory_index.py 500000`; it requires no provider or
AWS credentials. Native regressions use complete invented inputs and block HTTP.

The native resources and all seven original schedules remain unchanged. Operation
6406 inspects exact package bytes, the public release receipt and selected control
settings without invoking a producer, accessing current/private/account/consumer
packets or changing schedules. Its success would prove deployed code and controls,
not normal new-code publication or independent real-source replay.
