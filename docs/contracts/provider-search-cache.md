# Provider-search cache recovery

The provider-search SQLite cache is separate from the symbol-directory pickle
generation. Its existing `/tmp/justhodl-provider-search.sqlite` path, query
columns, native resources and schedules remain unchanged.

The reader requires a complete manifest and typed index metadata: a known
provider-search object location, compressed SHA-256, positive compressed and
expanded sizes, and the `sqlite-fts5+gzip` format. The existing 1500 MiB expanded
database limit is retained. Missing, malformed, future or regressing manifests
raise an error instead of becoming an empty catalog. Cache identity includes
all artifact metadata, so a replacement with the same timestamp is reloaded.

The candidate download is checked before any existing database is removed.
Expansion is bounded by the declared size, validates the complete gzip stream,
and leaves 64 MiB disk headroom. A read-only SQLite check requires a valid database
and the FTS5 table/columns used by the existing query. It does not verify economic
meaning, completeness of the catalog or the original providers' observations.

When both expanded databases fit, the previous file remains until atomic
replacement. Under disk pressure, the old file is streamed into a compressed
checkpoint and its whole decompressed size and hash are verified first. The old
file is released only when that checkpoint exists and the candidate can then fit.
An ordinary expansion/validation failure removes partial staging files, restores
the exact previous database and keeps the prior cache metadata. If acquisition,
checkpointing or capacity checks fail earlier, the original file remains intact.
Temporary staging is cleaned after success and failure.

This cannot recover from process termination, host loss, unrecoverable disk
corruption or an exhausted environment that cannot restore its checkpoint.
Production population capacity and latency remain unverified. Full disks can
defer refreshes; no AWS resource increase is included. Retaining the prior file
does not turn a failed current request into a successful source check: queries
with unavailable manifests report an error, and the warm response marks that
warehouse unavailable. Other successful native search results remain available.

Search and warm responses include add-only warehouse integrity metadata. Current
availability is separate from the last verified artifact identity and its clocks;
the local filesystem path is omitted. Caller mutation cannot change cached
identity metadata. No source replay or investment authority is granted. Existing
rows, errors, counts and pagination fields remain.

The complete preceding native source and complete invented SQLite/reproduction
bodies are retained in `tests/fixtures/symbol-directory/`. Native regressions
exercise real SQLite queries, low-space checkpoint recovery, failed acquisition,
corrupt gzip, incompatible schemas, same-clock replacements and unavailable-source
reporting. No actual AWS catalog or consumer data is used. Operation 6407 checks
the exact package, public release receipt and original resource/schedule settings
without invoking the engine or accessing current/private/account/consumer bodies.
