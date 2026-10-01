# Symbol-directory publication ordering

This contract covers the upgraded native directory builder. It does not qualify
catalog observations, market coverage, instrument identity or investment signals.
Existing sources, calculation fields, aliases and schedules remain.

Build start, build completion and publication preparation have separate clocks.
Complete docs, index and instrument-catalog bytes land under content-derived keys
before the manifest advances. Existing immutable objects must match their whole
expected bytes; a matching name or metadata is insufficient. The existing paired
index descriptor remains, with a separate instrument-catalog descriptor added.

The manifest is the authoritative selection point. Creation requires absence;
replacement requires the ETag of the complete previously read manifest. Conflict
retries reread the head and current clock. Older starts and regressing build
clocks cannot replace a newer publication. Different content with the same start
clock is rejected. Identical whole manifests can resume compatibility-copy repair.
Missing objects are distinct from failed transport/authorization, malformed JSON,
unknown schema, missing ETag and invalid clocks. The existing one-MiB manifest
reader bound is checked before writing.

This follows the [S3 conditional-write contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
It protects participating upgraded writers. An external writer that ignores
conditions is not fenced by this code; storage-policy enforcement is separate.

Legacy docs, index and instrument aliases are written after manifest selection,
with individual ETag conditions and build metadata. Each retry checks whether the
generation is still selected. A newer selected generation or newer alias prevents
an old retry from replacing it. These copies are not a multi-object transaction.
A reader requiring one generation must use immutable references from one pinned
manifest. Existing single-catalog legacy readers do not acquire that guarantee.

Failed immutable uploads cannot advance the manifest. Failed head writes raise
and leave retained immutable evidence; a transport failure can have an unknown
write outcome and is not reported as success. A failed compatibility copy occurs
after the generation is committed. Other copies are attempted, then the whole
native handler raises so a scheduled invocation cannot discard the failure in a
successful return body. A valid manifest is not rolled back. Configured retry
delivery and alerting are not certified here. An intentionally superseded build
returns an explicit unpublished result without changing aliases.

Only the native build function changes. Search, retrieval, warehouse recovery,
source acquisition, resources and seven original schedules remain unchanged.
Immutable retention has no automatic pruning. Actual storage growth, peak memory,
native latency, independent source replay, old compatibility consumers and normal
new-code publication remain unverified. Storage integrity grants no investment
or sizing authority.

The complete preceding handler and two whole overlapping invented builds are
retained in `tests/fixtures/symbol-directory/`. Native regressions cover races,
same-clock conflicts, complete legacy migration, interrupted uploads, malformed
heads, corrupt retained bytes, retry exhaustion and alias failure/recovery.
Read-only operation 6408 checks seven packaged sources, the exact release receipt,
resources and original schedules. It never invokes a producer or reads actual
current/private/account/consumer data.
