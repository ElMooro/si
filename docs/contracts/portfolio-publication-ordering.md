# Private portfolio publication ordering

The portfolio-risk publisher's reproduced older-over-newer defect spans two
stores. Stage 446 installs the authenticated mirror protocol first. The native
S3 adapter is still the legacy publisher at this checkpoint; end-to-end ordering
and normal private publication are not accepted yet. No real private packet is
read or producer invoked for release acceptance.

## Mirror contract

Existing owner/service GET and HEAD aliases remain private with `no-store` and
`Vary: Authorization`. Only the existing service credential can reserve or PUT
at `/private-artifact?kind=portfolio-risk`. Other artifact kinds are unchanged.
The existing `WORKSPACE_COORDINATOR` SQLite binding hosts the dedicated object
`private-artifact:portfolio-risk:publication-v1`; there is no namespace migration,
new binding, schedule or automatic message.

1. Before snapshot/provider acquisition, POST to the same route with
   `&action=reserve` and `{"minimum_revision":0}`. A native adapter may supply a
   larger validated current S3 revision as the floor. The reply contains `ok`,
   protocol `portfolio-risk-publication.v1`, a positive safe-integer `revision`
   and an opaque `token`. Revisions are persisted atomically; abandoned numbers
   are harmless gaps. Acquisition start order, not completion time, orders jobs.
2. Add `publication` to the complete existing packet: `schema_version`,
   `revision`, `started_at` in UTC, the opaque quoted source S3 `source_etag` and
   `source_value_sha256`. The last value must equal
   `snapshot_binding.value_sha256` using `typed-json-binary64.v1`. An undated or
   incomplete snapshot can still have a valid complete value identity; missing
   observation time never becomes a fabricated timestamp.
3. PUT the exact complete UTF-8 body with `X-JH-Publication-Token` and
   `X-JH-Body-SHA256`. The digest covers the entire received byte sequence,
   including formatting and unknown fields. The token belongs only in the
   transport header, not the investment packet. A dedicated native sender is
   required so a shared JSON serializer cannot silently change the bytes.
4. A successful reply binds `protocol`, `revision`, `body_sha256` and `status`
   (`published` or `unchanged`). HTTP 200 alone is not acknowledgement. A lower
   revision or same-revision/different-body retry is HTTP 409. Unknown/pruned
   reservations are also 409; invalid proofs or JSON are 400. Storage failure,
   corrupt/missing persisted data and invalid legacy migration data are 503.
   Error text never echoes storage/provider exceptions or private body values.

The reservation journal holds the last 128 issued revisions. A candidate older
than that window is explicitly rejected, never assigned a new rank without a new
calculation. Counter overflow or invalid persisted counters fail closed.

## Complete data, transactions and migration

The request byte ceiling remains 20,000,000. Reads require EOF, an unambiguous
optional Content-Length, unencoded UTF-8 and completion within the read deadline.
Strict JSON refuses duplicate keys, malformed Unicode, nonfinite numbers,
trailing data and excessive nesting. This byte ceiling is not a promise that
every possible document fits Worker CPU/memory limits.

The full raw body is stored in 256 KiB byte chunks. A single SQLite transaction
writes the chunks and current manifest and removes superseded current chunks.
An explicit per-object queue also serializes awaits outside storage transactions.
GET, HEAD and identical retries verify every chunk, total length and SHA-256.
A durable migration marker prevents a missing current pointer from falling back
to old KV. No partial packet is served.

Before the first governed PUT, legacy complete packets remain readable and
service legacy writes remain compatible, explicitly `legacy_unverified`.
The transition retains the complete valid legacy KV packet in immutable chunks
inside the same private object. That transaction must finish before governed
state becomes visible; malformed/oversized predecessors block the transition.
The old KV key is then left alone. Late legacy PUTs through the new code are
rejected, and an in-flight old Worker writing KV cannot replace governed reads.

The object keeps the current packet and the complete first migration predecessor.
The coordinated native adapter must retain complete prior risk packets in the
existing private S3 archive before replacement. The already deployed immutable
replay-bundle retention and complete snapshot binding remain in force.

## Native follow-through and acceptance limits

Native follow-through must use a revision reserved before acquisition, bounded
complete current-packet reads, source-version checks and conditional S3 writes.
The S3 compare-and-swap and authenticated mirror are separate transactions.
Mirror failure after S3 success must remain observable and retryable with exactly
the same raw bytes; neither store's receipt proves a distributed commit. An
in-flight legacy Lambda can still write S3 during the upgrade's runtime window.

The native deployment must first check a current exact-source Worker receipt
covering this protocol. Release acceptance checks complete compiled Worker
bytes and unchanged visible settings/schedules, using control-plane GETs only.
It does not invoke private routes, read account/current packets, or claim secret
values or normal private publication are verified. The compiled current code is
also exercised in local workerd with synthetic bindings and SQLite; outbound
network is refused and telemetry disabled. Every archived predecessor remains
inert.

References: [Cloudflare transaction semantics](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/),
[SQLite object limits](https://developers.cloudflare.com/durable-objects/platform/limits/),
and [legacy KV write semantics](https://developers.cloudflare.com/kv/api/write-key-value-pairs/).
