# Native snapshot ordering and recovery

The snapshot producer reserves an acquisition revision before research reads,
book reads, provider collection or watchlist synchronization. This expresses
attempt order, not market observation time or investment confidence. Its whole
packet includes the issued revision and UTC acquisition start. Validation-only
collection skips reservation, synchronization and every publication write.

Before acquisition and again before publication, the producer reads the exact
bucket policy, versioning setting and complete lifecycle rules. It requires
three narrowly scoped conditional-write and retention denies, the existing
private read boundary for current and history, enabled versioning, and no
potentially matching expiration rule. Missing permissions or incomplete settings
stop collection/publication. The existing shared role's captured S3 policy
contains the needed actions; no IAM grant is added. A configuration capture does
not prove effective runtime authorization.

Current S3 bodies are read to EOF within explicit byte/time bounds. The response
must supply an opaque quoted ETag, exact length, strict JSON, compatible whole
typed values, and valid publication metadata if ordered. Missing objects are
recognized only by the specific `NoSuchKey` service error. Access errors, partial
bodies, missing recovery tokens and corrupted documents do not become defaults.

Before replacing current, the producer retains the entire prior body under
`history/archive/feed/portfolio/snapshot.json/<full-sha256>.json`, using
`If-None-Match: *`. It reads the complete retained body back and compares exact
bytes. A conflicting archive or unconfirmed retention stops the current write.
The current write uses `If-Match` against the observed ETag or `If-None-Match: *`
when absent. Four conditional conflicts are the maximum. Each retry re-reads the
current body and retains its complete predecessor. A newer revision wins; an
equal revision with different bytes or token fails.

The body and reservation token are committed together in S3; the token is private
S3 metadata and is absent from the JSON. S3 is written before the mirror. Before
and after mirror delivery, the producer checks that current S3 still contains
the exact candidate and token. The authenticated mirror acknowledgement must
match protocol, revision, whole digest, wire length and typed identity size.
Only this observed success produces the ordinary success result. A superseded
attempt returns a conflict result. Unconfirmed mirror delivery raises an actual
Lambda invocation error rather than an HTTP-shaped 503 that an asynchronous
schedule would silently treat as success.

The next existing invocation re-delivers the exact stored body/token before
reserving new work. An acknowledged retry does not change its observations or
give them a new rank. The Worker retains 128 reservations. If a reservation has
expired or the mirror already has a newer revision, the old body is never
relabeled; a new reservation may collect new observations. Transport failure
during recovery stops before new acquisition. SDK write retries are disabled;
ambiguous S3 acknowledgements resolve only through complete readback.

| Observed state | Behavior |
| --- | --- |
| Required storage protection missing | Stop before acquisition; retain last good body |
| Prior body cannot be retained exactly | Do not replace current or publish candidate |
| Newer S3 revision already present | Supersede this attempt |
| S3 write unconfirmed | Do not publish mirror; report failure |
| S3 committed, mirror unconfirmed | Preserve exact recovery state and raise invocation error |
| Mirror acknowledged, S3 changed | Report superseded rather than current success |
| Exact retry | Read/verify and acknowledge unchanged bytes |

Cutover ships the new fail-closed producer first. Read-only operation 6373 verifies
its complete six-file effective source closure, receipt commit, promoted alias,
unchanged resources/schedules, and the already accepted Worker. Operation 6374
repeats that prerequisite before adding the three storage denies. All prior 31
policy statements and other fields are preserved. Two pre-write reads refuse
known drift; one policy write is resolved by whole readbacks, without blind
retry or rollback. The same native guard then checks the resulting settings.
The policy is 20,366 bytes, below S3's 20,480-byte limit, with 114 bytes remaining.

The reviewed path uses single `PutObject`. The exact current/archive scopes do
not permit unconditional uploads, multipart setup, copy or replication writes;
delete and version-delete are denied. This follows the documented conditional
request keys and their multipart/copy limitations. References:
[S3 conditional-write enforcement](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
and [conditional write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

S3 and the Worker are not a distributed transaction. Policy replacement has no
compare-and-swap primitive, and configuration reads do not lock future policy,
versioning or lifecycle changes. This change does not claim instantaneous policy
propagation or cancellation of previously admitted old-writer requests. Concurrent
unreviewed writers or a later configuration change remain outside that guarantee.
Current revisions can legitimately move again after an acknowledged delivery.

Acceptance runs current code with complete invented bodies, store failures and
interleaved schedules, plus the current Worker on those exact native bodies.
Retained predecessor source is inert. Runtime acceptance reads release receipts,
complete deployed packages, control-plane settings and schedules only. It never
invokes a live producer, accelerates its schedule, reads an actual account or
writes a private artifact for testing. Actual private publication, total native
memory under maximum loads, reconciled account NAV, source qualification and
investment authority remain separate requirements.
