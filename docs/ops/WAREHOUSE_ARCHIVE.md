# The warehouse archive and the bucket brakes (2026-10-20)

Khalid: "I have 500 GB on data.html that I want to keep and don't erase, but I also don't want anomalies to run my
budget to crazy amounts without stops and brakes." Three pieces, each doing one job; all three live and proven
on 2026-10-20 (ops 6570-6575, governor v1.5.0).

## What the live bucket holds (census ops 6570/6573, 2026-10-20)

`justhodl-dashboard-live`: **434 GB / 1.28 M current objects** over 239 prefixes - that is the warehouse (research
stores, indexes, archives, histories, private artifacts). Old copies (noncurrent versions) are what engines leave
behind when they overwrite a file: the October loop left 573,713 copies of one audit key under `system-events/`
(1.74 TB) and hundreds of thousands under `data/`; daily-rewritten outputs outside `data/` (equity-research/NVDA.json,
etf-flows/daily.json, portfolio/risk.json, root student-state.json...) had accumulated 80-46,000 copies each because
no rule covered them. Nothing in the fleet reads an old version (the factory pins current versions of immutable
inputs); explicit history lives in its own prefixes as current objects.

## Piece 1 - "never erased": the archive (ops 6574/6575)

* Bucket `justhodl-warehouse-archive-857687956942` (us-east-1): versioning on, SSE-S3, all public access blocked,
  bucket-owner enforced; lifecycle keeps the 3 newest old copies per file and aborts stray uploads after 7 days.
* **Seal**: the bucket policy denies `s3:DeleteObject`, `s3:DeleteObjectVersion`, `s3:ReplicateDelete`,
  `s3:PutBucketVersioning`, `s3:PutLifecycleConfiguration`, `s3:PutBucketPolicy`, `s3:DeleteBucketPolicy`,
  `s3:DeleteBucket`, `s3:PutBucketPublicAccessBlock` to **every principal except the account root user**. No Lambda
  role, no runner, no replication role, no ops script can delete from it or weaken it. Only Khalid, signed in as
  root in the console, can - that is the point. Proof: the runner's delete attempt on the canary copy returned
  AccessDenied.
* **Replication** from the live bucket: one rule, the whole bucket (prefix ''), destination Standard-IA, delete
  markers NOT replicated - a deletion in the live bucket never reaches the archive. Role
  `justhodl-s3-replication-role` (least privilege: read-for-replication on the source, ReplicateObject/Tags on the
  archive). Proof: canary `_health/replication-canary.json` arrived in 30 s.
* **Backfill** of the 1.28 M objects that existed before the rule: S3 Batch Replication. The first job
  (`de1a80a2-...`, S3-generated manifest) was cancelled while still preparing (ops 6577): AWS replicates *every
  eligible version* from a generated manifest, which would have carried the loop's 573k old copies into Standard-IA
  at 30 days' minimum charge each. **Done 2026-10-21 (ops 6583):** the live bucket was listed by the runner itself
  (`ListObjectVersions` per prefix without a delimiter, `HeadObject` for the 1,243 top-level files - the delimiter
  walk is the pagination trap of ops 6569/6570), 1,282,031 current objects (18,765 pre-versioning "null" version
  ids, accepted by the job) written as a CSV manifest to the private bucket, job `523b902a-5e7f-4ade-bf38-8c92eddcc947`
  created from it; 25 minutes later (ops 6576, 02:47 UTC) 1,229,048 of 1,282,030 tasks had succeeded, 0 failed, and
  the archive held 1,281,180 current objects / 433.1 GB = the live census. ~$14 once.
* Cost: ~434 GB in Standard-IA (128 KB minimum per object) ≈ $6-7/month + ~$3-5/month of replication PUTs for the
  daily writes ≈ **$10-12/month**.
* Restoring from the archive: copy the object (or prefix) back with `aws s3 cp s3://justhodl-warehouse-archive-.../<key>
  s3://justhodl-dashboard-live/<key>` from the runner (an ops script); every file is there under the same key, in
  Standard-IA (retrieval $0.01/GB).

## Piece 2 - old copies capped by count, not by age (ops 6571)

Bucket-wide rule `ops6541-bound-noncurrent-versions`: **keep the 10 newest old copies of every file; expire the rest
once they are a day old.** The 1-day age-only expiries on `data/`, `data/providers/` and `system-events/` were
retired (their delete-marker sweeps stay). Effects: a storm that rewrites one file 10,000 times keeps 10 copies; every
file keeps its last 10 states indefinitely - a deleted file's last copy has nothing newer and is kept, so the undo
window went from one day to "always". **Current objects are never touched by any lifecycle rule** (the only
current-object expiry is the intentional 30-day one on `screener/snapshots/`). Lifecycle acts asynchronously: the
573k copies of the audit key and the storm copies under `data/` are the visible proof once the daily S3 Inventory
(`s3://justhodl-ops-private-.../inventory/`, all versions, from ops 6569) shows them down to 10.

## Piece 3 - brakes for the bucket itself (governor v1.5.0, ops 6572)

`docs/ops/SPEND_GOVERNOR.md` section v1.5.0: once a day the governor reads the free daily S3 metrics and acts on
growth - warn (>= 150 GB / 20% / 500k objects: an issue), trip (>= 500 GB / 50% / 2 M objects, or two warn days: the
hottest writer held under CAP), second strike (versioning suspended, next writers held; `storage_restore` re-enables),
plus yesterday's S3 cost line (>= $10/day reported). Proven live: selftest ok, dry run read the bucket end to end.

## Net

Current data is never touched by any rule; the last 10 states of every file survive; a storm costs at most 10
copies plus ~2 hours of Lambda; the warehouse has a sealed off-bucket copy nobody but the root user can delete.
