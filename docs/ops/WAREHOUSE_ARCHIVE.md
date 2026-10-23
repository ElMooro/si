# The warehouse archive and the bucket brakes (2026-10-20)

Khalid: "I have 500 GB on data.html that I want to keep and don't erase, but I also don't want anomalies to run my
budget to crazy amounts without stops and brakes." Three pieces, each doing one job; all three live and proven
on 2026-10-20 (ops 6570-6575, governor v1.5.0).

## What the live bucket holds (census ops 6570/6573, 2026-10-20; CORRECTED by the S3 Inventory, ops 6610/6611, 2026-10-22)

**Correction (2026-10-22):** the census listing was truncated on `data/` (its own report said so) - the bucket's own
S3 Inventory of 2026-10-21 01:00 UTC counts **3,381,373 current objects / 1,023 GB**: `data/providers/` 1,137,344
objects / 294 GB (the series page store), `data/warm/` 778,619 / 207 GB (the warm bank), `audit-private/` 497,429 /
78 GB, `data/evidence/` 171,396, `data/etf-holdings-research/` 107,081, `data/option-flow-research/` 94,401 / 43 GB,
`data/series-cache/` 67,396, `data/fred-levels/` 60,098, `data/holdings-research/` 23,274 / 75 GB, `data/index/` 61 GB
... The figures below that say 434 GB / 1.28 M were the truncated view; the billed storage (Cost Explorer
TimedStorage-ByteHrs 34 GB-month a day ≈ 1.03 TB; CloudWatch BucketSizeBytes 1,037 GB on 10-20) agrees with the
inventory's current objects.

`justhodl-dashboard-live` (census view): **434 GB / 1.28 M current objects** over 239 prefixes - that is the warehouse (research
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
  the archive held 1,281,180 current objects / 433.1 GB = the live census. ~$14 once. **Job 523b902a finished
  2026-10-22: 1,282,030 / 1,282,030 succeeded, 0 failed (ops 6576, 15:50 UTC).**
* **But the listing was incomplete (found 2026-10-22, ops 6611):** ops 6583 discovered the `data/` sub-prefixes with
  one unpaginated `ListObjectsV2(Prefix="data/", Delimiter="/")` page, so every sub-prefix after about `data/pre...`
  was never walked - `data/providers/` (1.14 M objects, 294 GB), `data/warm/` (779 k, 207 GB), `data/series-cache/`,
  `data/statement-research/`, `data/price-observations/`, `data/symdir/`, `data/research-*`, `data/raw/`,
  `data/vintage-research/`, `data/xbrl-raw/` ... Measured against the inventory: of 3,073 sampled current objects
  only 1,176 (38%) are in the archive; `data/providers/` 1,708 of 1,137,344, `data/warm/` 7,201 of 778,619,
  `data/series-cache/` 604 of 67,396; the prefixes the listing did reach are complete (`data/archive/` 46,562 of
  46,551, `data/fundgraph/` 38,931 of 37,439 - the surplus is live replication since). **The archive holds ~1.33 M of
  3.38 M current objects, ~439 of ~1,023 GB.**
* **Backfill #2 (ops 6612, written 2026-10-22, dispatched only on Khalid's go):** manifest built from the inventory
  itself (one row per current object, keys already in the archive removed by a paginated listing of the archive),
  one S3 Batch Replication job for the ~2.05 M missing objects / ~584 GB. Cost once ≈ $21 of Standard-IA PUTs +
  $2.3 Batch Operations; the archive then holds ~1,023 GB of Standard-IA ≈ $13/month (the estimate below, $10-12/month,
  becomes **≈ $18-20/month**). ops 6576 tracks it. **Done 2026-10-22: job `646ccb69` Complete, 2,086,159 / 2,086,159
  succeeded, 0 failed, in about eight hours (dispatched 16:13 UTC on Khalid's "continue"); at 00:35 UTC on Oct 23 the
  archive held 3,456,698 current objects / 1,031.8 GB (102% of the inventory census - the surplus is live replication
  of the day's writes; `data/providers/` 1,137,345 = the inventory exactly, `data/warm/` 778,792), and a 3,109-key
  sample of current objects was found 100% present (ops 6611). The archive is a complete copy.** Trap recorded twice:
  `ListObjectsV2(Prefix="data/", Delimiter="/")` must be paginated - the unpaginated first page truncated both the
  ops 6583 manifest and ops 6576's own count (fixed 2026-10-23).
* Cost (design estimate): ~434 GB in Standard-IA (128 KB minimum per object) ≈ $6-7/month + ~$3-5/month of replication PUTs for the
  daily writes ≈ **$10-12/month**. **Measured 2026-10-23 (ops 6622, Cost Explorer):** the fleet writes ~84 k objects a
  day (`Requests-Tier1` PutObject 83,556 on Oct 22) and every one is replicated as a Standard-IA PUT ($0.01 per 1,000)
  = **$0.84/day ≈ $25/month** of replication writes; storage at the full 1,032 GB ≈ $13-15/month; replaced copies are
  billed for Standard-IA's 30-day minimum (~$5/month, estimate). The never-erase copy costs **~$45/month** in all.
  The two backfills cost $14.35 (Oct 21) and $25.09 (Oct 22) once. Options are in docs/audit/2026-10-23/aws-bill-oct22.md.
* Restoring from the archive: copy the object (or prefix) back with `aws s3 cp s3://justhodl-warehouse-archive-.../<key>
  s3://justhodl-dashboard-live/<key>` from the runner (an ops script); every file is there under the same key, in
  Standard-IA (retrieval $0.01/GB).

## Narrowing the archive - option B (2026-10-23): measured, then not possible with a replication rule

Khalid, 2026-10-23: "2archive lets do B" - protect only data that can't be re-downloaded, like the provider history on
data.html, and skip files the engines rebuild every day; measure the saving per folder before changing anything.

**Measured (ops 6631 per folder, ops 6632 per exact prefix; one full post-thaw day, every write in the inventory).**
The archive's copying costs **$29.00/month at today's rate** (74,455 copies a day: $22.34 of Standard-IA PUTs, $3.42
of 30-day minimums on replaced copies, +258.8 GB/month of billable growth), rising to **$46.79/month averaged over a
year** as the growth piles up - plus ~$16/month for the ~1,280 GB it already holds. Scope B, written down with the
evidence for every folder in `config/archive-scope.json` (22 prefixes: 2 re-downloadable provider captures, 11 research
packets compiled from originals that stay archived, 6 engine caches/views/indexes, 3 data.html catalog-page families),
would stop 41,275 copies a day: **-$15.18/month now, -$23.04/month averaged over a year**. Biggest items: the daily FMP
financial-statement capture `audit-private/20260909-originals/financial-statement-research/` $5.39, the Massive forex
bars `.../fx-research/` $2.20, `data/etf-holdings-research/` $1.63, `equity-research/` $0.71, `data/fundgraph/cache/`
$0.67 and `hits/` $0.65, the GDELT and FRED catalog pages $0.57 and $0.39. Kept on purpose: the warehouse (data/warm,
provider data under data/providers), every point-in-time original (option chains, ETF constituents, market internals,
futures catalogs), data/evidence, every history/ledger/vintage (data/fundgraph/vintage/ is a self-built analyst-estimates
ledger FMP cannot re-serve), all top-level data/*.json files. The census also corrected the first draft:
`data/risk-regime-research/sources/` holds the engine's own FRED + Massive option/FX originals and
`data/series-cache/*-source/` retains original public responses (IMF versioned) - both stay archived.

**The mechanism does not exist (ops 6633, ops 6636).** S3 replication filters can only include (prefix and/or tags);
there is no exclude. The usual workaround - a Disabled rule with a higher priority than the whole-bucket rule, on the
folder to skip - was tested on a canary prefix (`_health/archive-scope-excluded/`, rule `archive-scope-self-test`,
priority 100) before any real folder relied on it: every excluded canary was still stamped PENDING (and copied), for
4.3 minutes in ops 6633 (9 pairs) and 20 minutes in ops 6636 (38 pairs, one every 30 s). S3 skips a disabled rule
when it picks the rule that applies, so the whole-bucket rule still matches. Both runs put the backed-up configuration back exactly (backups under
`s3://justhodl-ops-private-.../backups/replication/`); the archive copies everything as before. The canaries are ~200-byte
files; their archive copies stay (no delete path but root).

**What is left (Khalid decides):**
* **Intelligent-Tiering for new copies (ops 6637, staged, recommended):** same one rule, same seal, every write still
  copied; only the storage class of new copies changes. Standard-IA bills each copy as at least 128 KB for at least 30
  days at $0.01 per 1,000; Intelligent-Tiering bills actual size, has no minimum duration, $0.005 per 1,000, and moves
  files >= 128 KB to $0.0125/GB after 30 days and $0.004/GB after 90 ($0.0025 per 1,000 such files a month to monitor);
  no retrieval fee on restore. Estimate (ops 6632): **$29.00 -> $16.44 first month, $46.79 -> $27.36 averaged over a
  year.** Existing copies stay Standard-IA.
* **Scope B the hard way:** replace the one rule with an include list - a few hundred prefix rules (every folder and
  top-level file under data/, audit-private/20260909-originals/ and the root except the 22 prefixes; S3 allows 1,000) -
  plus a scheduled job that adds a rule for every new folder or file. New data is unprotected until the job catches it,
  and a broken job silently stops archiving new data. -$15.18/month on Standard-IA, or -$8.10 more on top of
  Intelligent-Tiering ($16.44 -> $8.34). Not recommended.
* ops 6634 (apply) and ops 6635 (next-day check of every write via the inventory's ReplicationStatus field) were
  written for the disabled-rule design and stay unused: 6634 refuses without the self-test rule a passing canary leaves
  behind. An include-list implementation would replace 6634; 6635's inventory check carries over as is.

## Piece 2 - old copies capped by count, not by age (ops 6571)

Bucket-wide rule `ops6541-bound-noncurrent-versions`: **keep the 10 newest old copies of every file; expire the rest
once they are a day old.** The 1-day age-only expiries on `data/`, `data/providers/` and `system-events/` were
retired (their delete-marker sweeps stay). Effects: a storm that rewrites one file 10,000 times keeps 10 copies; every
file keeps its last 10 states indefinitely - a deleted file's last copy has nothing newer and is kept, so the undo
window went from one day to "always". **Current objects are never touched by any lifecycle rule** (the only
current-object expiry is the intentional 30-day one on `screener/snapshots/`). Lifecycle acts asynchronously: the
573k copies of the audit key and the storm copies under `data/` are the visible proof once the daily S3 Inventory
(`s3://justhodl-ops-private-.../inventory/`, all versions, from ops 6569) shows them down to 10.

**What the inventory showed on 2026-10-22 (ops 6610, inventory of 2026-10-21 01:00 UTC):** 8,419,588 old copies
(1,907 GB) on 152,135 keys, 3,688 keys above the 10-copy cap, 7,993,648 copies above it in total -
`data/miss-calibrator-proposals.json` 7,197,422 copies, `system-events/audit/2026-10-12.jsonl` 573,495 copies (1.74 TB),
then `student-state.json` 46,005, `data/warm/worldbank-full/_state/state.json` 32,979, `crypto-intel.json` 11,144,
`data/a2a/nudge-ledger.json` 9,994, `factory/salon/board.json` and `factory/scoreboard.json` 9,338 each. **No manual
purge is needed:** the storm copies under `data/` and `system-events/` were already expired by the 1-day age rules that
covered those prefixes until ops 6571 (the bucket's billed storage is ~1.03 TB = the current objects; CloudWatch counts
3.8 M objects, the inventory 11.8 M rows - expired versions are no longer billed but stay visible in listings until S3
removes them asynchronously, which for millions of versions takes days). The keys outside those prefixes
(student-state.json, crypto-intel.json, factory/, signals/, portfolio/ ...) are the count rule's job and will show at
10 in a later inventory; the daily inventory (delivered 14-20 h after its 01:00 UTC stamp) is the check.

Trend, inventory 2026-10-22 01:00 UTC (read 2026-10-23 15:33, ops 6610) against 2026-10-21: old copies 8,419,588 ->
8,040,791 (-379 k in a day), keys above the 10-copy cap 3,688 -> 2,271, copies above the cap 7,993,648 -> 7,575,589;
miss-calibrator-proposals.json 7,197,422 -> 6,808,388 (-5.4 %/day: S3 drains the expired pile asynchronously, roughly
two to three weeks at this pace), the audit key 573,495 -> 571,385, student-state.json 46,005 -> 45,614, crypto-intel
11,144 -> 11,126, factory/scoreboard 9,338 -> 9,288, portfolio/risk 2,790 -> 2,724 - the count rule is trimming, slowly,
and the 11-100-copy bucket fell 3,609 -> 2,198 keys. Current objects 3,420,242 / 1,028 GB (+39 k: the day's writes).
Nothing to do but read the next inventories; none of it is billed.

## Piece 3 - brakes for the bucket itself (governor v1.5.0, ops 6572)

`docs/ops/SPEND_GOVERNOR.md` section v1.5.0: once a day the governor reads the free daily S3 metrics and acts on
growth - warn (>= 150 GB / 20% / 500k objects: an issue), trip (>= 500 GB / 50% / 2 M objects, or two warn days: the
hottest writer held under CAP), second strike (versioning suspended, next writers held; `storage_restore` re-enables),
plus yesterday's S3 cost line (>= $10/day reported). Proven live: selftest ok, dry run read the bucket end to end.

## Net

Current data is never touched by any rule; the last 10 states of every file survive; a storm costs at most 10
copies plus ~2 hours of Lambda; the warehouse has a sealed off-bucket copy nobody but the root user can delete.
