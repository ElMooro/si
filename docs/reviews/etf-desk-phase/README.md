# ETF desk daily phase migration — draft, not executed

The existing desk runs at 22:20 UTC and snapshots canonical holdings at startup.
Canonical holdings start at 22:45 UTC. Consequently the September 29 desk embeds
September 28 holdings even though the September 29 canonical publication exists.
At September 30 20:37 UTC, all 100 shared desk holdings were overdue while all 100
were current in the canonical root. The 16 separately collected desk funds were
current. The root itself had 294/300 complete, unexpired snapshots; six unavailable
funds are a separate issue and this migration makes no coverage promises for them.

## Proposed phase and dependency proof

Change only `justhodl-etf-global-desk-daily` from `cron(20 22 * * ? *)` to
`cron(5 23 * * ? *)`, once daily in UTC. This is the existing classic EventBridge
rule on the default bus, ARN
`arn:aws:events:us-east-1:857687956942:rule/justhodl-etf-global-desk-daily`.
It is NOT an EventBridge Scheduler schedule. Do not create a replacement schedule.

- Canonical `justhodl-etf-constituents-daily`: 22:45 UTC; actual configured timeout
  840 seconds (14 minutes), runtime python3.12, memory 3072 MB.
- Public canonical compilation: September 28 22:48:25 and September 29 22:48:24;
  latest public object modified September 29 22:49:36 UTC.
- 23:05 gives six minutes after 22:45 + the 14-minute runtime bound. 23:00 gives
  only one minute, so 23:05 is preferred. This is operational margin, not a guarantee
  against delayed delivery/retries; per-source expiry must still fail honestly.
- Desk timeout stays 900 seconds, memory 4096 MB. Expected completion by 23:20
  under that bound, with no new daily acquisition or collection scope.
- The 23:15 `justhodl-flow-lookthrough` reader uses canonical holdings directly;
  it does not consume the desk. It therefore imposes no 23:15 desk deadline.
- The identified scheduled desk consumer is `justhodl-massive-signals`, 22:00 UTC
  the following day. At that point the prior 22:45 canonical collection is about
  23h15 old, inside its 26-hour source-check bound. Pages read on demand.
- No Lambda code, resource size, universe, entitlement, risk or scoring changes.

## Actual read-only probe

Run **36774680876**, probe **6380**, finished September 30 20:44:29 UTC.
The attached report records exact enabled rules/targets and Active/Successful
Lambda metadata for desk, canonical holdings and lookthrough. Scheduler GetSchedule
was authorized and returned ResourceNotFoundException for the three exact default-
group names; classic bindings were then read. This is not an access-denial fallback.

The run is red because its third object-header request used the incorrect key
`data/holdings-lookthrough-research.json` and received 404. The actual reader root
is `data/flow-lookthrough.json`. Desk and canonical object headers and all binding/
runtime observations completed before that error. No second probe, producer invoke,
provider request or AWS write was performed. Do not interpret that 404 as a producer
failure. Current exact binding evidence is sufficient for this draft; execution
still rechecks it and refuses any change.

## Migration sequencing — independent approval still required

1. Independently review the exact operation and this evidence. Do not execute from
   this draft branch. The direct-ops workflow pushes its checked-out ancestry and
   report to main; running it here would bypass the PR review boundary.
2. After approval, merge with **both `[skip-deploy] [skip-ops]` in the merge commit
   subject**. Do not allow a config-triggered Lambda deploy before migration:
   `check_existing_schedule.py` correctly rejects a new config against the old rule.
3. Run only `staged/ops_6381_etf_desk_phase_migration.py` on **main**, before **22:00 UTC**.
   No broad manifest reconciler, fleet operation or Lambda deploy is needed. The
   helper refuses missing Scheduler read authorization, any new same-name Scheduler,
   changed upstream phase/runtime, target drift, unreviewed rule state, bad manifest,
   or an unprepared repository config/manifest.
4. The helper creates `data/ops/etf-desk-phase-v1/before.json` with IfNoneMatch and
   verifies it before configuration writes. The record contains the complete exact
   nonsecret rule/target state, previous selected manifest entry (possibly absent),
   manifest hash/ETag, timestamp and execution commit. No environment or provider
   material is recorded.
5. It CAS-updates only the named entry in the live S3 schedule manifest, preserving
   every unrelated entry/metadata. It never applies the manifest to the fleet.
   It then rechecks the rule and changes only phase/description with PutRule.
   No PutTargets, create/delete, IAM, Lambda config or invocation calls exist.
   Existing retry/dead-letter/input settings are absent; any newly added setting
   makes the exact-target guard refuse the operation rather than discard it.
   Classic rules use UTC and have no flexible-window setting to migrate.
6. Verify exact rule/target and manifest readback and the operation report. Retry
   safely if interrupted after archival or manifest publication. A completed repeat
   is read-only. EventBridge PutRule has no revision/CAS parameter: immediate rereads
   narrow but cannot eliminate a competing writer race; serialize this one resource.
7. Observe the next natural desk publication: canonical parent generation must be
   that evening's publication, original acquisition clocks retained, 100 shared
   holdings source checks refreshed if upstream succeeded. Do not invoke to accelerate
   acceptance. Source effective dates need not equal collection dates.

## Manifest and rollback

The desk entry is absent from both repository manifest snapshots. Each gains only
this exact existing rule at the new phase; pre-existing differences between the
snapshots are preserved. The live S3 manifest may contain no desk entry or the exact
old entry; any other value fails closed. No unrelated manifest bytes are archived
or printed. A rollback merges only the archived selected entry into the latest
manifest with CAS, preserving intervening changes to unrelated entries.

Rollback also needs a reviewed `[skip-deploy] [skip-ops]` repository commit restoring
the old desk config and each manifest's previous selected entry (read the immutable
record first; do not blindly remove an entry if the actual predecessor contained it).
Keep the rollback helper/staged entrypoint available. Then dispatch only the reviewed
`staged/ops_6382_etf_desk_phase_rollback.py` on main before 22:00 UTC. It restores archived phase/description and
entry, leaves the target unchanged, verifies readback, and is idempotent. Do not
reverse the whole PR in a way that removes the operation before it can run.

Partial failure intentionally leaves the archive and an explicit refusal rather
than attempting an unreviewed automatic rollback. The strict deploy guard continues
to block code deployment while repository phase and actual binding disagree.

## Cost and validation

Daily frequency, vendor requests, collection outputs and storage-version rate stay
unchanged: one existing collection per day, merely 45 minutes later. Migration has
one small immutable reversal-object PUT, one conditional manifest PUT and one rule
update; completed repeats perform reads only. Rollback has one manifest PUT and one
rule update. No new resource, subscription or paid feed. The brief schedule windows
owned by the parent are untouched.

The original configured phase fails the dependency-order test (39 minutes before
the producer runtime bound). The new phase passes. Offline tests cover forward/
rollback idempotence, absent and present predecessor manifest entries, input/retry/
DLQ drift, authorization denial, unexpected Scheduler, upstream drift, safe execution
window, repository readiness, archival failure, manifest failure and recovery,
readback failure and unrecognized reversal records. No cloud clients run in tests.


Validation completed: 21 offline migration tests; 28 ETF desk model/store/handler/
acceptance tests plus 12 subtests; 926 deployment static and 15 candidate shell
checks; selected Lambda config/source validators; preflight and secret scanner.
Preflight emits its generic classic PutRule warning. This operation updates the
proven existing rule only and refuses a missing/changed binding; it provisions no
rule or Scheduler. No frontend files change, so page QA is not a migration gate.

After approval and the skip-deploy merge, the exact forward dispatch is:
`gh workflow run run-ops-direct.yml --ref main -f script=staged/ops_6381_etf_desk_phase_migration.py`

The exact rollback dispatch, only after preparing its reviewed old-phase repository
state and before 22:00 UTC, is:
`gh workflow run run-ops-direct.yml --ref main -f script=staged/ops_6382_etf_desk_phase_rollback.py`

Neither dispatch has been run. Both entrypoints refuse execution off main or
outside GitHub Actions. Claim S-shopiz#edphase0930a reserves 6381/6382; fresh main
already reserves the unrelated 6390–6392 range, which is untouched.
