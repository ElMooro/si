# ops 4251 — review closure

**Status:** success  
**Duration:** 482.6s  
**Finished:** 2026-08-01T19:47:52+00:00  

## Data

| copied | day | dest | detail | errors | function | key | kind | last_run | rate_per_s | section | source | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | Fri |  |  |  | justhodl-polygon-options-flow |  |  | 2026-07-31T18:39 |  | false_positive |  | weekend-idle |
|  | Fri |  |  |  | justhodl-trade-tickets |  |  | 2026-07-31T18:39 |  | false_positive |  | weekend-idle |
|  |  |  | 2 targets, 1 unique — this rule fires its function more than once per tick |  |  | signal-scorecard-daily | DUPLICATE_TARGET |  |  | drift |  |  |
| 106333 |  | 111339 |  | 0 |  |  |  |  | 239.7 | crr | 111339 |  |

## Log
## F1. The 'silenced' engines — prove the false positive

- `19:39:50` ✅   justhodl-polygon-options-flow          last ran Fri 18:39 UTC — Friday inside market hours; Saturday silence is the SCHEDULE, not a defect
- `19:39:50` ✅   justhodl-trade-tickets                 last ran Fri 18:39 UTC — Friday inside market hours; Saturday silence is the SCHEDULE, not a defect
- `19:39:50` sweep-parser gap recorded: cadence_hours() ignores the day-of-week field; weekend runs of the sweep will over-flag MON-FRI engines until it is day-aware.
## F2. The drift of one

- `19:39:51` reconciler mode=enforce-duplicates drift_count=1
- `19:39:51` ⚠   DUPLICATE_TARGET signal-scorecard-daily                       2 targets, 1 unique — this rule fires its function more than once per tick
- `19:40:05` ✅ reconciler after fix: drift = 0
## F3. CRR backfill to completion (threaded)

- `19:40:28` source=111339 dest=5006 missing=106333
- `19:41:06`    … 10000 copied (265/s)
- `19:41:42`    … 20000 copied (271/s)
- `19:42:18`    … 30000 copied (274/s)
- `19:42:54`    … 40000 copied (275/s)
- `19:43:30`    … 50000 copied (276/s)
- `19:44:07`    … 60000 copied (275/s)
- `19:44:45`    … 70000 copied (273/s)
- `19:45:23`    … 80000 copied (271/s)
- `19:46:16`    … 90000 copied (259/s)
- `19:46:58`    … 100000 copied (257/s)
- `19:47:52` copied=106333 errors=0 in 444s | destination=111339 source=111339
- `19:47:52` ✅ us-west-2 holds a COMPLETE copy (±in-flight daily writes, which forward replication covers)
## F4. DR freshness — against the REAL key

- `19:47:52` ✅ data/dr-snapshot-latest.json is 13.6h old
- `19:47:52` ✅ signal-scorecard artifact is 0.5h old (bound 12h)
- `19:47:52` follow-up recorded: the DR engine writes dated per-day code trees — the bucket grows ~fleet-size daily. Cheap, but a dedupe design pass belongs in a future session.
## RESULT

- `19:47:52` ✅ OPS 4251 PASS — review findings closed with evidence
