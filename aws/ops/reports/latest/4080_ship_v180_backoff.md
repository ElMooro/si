# ops 4080 — extension v1.8.0: AIMD backoff over the walled symbols

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-07-29T05:51:08+00:00  

## Data

| bat_bytes | bytes | new_version | old_version | ps1_bytes | ps1_edge |
|---|---|---|---|---|---|
|  | 21055 | 1.8.0 | 1.7.9 |  |  |
|  |  |  |  | 5608 | 5608 |
| 412 |  |  |  |  |  |

## Log
## A. rebuild + upload the extension zip

- `05:51:06`   previous S3 zip: v1.7.9 (19638 B)
## B. upload installer v4

## C. stub integrity

## D. repo/Pages zip parity

- `05:51:08`   repo zip version: 1.8.0
## VERDICT

- `05:51:08`   ✓ zip carries v1.8.0
- `05:51:08`   ✓ auto-start guard is version-stamped (update-day trap)
- `05:51:08`   ✓ no bare-date value is written to jh_auto_day
- `05:51:08`   ✓ v1.7.8 priority walk still present
- `05:51:08`   ✓ symsearch canary still present
- `05:51:08`   ✓ fixed 240ms step is GONE
- `05:51:08`   ✓ AIMD throttle wired into the scanner route
- `05:51:08`   ✓ circuit breaker + pause guard present
- `05:51:08`   ✓ backoff telemetry ships
- `05:51:08`   ✓ ECONOMICS payload probe present
- `05:51:08`   ✓ rate telemetry still present
- `05:51:08`   ✓ harvester + autonomy intact
- `05:51:08`   ✓ served .ps1 is byte-exact
- `05:51:08`   ✓ caret-free (the v2 cmd-escaping bug class)
- `05:51:08`   ✓ no longer nukes the loaded folder
- `05:51:08`   ✓ updates IN PLACE via a staging copy
- `05:51:08`   ✓ detects update vs fresh install
- `05:51:08`   ✓ still pulls the zip from S3 + writes the shortcut
- `05:51:08`   ✓ the .bat Khalid already has still fetches this .ps1
- `05:51:08`   ✓ repo zip matches S3 (no stale Pages decoy)
- `05:51:08` ✅ PASS_ALL — v1.8.0 live. Reload and the slow pass retries the 9,568 walled symbols under AIMD backoff; the next op reads wall_events/recoveries to say whether the wall lifts, and econ_probe settles the ECONOMICS payload question either way.
