# ops 4074 — extension zip: origin vs edge vs install path

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-07-29T03:04:53+00:00  

## Data

| edge_busted | edge_plain | s3_zip |
|---|---|---|
| 1.7.8 | 1.7.8 | 1.7.8 |

## Log
## A. the REAL install chain (.bat → .ps1 → S3 zip)

- `03:04:53`   S3 zip                : 1.7.8  (19286 B)
- `03:04:53`   .bat stub             : 412 B
- `03:04:53`   .ps1 logic            : 3669 B
- `03:04:53`   → re-running the .bat already on his machine pulls v1.7.8; the stub pattern means no re-download was ever needed
## B. edge — plain vs cache-busted (which fault is binding?)

- `03:04:53`   plain fetch           : 1.7.8  (19286 B)
- `03:04:53`     cf-cache-status=MISS age=None etag="6a696d2d-4b56"
- `03:04:53`   cache-busted #1       : 1.7.8  (19286 B) cf=MISS age=None
## DIAGNOSIS

- `03:04:53`   Fully closed — edge and origin both serve v1.7.8.
## VERDICT

- `03:04:53`   ✓ S3 zip is v1.7.8
- `03:04:53`   ✓ .bat is the stub that fetches the .ps1
- `03:04:53`   ✓ .ps1 pulls the zip from S3 (not the edge decoy)
- `03:04:53`   ✓ .ps1 still caret-free (the v2 bug class)
- `03:04:53`   ✓ .ps1 expands to a stable folder and drives the Load-unpacked flow
- `03:04:53`   ✓ .ps1 writes the desktop shortcut (all desktop roots)
- `03:04:53`   ✓ edge origin serves v1.7.8 when cache is bypassed
- `03:04:53` ✅ PASS_ALL — install chain current, origin serving v1.7.8.
