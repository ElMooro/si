# ops 5420 -- stock-buying v1.5.2 restore gate

**Status:** failure  
**Duration:** 31.6s  
**Finished:** 2026-09-11T20:13:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| fmp_key | gated | modules | n_scored | source_bytes | step | waited_s | with_beats | with_fmpq | zip_bytes |
|---|---|---|---|---|---|---|---|---|---|
|  |  | 53 |  | 40537 | code |  |  |  | 232529 |
| True | 66 |  | 614 |  | run | 30 | 0 | 0 |  |

## Log
## 1. deployed code

- `20:12:59` state=Active last_update=Successful last_modified=2026-09-11T20:09:48.000+0000 timeout=300s memory=512MB runtime=python3.12
- `20:12:59` ✅ zip has lambda_function.py: True
- `20:12:59` ✅ source is the full engine (>35KB): True
- `20:12:59` ✅ v1.5.2 marker: True
- `20:12:59` ✅ /stable/ base URL: True
- `20:12:59` ✅ no /api/v3 base URL: True
- `20:12:59` ✅ eps_beats present: True
- `20:12:59` ✅ managed_secret bundled: True
- `20:12:59` ✅ no PLACEHOLDER / keep-alive stub: True
## 2. live run

- `20:12:59` baseline data/stock-buying.json: generated_at=2026-09-11T02:30:08+00:00 n_scored=614 fmp_key=True bytes=619881
- `20:12:59` invoked (Event) status=202 at 2026-09-11T20:12:59+00:00
- `20:13:29` regenerated after 30s: generated_at=2026-09-11T20:13:24+00:00 n_universe=492 n_scored=614 fmp_key=True top=300
- `20:13:29` rows below_sma (FMP path taken)=66  rows with revisions_beats (/stable/earnings parsed)=0  rows with accel/_fmpq (/stable/income-statement parsed)=0
- `20:13:29`   GPN    tier=SCREENED  score=95.3 beats=None accel=None
- `20:13:29`   DELL   tier=SCREENED  score=95.3 beats=None accel=None
- `20:13:29`   NEM    tier=SCREENED  score=95.3 beats=None accel=None
- `20:13:29`   MPC    tier=SCREENED  score=95.3 beats=None accel=None
- `20:13:29`   ROP    tier=WATCH     score=95.3 beats=None accel=None
## 3. log scan since deploy

- `20:13:30` ✅ no Traceback / [ERROR] / timeout lines since 2026-09-11T20:09:39+00:00
## verdict

- `20:13:30` ✗ 66 rows took the FMP path but none carry revisions_beats -- /stable/earnings not parsing
