# ops 4030 — cap lifted + upload proof

**Status:** failure  
**Duration:** 745.5s  
**Finished:** 2026-07-29T00:32:22+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | diag | ingest_invocations_90min | modified | n_watchlists |
|---|---|---|---|---|
| 3.2 |  |  | 2026-07-29T00:16:44+00:00 | 0 |
|  |  | 8 |  |  |
|  | {"started": 1785281515557, "done": 2142, "total": 10319, "ss_ok": 0, "ss_err": 2142, "html_ok": 2122, "html_err": 20, "matched": 1199, "first_err": "ss:TypeError: Failed to fetch"} |  |  |  |

## Log
## A. proof his SYNC landed

## B. lift the cap (491 lists must fit)

## C. birth watch — next sync is <=15 min out

- `00:32:22` ✅   BORN — 1000 sources at 2026-07-29T00:31:43.033861+00:00
- `00:32:22`     CRYPTOCAP:TOTAL3: django_model
- `00:32:22`     TVC:NI225: django_model
- `00:32:22`     TVC:FTMIB: django_model
- `00:32:22`     TVC:HSI: django_model
- `00:32:22`     TVC:NZ50G: django_model
- `00:32:22`     SIX:SMI: django_model
- `00:32:22`     SZSE:399001: django_model
- `00:32:22`     OMXCOP:MAERSK_B: django_model
- `00:32:22`     EUREX:FMOG1!: django_model
- `00:32:22`     OANDA:AU200AUD: django_model
- `00:32:22` ✅   watchlists artifact refreshed TODAY
- `00:32:22` ✗   ~200 lists landed (pre-fix cap)
- `00:32:22` ✅   ingest heard from the browser
- `00:32:22` ✅   cap 1200 settled in the deployed zip
- `00:32:22` ✅   tv-sources.json BORN with content
- `00:32:22` ✗ FAILED: ['~200 lists landed (pre-fix cap)']
