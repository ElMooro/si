# ops 4713 — finish macro-33 + probe TE native PMI historical depth

**Status:** failure  
**Duration:** 113.3s  
**Finished:** 2026-08-15T19:05:22+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Drive the 33 core-macro series to completion

- `19:03:29`   before: done=214 catalog=225
- `19:05:16`   round 1: {"ok": true, "pulled": 0, "failed": 11, "row_cap_hits": 0, "done": 225, "catalog": 225, "status": "COMPLETE-maintaining", "mean_agree_pct": 100.0}
- `19:05:16` ✅   full catalog (225) converged
## 2. Probe TE's NATIVE indicator-history endpoint for real PMI depth (not the /fred/ mirror)

- `19:05:16`   united states    HTTP 403
- `19:05:17`   china            HTTP 403
- `19:05:18`   germany          HTTP 403
- `19:05:18`   japan            HTTP 403
- `19:05:19`   united kingdom   HTTP 403
## 3. Also check services PMI + a non-PMI global indicator, for scope

- `19:05:20`   united states    / services pmi         HTTP Error 403: Forbidden
- `19:05:20`   germany          / inflation rate       HTTP Error 403: Forbidden
- `19:05:21`   china            / gdp growth rate      HTTP Error 403: Forbidden
## verdict

- `19:05:22` ✗ no country returned real PMI historical depth — TE's native indicator endpoint may need a different category name for PMI, or genuinely doesn't carry it
