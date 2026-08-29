## P0 baseline, then run the catalog

**Status:** failure  
**Duration:** 792.3s  
**Finished:** 2026-08-29T19:35:41+00:00  

## Error

```
SystemExit: 1
```

## Log
- `19:22:29`   BEFORE eurostat: {"series": null, "n_keys": 8191, "total_mb": 8892.72, "coverage_pct": 100.0, "datasets": 8191}
- `19:22:29`   BEFORE hub totals: keys=776661 gb=214.86 datasets=800961
- `19:22:29`   new catalog code present (LastModified=2026-08-29T19:22:24) mem=1024 timeout=600s
- `19:35:40`   invoke err Connection was closed before we received a valid response from endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-provider-catalog/invocations".
## P1 the eurostat card now

- `19:35:40`   series.count = 0  (counted=None, ids=0)
- `19:35:40`   n_keys       = 8,191   total_mb = 8892.72
- `19:35:40`   derived      = null
- `19:35:40`   note         = None
- `19:35:40`   datasets=8191 datasets_target=8152 coverage_pct=100.0
- `19:35:40`   *** series count did not pick up the manifest ***
- `19:35:40`   *** counted prefix produced nothing ***
## P2 regression -- coverage and document size

- `19:35:40`   coverage_pct = 100.0 (must still be the warm-mirror ratio, unaffected by derived pages)
- `19:35:40`   data/providers/eurostat.json size = 0.02 MB (this is what data.html downloads)
- `19:35:40`   per-key rows in the document: 100
- `19:35:40`   other providers' series counts unchanged? [('statcan', None), ('fred', None), ('ecb', None), ('oecd', None), ('bis', None), ('census-us', None)]
## P3 hub totals

- `19:35:40`   AFTER hub totals: providers=57 datasets=800961 keys=776,661 gb=214.86
- `19:35:41`   -> data/ops/eurostat-card-fix.json
- `19:35:41` ops 5039 RED: P0:invoke; P1:series; P1:derived
