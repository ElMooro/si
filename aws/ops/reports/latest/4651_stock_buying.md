# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 296.2s  
**Finished:** 2026-08-13T18:26:33+00:00  

## Error

```
SystemExit: 1
```

## Log
## FMP key donor -> engine env

- `18:21:38` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## authority probes

- `18:21:38` S3: data/fundamental-census-history.json (226 B, 2026-08-01 06:48)
- `18:21:38` S3: data/fundamental-census-matrix.json (1180010 B, 2026-08-01 06:48)
- `18:21:38` S3: data/fundamental-census.json (78138 B, 2026-08-01 06:48)
- `18:21:38` S3: data/fundamentals-decisive-call.json (657 B, 2026-08-13 13:20)
- `18:21:38` S3: data/fundamentals.json (13646 B, 2026-08-13 13:00)
- `18:21:38` ⚠ census_idx replica: '{' was never closed (cidx, line 56)
## verbatim matrix truth

- `18:21:38` top keys: ['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics', 'cols']
- `18:21:38`   generated_at -> str len=32
- `18:21:38`   n_tickers -> int len=-
- `18:21:38`   n_metrics -> int len=-
- `18:21:38`   tickers -> list len=498
- `18:21:38`   sectors -> list len=498
- `18:21:38`   industries -> list len=498
- `18:21:38`   quality -> list len=498
- `18:21:38`   turn -> list len=498
## column census (293) + concept samples

- `18:21:38` ⚠ col census: 'list' object has no attribute 'keys'
- `18:21:38` US10Y via blackswan: last=None age=Noned
## deploy (create-capable) + schedule

- `18:26:33` ✗   [deploy] CONTRACT MISS — v1.0.5 live (created=False)
