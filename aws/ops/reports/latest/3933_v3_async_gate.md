# ops 3933 — v3.0 async full-run gate

**Status:** failure  
**Duration:** 648.6s  
**Finished:** 2026-07-26T22:50:22+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | elapsed_s | fred_calls | n_live | statuses |
|---|---|---|---|---|
| 79.5 | 475.0 | 270 | 446 | {'META': 1, 'LIVE': 446, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 112} |

## Log
- `22:39:34`   async force fired at 2026-07-26T22:39:34.455960+00:00; polling artifact…
- `22:47:38` ✅   artifact refreshed after ~480s
- `22:47:38`   USCLI: LIVE value=120 src=fleet:data/global-business-cycle.json:by_country.USA.cli_level
- `22:47:38`   EUINTR: LIVE value=2.25 src=fred_alias:ECBDFR
- `22:47:38`   10USNOTE: LIVE value=108.625 src=yahoo:ZN=F
- `22:47:38`   NOVO_B: LIVE value=320.6 src=yahoo:NOVO-B.CO
- `22:47:38`   EU02Y-TVC: None value=None src=None
## cached run (sync, fast)

- `22:50:22`   invoke2: {"ok": true, "n_symbols": 561, "n_live": 443, "n_cached": 212, "fred_calls": 395, "coverage_pct": 79.0}
- `22:50:22` ✅   POLYGON_KEY present
- `22:50:22` ✅   force run wrote fresh artifact
- `22:50:22` ✗   LIVE >= 450
- `22:50:22` ✅   zero bare UNRESOLVED
- `22:50:22` ✅   USCLI LIVE via fleet
- `22:50:22` ✅   EUINTR LIVE via fred_alias
- `22:50:22` ✅   10USNOTE LIVE via yahoo:ZN=F
- `22:50:22` ✅   NOVO_B LIVE via yahoo:NOVO-B.CO
- `22:50:22` ✗   EU02Y-TVC LIVE via ecb:
- `22:50:22` ✅   cache engaged (n_cached >= 100)
- `22:50:22` ✗   fred calls small on cached run
- `22:50:22` ✗ FAILED: ['LIVE >= 450', 'EU02Y-TVC LIVE via ecb:', 'fred calls small on cached run']
