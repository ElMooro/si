# ops 4643 — DXY predict-the-future engine

**Status:** failure  
**Duration:** 210.7s  
**Finished:** 2026-08-13T01:42:38+00:00  

## Error

```
SystemExit: 1
```

## Data

| candidates | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|
| [["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Bonds - Sovereign : Bonds dumping especially sovereign bonds dumping is a major Warning signal that macro is deteriorati", 11], ["BONDS : Fixed income tends to lean itself toward macro more than equities thats more micro", 103], ["Crypto : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 327], ["Dollar Shortage Indicators", 17], ["DXY predict Future Moves : Currencies best seen in \"3M\": FED hiking rates strengthen the Dollar", 85], ["DXY: Currencies best seen in \"3M\" : DXY pumping means tightening and liquidity drying up in the Eurodollar system.", 90], ["Emerging Market Sovereign Crisis - Eurodollar crisis", 20]] |  |  |  |  |  |  |  |  |  |
|  | None |  |  |  |  |  |  |  |  |
|  |  | MACRO PREDICT (1 lists) | 65 | 0 | 47 | None | None | None | None |

## Log
## pre-dump: macro-predict list candidates

- `01:39:08` ✅   [list-exists] macro-predict list present: [('Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.', 437), ('Bonds - Sovereign : Bonds dumping especially sovereign bonds dumping is a major Warning signal that macro is deteriorati', 11), ('BONDS : Fixed income tends to lean itself toward macro more than equities thats more micro', 103)]
## deploy (ops-side) + settle + schedule

- `01:39:24` ✅   [deploy] v1.0.0 live (created=False)
- `01:39:25` hourly schedule created
## run + dxy truth

- `01:39:32` ✅   [list-found] list 'MACRO PREDICT (1 lists)' (65 members)
- `01:39:32` ✅   [resolution] 47/65 resolved (shared cache pool)
- `01:39:32` ✗   [polarity] CONTRACT MISS — 0 mechanically-signed rows
- `01:39:32` ✗   [dials] CONTRACT MISS — MACRO TREND None (None) · REVERSAL None (None)
## edge (with CF purge)

- `01:39:32` CF purge: True
- `01:39:37` CONTROL dxy: len=6612 has_json=True
- `01:39:37` ⚠ purge: HTTP Error 404: Not Found
- `01:39:38` edge 1: HTTP Error 404: Not Found
- `01:39:38` edge probe 1: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:39:58` edge 2: HTTP Error 404: Not Found
- `01:39:58` edge probe 2: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:40:18` edge 3: HTTP Error 404: Not Found
- `01:40:18` edge probe 3: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:40:38` edge 4: HTTP Error 404: Not Found
- `01:40:38` edge probe 4: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:40:58` edge 5: HTTP Error 404: Not Found
- `01:40:58` edge probe 5: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:41:18` edge 6: HTTP Error 404: Not Found
- `01:41:18` edge probe 6: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:41:38` edge 7: HTTP Error 404: Not Found
- `01:41:38` edge probe 7: page_ok=False len=? head=? | pay_ok=False keys=?
- `01:41:58` edge probe 8: page_ok=True len=6646 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=False keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `01:42:18` edge probe 9: page_ok=True len=6646 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=False keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `01:42:38` ✗   [edge] CONTRACT MISS — page + payload at the edge
## verdict

- `01:42:38` ✗ liq-indicators: 3 red
