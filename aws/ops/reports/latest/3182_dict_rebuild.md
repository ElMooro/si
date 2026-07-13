# ops 3182 — purge the poisoned dictionary, rebuild it right

**Status:** success  
**Duration:** 422.0s  
**Finished:** 2026-07-13T00:22:20+00:00  

## Error

```
SystemExit: 0
```

## Data

| coverage_pct | dict_before | fred_named | fred_to_fetch | junk_purged | kept | n_fails | n_warns | named | named_pct | symbols | unique_symbols | verdict | was |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 59.9 |  |  |  |  |  |  |  |  |  |  | 6507 |  | 59.7 |
|  | 6507 |  |  | 528 | 5979 |  |  |  |  |  |  |  |  |
|  |  |  | 528 |  |  |  |  |  |  |  |  |  |  |
|  |  | 452 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 6431 | 98.8 | 6507 |  |  |  |
|  |  |  |  |  |  | 0 | 0 |  |  |  |  | PASS |  |

## Log
## 1. Re-map (picks up continuous futures)

- `00:19:21` ✅ symbol map rebuilt: 59.9% (3899 symbols)
## 2. Purge the junk, keep the good

- `00:19:22` ✅ purged 528 junk entries (the cache was poisoning itself)
## 3. Rebuild FRED titles on the runner (throttled)

- `00:22:19` ✅ 452 FRED series carry their OFFICIAL title
## 4. THE GATE — do the names read like a human wrote them?

- `00:22:20`   FRED:DGS10           →                                                            [— · — · —]
- `00:22:20`   FRED:WALCL           → Assets: Total Assets: Total Assets (Less Eliminations from [Mil. of U.S. $ · W · 2002-12-18 → 2026-07-08]
- `00:22:20`   FRED:PRAWMINDEXM     → Global price of Agr. Raw Material Index                    [Index 2016 = 100 · M · 1992-01-01 → 2026-06-01]
- `00:22:20`   FRED:FEDFUNDS        → Federal Funds Effective Rate                               [% · M · 1954-07-01 → 2026-06-01]
- `00:22:20`   NYMEX:CL1!           → CL1! (NYMEX)                                               [None: None]
- `00:22:20`   TVC:US10Y            → US10Y (TVC)                                                [FRED: DGS10]
- `00:22:20`   NASDAQ:NVDA          → Nvidia Corp                                                [MARKET: NVDA]
- `00:22:20`   ECONOMICS:CNFER      → China — Total reserves (includes gold, current US$)        [WORLDBANK: CN|FI.RES.TOTL.CD]
## 5. Refresh the fleet dictionary consumers

- `00:22:20` weekly engine invoked to fill the non-FRED tail
