# ops 3167 — free-source symbol map (1990+)

**Status:** success  
**Duration:** 436.0s  
**Finished:** 2026-07-12T21:59:05+00:00  

## Error

```
SystemExit: 0
```

## Data

| coverage_pct | lists | n_fails | n_warns | probes | probes_reaching_1990s | searches_used | src_coingecko | src_formula | src_fred | src_stooq | src_unmapped | templates_dead | templates_live | theses_measured | theses_over_60pct_coverage | unique_symbols | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 207 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6507 |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 15 | 2 |  |  |  |  |
| 44.5 |  |  |  |  |  | 0 | 37 | 337 | 782 | 1739 | 3612 |  |  |  |  |  |  |
|  |  |  |  | 15 | 10 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 107 | 63 |  |  |
|  |  | 0 | 3 |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Universe census

- `21:51:50` ── top ECONOMICS indicator codes (the mapping targets): BOT=186, FI=174, GDPYY=168, GDG=164, DIR=150, FER=121, CBBS=89, BCOI=73, IPRI=59, INTR=38, INBR=31, CS=30, CAG=28, NO=28
## 2. Validate OECD templates (kill the dead ids)

- `21:51:58`   live: CLI, HOU
- `21:51:58`   DEAD (fall back to FRED search): GDPYY, IRYY, CPI, INTR, IR, UR, M2, M3, BCOI, CCI, PROD, EXP, IMP, BOT, SP
## 3. Map the universe

- `21:59:01`   UNMAPPED    3612  (55.5%)
- `21:59:01`   STOOQ       1739  (26.7%)
- `21:59:01`   FRED         782  (12.0%)
- `21:59:01`   FORMULA      337  (5.2%)
- `21:59:01`   COINGECKO     37  (0.6%)
- `21:59:01` ✅ symbol-map.json written: 2895/6507 symbols on a free source (44.5%)
## 4. HISTORY PROOF — earliest date each source returns

- `21:59:01`   US 10y                       FRED   DGS10                1990-01-02 → 2026-07-09  (9135 obs)
- `21:59:01`   Fed funds                    FRED   FEDFUNDS             1990-01-01 → 2026-06-01  (438 obs)
- `21:59:02`   US M2                        FRED   M2SL                 1990-01-01 → 2026-05-01  (437 obs)
- `21:59:02`   Fed B/S                      FRED   WALCL                2002-12-18 → 2026-07-08  (1230 obs)
- `21:59:02`   VIX                          FRED   VIXCLS               1990-01-02 → 2026-07-09  (9226 obs)
- `21:59:03`   Bund 10y (template)          FRED   IRLTLT01DEM156N      1990-01-01 → 2026-05-01  (437 obs)
- `21:59:03`   S&P 500                      MARKET ^GSPC                1990-01-02 → 2026-07-10  (9197 obs)
- `21:59:03`   DXY                          MARKET DX-Y.NYB             1990-01-01 → 2026-07-10  (9306 obs)
- `21:59:04`   SPY                          MARKET SPY                  1993-01-29 → 2026-07-10  (8418 obs)
- `21:59:04`   Gold                         MARKET GC=F                 2000-08-30 → 2026-07-10  (6488 obs)
- `21:59:04`   Nikkei                       MARKET ^N225                1990-01-04 → 2026-07-10  (8962 obs)
- `21:59:05`   NVDA                         MARKET NVDA                 1999-01-22 → 2026-07-10  (6908 obs)
- `21:59:05`   VIX index                    MARKET ^VIX                 1990-01-02 → 2026-07-10  (9198 obs)
- `21:59:05` ✅ 10/15 probes deliver 1990s-or-earlier history — the thesis study can be rebuilt on 35 years instead of 2
## 5. Per-thesis coverage under the new map

- `21:59:05` ── coverage by thesis (top 14):
- `21:59:05`   100%   40/40   Commercial banks
- `21:59:05`   100%   36/36   Corp Yields
- `21:59:05`   100%   31/31   Commercial banks
- `21:59:05`   100%   29/29   Global Commodities prices
- `21:59:05`   100%   26/26   3X ETF
- `21:59:05`   100%   22/22   Energy and oil stocks
- `21:59:05`    97%   37/38   Commercial Banks
- `21:59:05`    97%   34/35   Credit Spreads
- `21:59:05`    97%   29/30   Fed Balance sheet
- `21:59:05`    96%  263/273  Brent Johnson Portfolio: THE SHORTTERM SWING
- `21:59:05`    96%   50/52   Different Types of Stock indexes
- `21:59:05`    96%   24/25   Credit Risk
- `21:59:05`    96%   22/23   GLOBAL EQUITY
- `21:59:05`    95%  123/129  Banking Sector : Banks = Liquidity Proxy Eve
- `21:59:05` ⚠ coverage under 50% — FRED-search budget spends 500 lookups per run and caches them; a second run maps the next tranche
- `21:59:05` ⚠ probe empty: FRED:NAEXKP01JPNQ657S (Japan GDP YoY (template))
- `21:59:05` ⚠ probe empty: FRED:CPALTT01GBRM659N (UK CPI YoY (template))
