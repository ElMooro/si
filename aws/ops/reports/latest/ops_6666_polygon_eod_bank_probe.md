# ops 6666 - Polygon EOD bank: why no answers in the wave-2 light (read-only)

**Status:** success  
**Duration:** 2.9s  
**Finished:** 2026-10-26T07:14:00+00:00  

## Log
- `07:13:57` now 2026-10-26T07:13:57.713854+00:00; polygon_bankfirst last completed session = 2026-10-23; in session window = False
## 1. the eod manifest

- `07:13:57` manifest key data/warm/polygon-full/eod/manifest.json -> {"engine": "justhodl-polygon-eod", "version": "1.0.0", "updated_at": "2026-10-26T06:55:04+00:00", "phase": "STEADY", "runs": 11, "sessions_available": 1296, "sessions_folded": 1296, "sessions_pending": 0, "first_session": "2021-08-26", "last_session": "2026-10-23", "tickers": 21058, "objects": 21058, "pages": 21058, "window_start": "2021-08-26", "gaps": [1, ["2021-09-06"]], "holidays_recorded": [50, ["2026-04-03", "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07"]], "fields": [8, ["l", "c", "v", "vw", "n"]], "layout": "data/warm/polygon-full/eod/<T>.json.gz", "label": "Polygon grouped-daily sessions (justhodl-polygon-full) transposed per ticker - never LIVE; daily 03:10 UTC after polygon-full 02:29", "last_run": {"started": "2026-10-26T06:51:48+00:00", "sessions": ["2026-06-09", "2026-06-10", "2026-06-11", "2026-06-12", "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-0
## 2. per-ticker eod files + readers

- `07:13:57` AAPL: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:13:58`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.23s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:13:58`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:13:58`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 347.725)
- `07:13:58`    walker historical-price-eod-full: 5000 rows, last 2026-10-23
- `07:13:58` MSFT: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:13:58`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.06s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:13:58`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:13:58`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 524.51)
- `07:13:58`    walker historical-price-eod-full: 5000 rows, last 2026-10-23
- `07:13:58` GME: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:13:59`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.15s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:13:59`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:13:59`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 24.82)
- `07:13:59`    walker historical-price-eod-full: 5000 rows, last 2026-10-23
- `07:13:59` SOFI: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:13:59`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.08s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:13:59`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:13:59`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 14.615)
- `07:13:59`    walker historical-price-eod-full: 1482 rows, last 2026-10-23
- `07:13:59` PLTR: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:13:59`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.08s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:13:59`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:13:59`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 196.75)
- `07:13:59`    walker historical-price-eod-full: 1524 rows, last 2026-10-23
- `07:13:59` NVDA: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:13:59`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.06s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:13:59`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:13:59`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 210.96)
- `07:13:59`    walker historical-price-eod-full: 5000 rows, last 2026-10-23
- `07:14:00` F: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:14:00`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.09s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:14:00`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:14:00`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 11.6)
- `07:14:00`    walker historical-price-eod-full: 5000 rows, last 2026-10-23
- `07:14:00` CCL: eod file rows 1296 first 2021-08-26 last 2026-10-23 (last row date 2026-10-23) fields ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']
- `07:14:00`    answer(range 2025-11-25..2026-10-26) -> 229 rows in 0.05s | STATS {"hits": 1, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0} | ctl off=None reason=None
- `07:14:00`    answer(range ..last completed session) -> 229 rows | STATS {"hits": 2, "misses": 0, "stale": 0, "unshaped": 0, "gap": 0, "passthrough": 0, "errors": 0, "bypassed": 0}
- `07:14:00`    bars(complete_only) -> 5; bars(any) -> 5, last ('2026-10-23', 26.11)
- `07:14:00`    walker historical-price-eod-full: 5000 rows, last 2026-10-23
## 3. session calendar check

- `07:14:00` eod bank BANK_FIRST_SESSION 2021-08-26; today weekday 0
- `07:14:00` ✅ ops 6666 complete (read-only)
