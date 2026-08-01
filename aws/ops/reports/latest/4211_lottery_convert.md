# ops 4165 — convert the chewed queue

**Status:** failure  
**Duration:** 494.6s  
**Finished:** 2026-08-01T00:08:22+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4211_lottery_convert.py", line 117, in main
    ylive = sum(1 for r6 in v.get("symbols") or []
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_4211_lottery_convert.py", line 118, in <genexpr>
    if r6.get("status") == "LIVE" and re.match(
                                      ^^
NameError: name 're' is not defined. Did you forget to import 're'

```

## Data

| family_labeled | pmi_labeled | total_live |
|---|---|---|
|  |  | 5517 |
| 41 | 76 |  |

## Log
- `00:00:19` ✅   justhodl-tradingview settled at loop 1
- `00:08:22` ✅   artifact after ~450s
- `00:08:22`   statuses: {"LIVE": 5517, "NO_FREE_SOURCE": 4499, "PENDING_RESOLUTION": 40, "DISCONTINUED": 2, "META": 1}
- `00:08:22`   NFS reasons: {"no free API found (TV/TradingEconomics only)": 2554, "country-indicator pair: no free agency sourc": 837, "attempted: no free mirror (tv-proprietary/un": 760, "exchange/venue has no free mirror": 137, "S&P Global PMI licensed": 76, "CFTC dataset lacks this column/market": 56}
