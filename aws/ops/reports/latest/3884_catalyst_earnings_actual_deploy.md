# ops 3884 — deploy EARNINGS_ACTUAL, gate on the exact semi tickers this fix is for

**Status:** success  
**Duration:** 18.9s  
**Finished:** 2026-07-25T19:42:56+00:00  

## Data

| before_by_type | before_n_events | before_s3 | n_earnings_actual | tickers_present |
|---|---|---|---|---|
| {'AUCTION': 7, 'EARNINGS': 568, 'FOMC': 2, 'REBALANCE': 1, 'WITCHING': 1} | 579 | 2026-07-25T17:51:43+00:00 |  |  |
|  |  |  | 46 | ['ABT', 'ACN', 'ASML', 'AXP', 'BAC', 'BLK', 'BX', 'C', 'CB', 'CME', 'DHR', 'ELV', 'FDX', 'GE', 'GM', 'GOOG', 'GOOGL', 'GS', 'HCA', 'HON', 'IBM', 'INTC', 'ISRG', 'JNJ', 'JPM', 'LMT', 'MS', 'MU', 'NFLX', 'NKE', 'NOC', 'NOW', 'PEP', 'PLD', 'PM', 'RTX', 'SCHW', 'SLB', 'T', 'TMO', 'TMUS', 'TSLA', 'TSM', 'UNH', 'VZ', 'WFC'] |

## Log
## 1. BEFORE

## 2. ZIP-SETTLE BY MARKER

- `19:42:38` ✅   new artifact live on attempt 1 (92,210 zip bytes)
- `19:42:46` ✅   State=Active LastUpdateStatus=Successful
## 3. invoke

- `19:42:56` ✅   artifact rewritten on attempt 1 (2026-07-25T19:42:49+00:00)
## 4. the falsifiable gate — the exact tickers this fix exists for

- `19:42:56` ✅   EARNINGS_ACTUAL now exists as an event type
- `19:42:56` ✅   INTC present with negative days_to
- `19:42:56` ✅   INTC's real -7.89% 1d return survived intact
- `19:42:56` ✅   TSM present
- `19:42:56` ✅   ASML present
- `19:42:56` ✅   MU present
- `19:42:56` ✅   no event lost its days_to sign convention (all EARNINGS_ACTUAL are days_to <= 0)
- `19:42:56` ✅   by_type reflects the new source
- `19:42:56` ✅   forward EARNINGS source untouched (count didn't collapse)
## 5. spot-check the exact record

- `19:42:56`   INTC full record: {"date": "2026-07-23", "time": null, "type": "EARNINGS_ACTUAL", "title": "INTC reported", "subtitle": "EPS 0.42 vs 0.19 est (+121.0% surprise) \u00b7 1d reaction -7.9%", "impact": "HIGH", "source": "Benzinga (via Massive) \u2014 actual results", "url": null, "size_billions": null, "ticker": "INTC", "period_end": "Q2 2026", "eps_actual": 0.42, "eps_estimate": 0.19, "eps_surprise_pct": 121.05, "revenue_actual": 16128000000.0, "revenue_surprise_pct": 12.0, "pead_label": "BEAT_BUT_FELL", "pead_score": 43, "return_1d_pct": -7.89, "return_5d_pct": null, "return_20d_pct": null, "days_to": -2}
- `19:42:56` ✅ PASS_ALL — 46 EARNINGS_ACTUAL events live, including the exact 4 tickers (INTC/TSM/ASML/MU) this fix was built for
