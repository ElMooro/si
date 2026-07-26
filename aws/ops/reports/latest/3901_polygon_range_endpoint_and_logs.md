# ops 3901 — the ACTUAL endpoint evaluate_call() uses + real invocation history

**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-07-26T03:25:46+00:00  

## Data

| first_close | n_bars_returned | n_recent_streams |
|---|---|---|
| 84.9 | 7 |  |
|  |  | 5 |

## Log
## 1. get the verified-working key

- `03:25:45` ✅   key present, len=32
## 2. THE EXACT endpoint evaluate_call() uses — historical range for a real past call date

- `03:25:45` ✅   SUCCESS: {"ticker": "ABT", "queryCount": 7, "resultsCount": 7, "adjusted": true, "results": [{"v": 11018435.60082, "vw": 84.7053, "o": 84.18, "c": 84.9, "h": 85.265, "l": 84.05, "t": 1778731200000, "n": 126225}, {"v": 13208556.890043, "vw": 84.7496, "o": 85.89, "c": 84.47, "h": 86.475, "l": 84.13, "t": 1778817600000, "n": 135371}, {"v": 12346980.169489, "vw": 87.2225, "o": 84.86, "c": 87.91, "h": 87.92, "l": 84.4, "t": 1779076800000, "n": 139385}, {"v": 11693547.774266, "vw": 88.7448, "o": 88.36, "c": 88
## 3. real CloudWatch invocation history — has this actually been running, and what do its own logs say

- `03:25:46`   stream 2026/07/25/[$LATEST]a59516b9316a4a9e8fe5c5f29c126d8c: last event 4.4h ago
- `03:25:46`   stream 2026/07/25/[$LATEST]96be97b3845a4c5a911163d544da7d61: last event 5.4h ago
- `03:25:46`   stream 2026/07/24/[$LATEST]5cc06a84e7304024a6f75ed6d04cbfad: last event 28.4h ago
- `03:25:46`   stream 2026/07/24/[$LATEST]87bbc42d1a6748eea823554d4f429265: last event 29.4h ago
- `03:25:46`   stream 2026/07/23/[$LATEST]593bdce4bd9e48c494d4442f5bae1ad1: last event 52.4h ago
- `03:25:46`   MOST RECENT RUN full log tail:
update err 2026-07-20#MNST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-20#MRK#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-20#SPCX#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-20#TSLA#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-20#WST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#GILD#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#INCY#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#JNJ#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#LLY#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#MNST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#MRK#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#SPCX#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#TSLA#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#TSM#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-21#WST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#AMD#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#AVGO#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#GILD#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#INCY#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#JNJ#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#LLY#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#LLY#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#MNST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#MRK#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#MU#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#SKHY#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#SPCX#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#TSLA#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#TSM#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-22#WST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#AMD#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#AVGO#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#GILD#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#GOOG#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#INCY#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#LLY#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#LLY#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#MNST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#MRK#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#MU#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#PM#REGIME_PICK: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#SKHY#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#SPCX#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#TSLA#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#TSM#OPTIONS_TIER_A: Float types are not supported. Use Decimal types instead.
update err 2026-07-23#WST#REGIME_PICK: Float types are not supported. Use Decimal types instead.
updated=0  unchanged=27  unevaluable=0
✓ trade-journal.json written (200 ledger entries, 5 strategies)
END RequestId: 3b6a653f-f030-434d-bab5-53345f551940
REPORT RequestId: 3b6a653f-f030-434d-bab5-53345f551940	Duration: 172220.61 ms	Billed Duration: 172745 ms	Memory Size: 1024 MB	Max Memory Used: 128 MB	Init Duration: 523.84 ms	
XRAY TraceId: 1-6a654014-43efba513bfe1fbb2c388d30	SegmentId: aae2282d7ebb949e	Sampled: true
- `03:25:46` ✅ PROBE COMPLETE
