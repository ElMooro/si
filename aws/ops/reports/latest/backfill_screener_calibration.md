# 1) Fetch SPY daily closes via Polygon

**Status:** success  
**Duration:** 143.0s  
**Finished:** 2026-05-04T21:20:53+00:00  

## Log
- `21:18:31`   fetched 84 SPY daily closes (2026-01-01 → 2026-05-04)
- `21:18:31`   range: 2026-01-02 ($683.17) → 2026-05-04 ($718.01)
# 2) Scan all screener_top_pick signals

- `21:18:32`   scanned 3445 signal records (7 pages)
- `21:18:32`   needs backfill: 2725
- `21:18:32`   already set:    720
# 3) Patch signal records — set baseline_benchmark_price

- `21:19:42`   ✓ patched 2725 signals
- `21:19:42`   ⚠ skipped (no SPY match): 0
# 4) Now rescore outcomes with correct=None

- `21:19:42`   scanned 1405 outcome records (6 pages)
- `21:19:42`   needs rescore: 1405
- `21:19:42`   already scored: 0
# 5) Rescore each outcome

- `21:20:19`   ✓ rescored: 1405
- `21:20:19`   ⚠ skipped (missing baseline):    0
- `21:20:19`   ⚠ skipped (missing check prices): 0
# 6) Sample 5 freshly-scored outcomes

- `21:20:19`   [0] db107f16-3b46-4044-be58-9fc1eedc6619_day_30
- `21:20:19`       correct: True
- `21:20:19`       excess_return: 5.70975
- `21:20:19`       asset_return:  17.94776
- `21:20:19`       benchmark_return: 12.23801
- `21:20:19`       backfilled_at: 2026-05-04T21:19:42.397321+00:00
- `21:20:19`   [1] 1b34a519-8ce8-40d6-9f69-a86ed894c13b_day_30
- `21:20:19`       correct: True
- `21:20:19`       excess_return: 3.281682
- `21:20:19`       asset_return:  13.954611
- `21:20:19`       benchmark_return: 10.672929
- `21:20:19`       backfilled_at: 2026-05-04T21:19:42.425609+00:00
- `21:20:19`   [2] 8efe867f-8c3b-4878-bcdc-dd58bf78c83a_day_30
- `21:20:19`       correct: True
- `21:20:19`       excess_return: 46.819358
- `21:20:19`       asset_return:  54.618057
- `21:20:19`       benchmark_return: 7.798698
- `21:20:19`       backfilled_at: 2026-05-04T21:19:42.451761+00:00
- `21:20:19`   [3] 1cb46669-48da-4019-8e7e-f450c246fb68_day_30
- `21:20:19`       correct: True
- `21:20:19`       excess_return: 9.887905
- `21:20:19`       asset_return:  22.674734
- `21:20:19`       benchmark_return: 12.786828
- `21:20:19`       backfilled_at: 2026-05-04T21:19:42.477990+00:00
- `21:20:19`   [4] f72a27c0-8276-4c6c-a580-a0cd421892d7_day_30
- `21:20:19`       correct: True
- `21:20:19`       excess_return: 22.760465
- `21:20:19`       asset_return:  28.32136
- `21:20:19`       benchmark_return: 5.560895
- `21:20:19`       backfilled_at: 2026-05-04T21:19:42.503943+00:00
# 7) Distribution of newly-scored correct values

- `21:20:53`   correct=True        n=1075
- `21:20:53`   correct=False       n=330
- `21:20:53` 
- `21:20:53`   → screener_top_pick accuracy: 1075/1405 = 76.5%
