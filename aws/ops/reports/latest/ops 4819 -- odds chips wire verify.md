# G0. FIELD-level spine + consumer contracts

**Status:** failure  
**Duration:** 65.0s  
**Finished:** 2026-08-17T15:01:48+00:00  

## Error

```
SystemExit: 1
```

## Log
- `15:00:44` ✅   spine LIVE, q1.beat_pct=32.7, assignments=4966
- `15:00:44` ✅   data/invest.json present (prev generated_at=2026-08-17T15:00:26)
- `15:00:44` ✅   data/stock-buying.json present (prev as_of=2026-08-17T14:40:18)
# 1. deploy settle (both markers)

- `15:00:45` ✅ justhodl-invest marker 'invest-odds v1' settled (attempt 1)
- `15:01:06` ✅ justhodl-stock-buying marker 'sb-odds v1' settled (attempt 3)
# 2. Event-invoke both + poll (<=12 min)

- `15:01:27` ✅ justhodl-invest fresh in 20s
- `15:01:48` ✅ justhodl-stock-buying fresh in 41s
# 3. truths

- `15:01:48` ✅   invest schema unchanged (invest/0.1)
- `15:01:48` ✅   stock-buying schema_version unchanged (1)
- `15:01:48` ⚠   justhodl-invest: stock_picks empty (bootstrap tape) -- coverage skipped, meta={"as_of": "2026-08-17", "ledger_weeks": 53, "picks_with_odds": 0}
- `15:01:48` ✗   justhodl-stock-buying: coverage only 0/289
- `15:01:48` ✅   justhodl-stock-buying: sampled odds.q == spine q (0/0)
- `15:01:48` ✗   justhodl-stock-buying: header meta missing
- `15:01:48` ✗   sb row fields missing on chip rows
# 4. sample chips

# 5. verdict

- `15:01:48` ✗ HARD FAILS: ['coverage_justhodl-stock-buying', 'meta_justhodl-stock-buying', 'sb_fields']
