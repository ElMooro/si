- `14:44:32` G0 v6 flush armed; page copy matches cron
- `14:44:32` G0b deployed artifact confirmed to carry the v6 flush
**Status:** failure  
**Duration:** 257.3s  
**Finished:** 2026-08-21T14:48:49+00:00  

## Error

```
SystemExit: 1
```

## Log
- `14:44:32` G0b justhodl-sec-13f unchanged this push -- no redeploy expected
- `14:44:32` G0b justhodl-13f-clone-alpha unchanged this push -- no redeploy expected
- `14:48:17` positions refreshed after 223s (full re-parse, no cache reuse)
- `14:48:23` PASS G1 per-fund rows no longer show pre-fix tickers  []
- `14:48:23` FAIL G2 per-fund row names agree with the aggregate  29 rows: [{'fund': 'BERKSHIRE', 'ticker': 'VRSN', 'row': 'INC. VERISIGN,', 'agg': 'VERISIGN'}, {'fund': 'RENAISSANCE', 'ticker': 'EXEL', 'row': 'EXELIXIS, INC.', 'agg': 'EXELIXIS'}, {'fund': 'RENAISSANCE', 'ticker': 'VRSN', 'row': 'INC. VERISIGN,', 'agg': 'VERISIGN'}, {'fund': 'AQR', 'ticker': 'ABNB', 'row': 'AIRBNB, INC.', 'agg': 'AIRBNB'}, {'fund': 'AQR', 'ticker': 'CPAY', 'row': 'CORPAY, INC.', 'agg': 'CORPAY'}]
- `14:48:44` PASS G3a clone-alpha rebuilt (was 4d stale, weekly cron)  20s
- `14:48:49` PASS G3b leaderboard no longer says 'Duration Capital'  
- `14:48:49` PASS G4a roster 18/18  18/18/0
- `14:48:49` PASS G4b zero label-vs-filer mismatches  []
- `14:48:49` PASS G4c holders <= roster  
- `14:48:49` as_of=2026-06-30 tickers=8131
- `14:48:49` ops 4941 RED: G2 per-fund row names agree with the aggregate
