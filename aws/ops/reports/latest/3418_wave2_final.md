# ops 3415 — joins wave 2

**Status:** success  
**Duration:** 138.5s  
**Finished:** 2026-07-17T22:59:09+00:00  

## Error

```
SystemExit: 0
```

## Log
- `22:57:58` PASS  G0_all_settled — 5 engines settled
- `22:57:58` PASS  G1_registry_schedule — exists
- `22:58:39` PASS  G2_registry — feeds=785 stale=105 worst=['data/feedback-summary.json', 'data/history-api-url.json', 'data/user-trades-stats.json', 'data/user-trades.json']
- `22:58:42` PASS  G3_squeeze_guard — book=6 excluded=[] risk_tagged=0
- `22:59:02` PASS  G4_jsi_src — jsi_pctile=29.728588024078572 src=jsi-history.self scale=1.0
- `22:59:09` FAIL  G5_setups_context — sector_ctx=0/25 walls=0/25 playbook_rules=3 sample_pb=[{"id": "tv-0a62c4e914bbe3e3", "symbol": "UNTAGGED", "family": "TIMING", "text": "[TV:UNTAGGED] ECONOMY CRASH LAGS YIELD
- `22:59:09` VERDICT: GAPS: G5_setups_context
