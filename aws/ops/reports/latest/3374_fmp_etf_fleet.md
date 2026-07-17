# ops 3374 — FMP ETF fleet hardening gates

**Status:** success  
**Duration:** 375.7s  
**Finished:** 2026-07-17T04:03:02+00:00  

## Error

```
SystemExit: 0
```

## Log
- `03:57:20` PASS  G1_deploys_settled_bundled — settled=['justhodl-etf-constituents', 'justhodl-industry-rotation']
- `04:03:02` FAIL  G2_ec_feed_quality — no fresh file
- `04:03:02` PASS  G3_ir_tolerant_weights — zip bundles pctf + _wpct; next 21:35Z run self-proves
- `04:03:02` TR recon: {"deployed": true, "last_modified": "2026-07-17T03:57:48.000+0000", "timeout": 900}
- `04:03:02` VERDICT: GAPS: G2_ec_feed_quality
