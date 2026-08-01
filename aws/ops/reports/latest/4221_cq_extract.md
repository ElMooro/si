# ops 4217 — wave-2 (recession, sentinel, dollar)

**Status:** failure  
**Duration:** 50.2s  
**Finished:** 2026-08-01T03:31:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| cq_aliases | cq_metrics | cqfeed | crypto-cycle-risk_block | crypto-exchange-flows_block |
|---|---|---|---|---|
|  |  | {"metrics": 19} |  |  |
| 45 | 19 |  |  |  |
|  |  |  |  | False |
|  |  |  | True |  |

## Log
- `03:31:13`   [cq_flows] wired: {"marker": "ops4221", "reserve_btc": 2713907.20793477, "reserve_usd": 170570179773.8276, "netflow_btc": 505.4122
- `03:31:29`   [cq_cycle] wired: {"marker": "ops4221", "mvrv": 1.18837439, "sopr": 0.99465663, "sth_sopr": 0.98739621, "nupl": 0.15851435, "reali
- `03:31:29` ✅   ledger: wired=16
- `03:31:29` ✅   cq metrics >= 17
- `03:31:29` ✗   justhodl-crypto-exchange-flows cq_flows emitted
- `03:31:29` ✅   justhodl-crypto-cycle-risk cq_cycle emitted
- `03:31:29` ✅   mvrv plausible 0.5-5
- `03:31:29` ✗ FAILED: ['justhodl-crypto-exchange-flows cq_flows emitted']
