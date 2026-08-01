# ops 4217 — wave-2 (recession, sentinel, dollar)

**Status:** success  
**Duration:** 51.4s  
**Finished:** 2026-08-01T03:32:47+00:00  

## Data

| cq_aliases | cq_metrics | cqfeed | crypto-cycle-risk_block | crypto-exchange-flows_block |
|---|---|---|---|---|
|  |  | {"metrics": 19} |  |  |
| 45 | 19 |  |  |  |
|  |  |  |  | True |
|  |  |  | True |  |

## Log
- `03:32:31`   [cq_flows] wired: {"marker": "ops4221", "reserve_btc": 2713907.20793477, "reserve_usd": 170570179773.8276, "netflow_btc": 505.4122
- `03:32:47`   [cq_cycle] wired: {"marker": "ops4221", "mvrv": 1.18837439, "sopr": 0.99465663, "sth_sopr": 0.98739621, "nupl": 0.15851435, "reali
- `03:32:47` ✅   ledger: wired=18
- `03:32:47` ✅   cq metrics >= 17
- `03:32:47` ✅   justhodl-crypto-exchange-flows cq_flows emitted
- `03:32:47` ✅   justhodl-crypto-cycle-risk cq_cycle emitted
- `03:32:47` ✅   mvrv plausible 0.5-5
- `03:32:47` ✅ CQ EXTRACTED — 19 metrics daily, aliases in bus, two engines on the paid rail
