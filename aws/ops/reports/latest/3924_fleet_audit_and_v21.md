# ops 3924 — fleet audit of the 186 + vault v2.1 fleet-resolver

**Status:** success  
**Duration:** 112.8s  
**Finished:** 2026-07-26T21:23:58+00:00  

## Data

| coverage_pct | n_found_in_fleet | n_live | n_no_free_source_before | statuses |
|---|---|---|---|---|
|  |  |  | 186 |  |
|  | 21 |  |  |  |
| 68.1 |  | 386 |  | {'NO_FREE_SOURCE': 179, 'LIVE': 386, 'DISCONTINUED': 2} |

## Log
## A. which NO_FREE_SOURCE symbols does the fleet ALREADY compute?

- `21:22:11`   ADR -> justhodl-13f-positions, justhodl-boom-stage, justhodl-dark-pool, justhodl-deal-scanner
- `21:22:11`   BAMLC4A0C710YEY -> justhodl-credit-stress, justhodl-daily-report-v3
- `21:22:11`   BDI -> justhodl-eurodollar-plumbing
- `21:22:11`   BTPBUND -> justhodl-credit-stress, justhodl-crisis-composite
- `21:22:11`   DCPF3M -> justhodl-crisis-canaries, justhodl-eurodollar-plumbing, openbb-system2-api
- `21:22:11`   DCPN3M -> justhodl-canary-grid, justhodl-eurodollar-plumbing, justhodl-repo-monitor, justhodl-risk-gate
- `21:22:11`   DECLI -> justhodl-crypto-emergence, justhodl-insider-radar, justhodl-market-internals, justhodl-sector-emergence
- `21:22:11`   EFS -> justhodl-ai-brief-router, justhodl-confluence-meta
- `21:22:11`   ES10Y-TVC -> justhodl-credit-stress, justhodl-crisis-composite
- `21:22:11`   EUGDPYY -> justhodl-macro-nowcast
- `21:22:11`   FR10Y-TVC -> justhodl-credit-stress
- `21:22:11`   IBHY -> openbb-system2-api
- `21:22:11`   IT10Y -> justhodl-credit-stress, justhodl-crisis-composite
- `21:22:11`   RIFSPPNA2P2D90NB -> justhodl-risk-gate
- `21:22:11`   RIFSPPNAAD90NB -> justhodl-canary-grid, justhodl-repo-monitor
- `21:22:11`   SVXY -> justhodl-etf-flows, justhodl-etf-fund-flows, justhodl-etf-true-flows, justhodl-rv-iv-scanner
- `21:22:11`   UNTAGGED -> justhodl-brain-compiler, justhodl-notes-intel, justhodl-playbook-engine, justhodl-tv-notes-crawler
- `21:22:11`   USCA -> justhodl-smart-money-13f
- `21:22:11`   USM0 -> justhodl-canary-grid
- `21:22:11`   USM1 -> economyapi, justhodl-anomaly-detector, justhodl-divergence-engine-v2, justhodl-implied-prob
- `21:22:11`   UVXY -> justhodl-ai-chat, justhodl-concentration-liquidity, justhodl-crisis-knowledge-base, justhodl-etf-census
## B. vault v2.1 settle + invoke

- `21:22:11` ✅   settled attempt 1
- `21:23:58`   BTPBUND: LIVE value=76.4 src=fleet:data/euro-fragmentation.json
- `21:23:58`   IT10Y: LIVE value=3.734 src=fred_alias:IRLTLT01ITM156N
- `21:23:58`   USM2: LIVE value=23052.3 src=fred_alias:M2SL
- `21:23:58`   EUINTR: LIVE value=2.25 src=fred_alias:ECBDFR
- `21:23:58`   JPM3: LIVE value=1597003700000000.0 src=fred_alias:MABMM301JPM189S
- `21:23:58` ✅   audit map persisted to data/tv-fleet-map.json
- `21:23:58` ✅   fleet audit ran
- `21:23:58` ✅   v2.1 settled
- `21:23:58` ✅   LIVE increased over 379
- `21:23:58` ✅   BTPBUND LIVE from the fleet's own feed
- `21:23:58` ✅   zero bare UNRESOLVED preserved
- `21:23:58` ✅ PASS_ALL — v2.1: 386 LIVE (68.1%), 21 of the remaining symbols located in fleet sources for the next pass
