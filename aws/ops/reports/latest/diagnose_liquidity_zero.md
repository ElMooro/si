# Why did secretary v2.1 report net_liquidity as $0B?

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-04-23T12:21:23+00:00  

## Data

| generated | net_liq | regime | rrp_current | rrp_present | series_count | source | tga_present | walcl_current | walcl_present |
|---|---|---|---|---|---|---|---|---|---|
|  | 0 | TIGHTENING |  |  |  | secretary-latest.json |  |  |  |
|  |  |  |  | False | 0 | fred_in_secretary | False |  | False |
| 2026-04-23T12:18:02.241733Z |  |  | None |  |  | data/report.json |  | None |  |

## Log
## 1. secretary-latest.json liquidity block

- `12:21:22`   version: 2.1
- `12:21:22`   timestamp: 2026-04-23 07:13:51 ET
- `12:21:22`   net_liquidity: $0B
- `12:21:22`   net_liq_change_1m: $0.0B
- `12:21:22`   regime: TIGHTENING
- `12:21:22`   fed_balance_sheet: $0.0B
- `12:21:22`   rrp: $0.0B
- `12:21:22`   tga: $0.0B
- `12:21:22`   reserves: $0.0B
- `12:21:22`   sofr: 0
## 2. FRED data snapshot (from secretary-latest.json)

- `12:21:22`   Total series fetched: 0
- `12:21:22`   WALCL: ❌ NOT PRESENT IN SCAN
- `12:21:22`   RRPONTSYD: ❌ NOT PRESENT IN SCAN
- `12:21:22`   WTREGEN: ❌ NOT PRESENT IN SCAN
- `12:21:22`   WRESBAL: ❌ NOT PRESENT IN SCAN
- `12:21:22`   SOFR: ❌ NOT PRESENT IN SCAN
- `12:21:22`   VIXCLS: ❌ NOT PRESENT IN SCAN
- `12:21:22`   DGS10: ❌ NOT PRESENT IN SCAN
- `12:21:22`   NAPM: ❌ NOT PRESENT IN SCAN
## 3. data/report.json (daily-report-v3 view)

- `12:21:23`   Keys: ['version', 'generated_at', 'fetch_time_seconds', 'cftc_positioning', 'khalid_index', 'risk_dashboard', 'net_liquidity', 'sectors', 'signals', 'market_flow', 'ath_breakouts', 'fred', 'stocks', 'crypto', 'crypto_global']...
- `12:21:23`   FRED series: 14
- `12:21:23`   WALCL: None
- `12:21:23`   RRPONTSYD: None
- `12:21:23`   WTREGEN: None
## 4. Live FRED API test (WALCL right now)

- `12:21:23` ✗   Live FRED fetch failed: HTTP Error 429: Too Many Requests
- `12:21:23` Done
