# ops 4217 — wave-2 (recession, sentinel, dollar)

**Status:** success  
**Duration:** 40.9s  
**Finished:** 2026-08-01T03:07:34+00:00  

## Data

| allocator_block | allocator_err | canary-warroom_block | canary-warroom_err | debate-engine_block | debate-engine_err | intraday-pulse_block | intraday-pulse_err |
|---|---|---|---|---|---|---|---|
| True | None |  |  |  |  |  |  |
|  |  | True | None |  |  |  |  |
|  |  |  |  | True | None |  |  |
|  |  |  |  |  |  | True | None |

## Log
- `03:07:20`   [bus_context] wired: {"marker": "ops4219", "world_policy_rate_median": 5.0, "us10y": 4.61, "ip_contracting_pct
- `03:07:20`   allocator: {"marker": "ops4219", "world_policy_rate_median": 5.0, "us10y": 4.61, "ip_contracting_pct": 39.8, "gdp_contracting_pct": 6.5, "note": "global sizing context"}
- `03:07:29`   [bus_canaries] wired: {"marker": "ops4219", "ip_contracting_pct": 39.8, "gdp_contracting_pct": 6.5, "inflation
- `03:07:29`   warroom: {"marker": "ops4219", "ip_contracting_pct": 39.8, "gdp_contracting_pct": 6.5, "inflation_hot_n": 41, "inflation_hot_sample": ["AOIRYY", "ARIRYY", "BDIRYY", "BIIRYY", "BOIRYY", "CUI
- `03:07:33`   [bus_facts] wired: {"marker": "ops4219", "us_cpi_yoy": 3.73, "us_ip_yoy": -0.39, "de10y": null, "cn_gdp_yoy": 
- `03:07:34`   [bus_pulse] wired: {"marker": "ops4219", "us10y": 4.61, "usintr": 3.63, "de10y": null, "note": "rates trio bes
- `03:07:34` ✅   ledger COMPLETE: wired=14 next=0
- `03:07:34` ✅   justhodl-allocator bus_context emitted
- `03:07:34` ✅   allocator world rate 1-12
- `03:07:34` ✅   justhodl-canary-warroom bus_canaries emitted
- `03:07:34` ✅   justhodl-debate-engine bus_facts emitted
- `03:07:34` ✅   justhodl-intraday-pulse bus_pulse emitted
- `03:07:34` ✅ EVERYWHERE COMPLETE — fourteen surfaces on one bus
