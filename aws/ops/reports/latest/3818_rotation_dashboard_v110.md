# ops 3818 — rotation-dashboard v1.2.0 (COT low-n + flows field names)

**Status:** success  
**Duration:** 15.6s  
**Finished:** 2026-07-24T20:44:56+00:00  

## Data

| crowding_rows | degraded | dollar_direction | dxy_3m | overweight | regime |
|---|---|---|---|---|---|
| 11 | NONE | RISING | 2.87 | XLV, XLF, RSP, IWD, IWM, SPY, XLK | STAGFLATION |

## Log
## G0. KEY CONTRACT — against LIVE artifacts, not source

- `20:44:41` ✅   dollar: bbdxy.dxy_synth.chg_3m_pct = 2.87
- `20:44:41` ✅   cftc: data{} has 29 contracts with weekly_reports
## G0b. etf-true-flows shape (why flows joined 0 rows)

- `20:44:41`   top-level: ['engine', 'version', 'generated_at', 'duration_s', 'n_etfs', 'maturity', 'method', 'inflows', 'outflows', 'category_rotation', 'by_etf']
- `20:44:41`   'inflows': list[25], row keys=['ticker', 'category', 'shares_outstanding', 'price', 'aum_est_b', 'net_flow_1d_usd', 'net_flow_5d_usd', 'net_flow_20d_usd', 'shares_chg_5d_pct']
- `20:44:41`     sample: {"ticker": "VOO", "category": "BROAD_EQUITY_US", "shares_outstanding": 2432284459.0, "price": 682.41992, "aum_est_b": 1659.84, "net_flow_1d_usd": -9257099916.0, "net_flow_5d_usd": 7576320126.0, "net_flow_20d_usd": -31068406133.0, "shares_chg_5d_pct": -0.55}
## 1. Deploy v1.1.0

- `20:44:42` ✅   POLYGON_API_KEY from justhodl-equity-research
- `20:44:42`   zip: 94753 bytes
## 1. Lambda

- `20:44:42`   Lambda exists — updating
- `20:44:47` ✅   ✓ updated justhodl-rotation-dashboard
## 2. Zip-settle

- `20:44:52` ✅   settled after 5s
## 3. Invoke

- `20:44:56`   {'statusCode': 200, 'body': '{"ok": true, "scored": 37, "eligible": 20, "overweight": ["XLV", "XLF", "RSP", "IWD", "IWM", "SPY", "XLK"]}'}
## 4. Verify — did degraded shrink?

- `20:44:56` ✅   dollar 3m populated = 2.87 (RISING)
- `20:44:56` ✅   dollar tilt applied into prior = -0.25
- `20:44:56` ✅   no 'dollar-radar 3m' in degraded 
- `20:44:56` ✅   crowding populated on >=4 assets = 11 assets
- `20:44:56` ✅   no 'cftc-all-cache unmapped' in degraded 
- `20:44:56` ✅   ETF flows joined on >=10 assets = 35 assets
- `20:44:56`     XLV   INFLOW   20d $0.85B (1.98% AUM)
- `20:44:56`     XLF   INFLOW   20d $1.87B (3.33% AUM)
- `20:44:56`     RSP   INFLOW   20d $1.81B (1.88% AUM)
- `20:44:56`     IWM   INFLOW   20d $0.06B (0.07% AUM)
- `20:44:56`     SPY   INFLOW   20d $6.16B (0.79% AUM)
- `20:44:56`     XLK   FLAT     20d $0.00B (None% AUM)
- `20:44:56` ✅   still scoring full universe = 37
- `20:44:56` ✅   trend gate still discriminates = 20/37
- `20:44:56`     IWM   RTY   COT idx 25.7 -> NEUTRAL (n=7)
- `20:44:56`     SPY   ES    COT idx 71.0 -> NEUTRAL (n=7)
- `20:44:56`     USO   CL    COT idx 100.0 -> CROWDED (n=7)
- `20:44:56`     DBC   CL    COT idx 100.0 -> CROWDED (n=7)
- `20:44:56`     CPER  HG    COT idx 0.0 -> WASHED_OUT (n=7)
- `20:44:56`     EFA   NQ    COT idx 22.3 -> NEUTRAL (n=7)
- `20:44:56`     SHY   ZF    COT idx 63.9 -> NEUTRAL (n=7)
- `20:44:56`     SLV   SI    COT idx 0.0 -> WASHED_OUT (n=7)
- `20:44:56`   ── top 8 after dollar tilt ──
- `20:44:56`     # 1 XLV      38.7 LEADING    gate=PASS
- `20:44:56`     # 2 XLF      25.4 LEADING    gate=PASS
- `20:44:56`     # 3 RSP      21.0 LEADING    gate=PASS
- `20:44:56`     # 4 IWD      19.8 LEADING    gate=PASS
- `20:44:56`     # 5 IWM      12.5 LEADING    gate=PASS
- `20:44:56`     # 6 SPY      11.4 BENCHMARK  gate=PASS
- `20:44:56`     # 7 XLK       9.9 LEADING    gate=PASS
- `20:44:56`     # 8 USO       9.0 LAGGING    gate=PASS
- `20:44:56` ✅ PASS_ALL 8/8
