# ops 4004 — CB expansion pipeline verify

**Status:** failure  
**Duration:** 936.6s  
**Finished:** 2026-07-28T05:15:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| generated_at | jgb_curve | marker | monotone | n_live |
|---|---|---|---|---|
| 2026-07-28T04:59:52.961497+00:00 |  | tradingview-vault v3.10 ops4010 boj-and-six |  | 480 |
|  | [1.249, 2.032, 2.778, 3.665, 3.969] |  | True |  |

## Log
## A. vault: poll for v3.9 write, then symbol sanity

- `05:01:23`   [3] marker=tradingview-vault v3.9.1 ops new_live=1
- `05:03:23`   [7] marker=tradingview-vault v3.10 ops4 new_live=12
- `05:05:24`   [11] marker=tradingview-vault v3.10 ops4 new_live=12
- `05:07:25`   [15] marker=tradingview-vault v3.10 ops4 new_live=12
- `05:09:25`   [19] marker=tradingview-vault v3.10 ops4 new_live=12
- `05:09:55`   JPMB    LIVE           v=-13.7 src=bank-of-japan sane=True
- `05:09:55`   JPM2YY  LIVE           v=2.23 src=bank-of-japan sane=True
- `05:09:55`   JPM3    LIVE           v=1597003700000000.0 src=fred_alias:MABMM sane=False
- `05:09:55`   JPTANKAN NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `05:09:55`   JPCALLR LIVE           v=0.977 src=bank-of-japan sane=True
- `05:09:55`   BRINTR  LIVE           v=14.25 src=bcb-brazil sane=True
- `05:09:55`   BRFER   LIVE           v=368899.0 src=bcb-brazil sane=True
- `05:09:55`   BRLUSD  LIVE           v=5.1005 src=bcb-brazil sane=True
- `05:09:55`   FIIPYY  LIVE           v=-7.22 src=fred_yoy:FINPROI sane=True
- `05:09:55`   ESIPYY  LIVE           v=-0.25 src=fred_yoy:ESPPROI sane=True
- `05:09:55`   ITIPYY  LIVE           v=-3.34 src=fred_yoy:ITAPROI sane=True
- `05:09:55`   CHIPYY  NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `05:09:55`   KRIPYY  LIVE           v=2.47 src=fred_yoy:KORPROI sane=True
- `05:09:55`   BRIPYY  LIVE           v=2.27 src=fred_yoy:BRAPROI sane=True
- `05:09:55`   IT10Y   LIVE           v=3.734 src=fred_alias:IRLTL sane=True
- `05:09:55`   ES10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `05:09:55`   FI10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `05:09:55`   CH10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `05:09:55`   KR10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `05:09:55`   CHINTR  NO_FREE_SOURCE v=None src=unresolved_econo sane=False
- `05:09:55`   KRINTR  NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
## B. barometers: classify + sign the new symbols

- `05:09:56`   justhodl-domain-barometers: pushed from runner
- `05:10:07`   justhodl-domain-barometers: pushed from runner
- `05:10:18`   justhodl-domain-barometers: pushed from runner
- `05:10:29`   justhodl-domain-barometers: pushed from runner
- `05:10:40`   justhodl-domain-barometers: pushed from runner
- `05:10:50`   justhodl-domain-barometers: pushed from runner
- `05:11:01`   justhodl-domain-barometers: pushed from runner
- `05:11:12`   justhodl-domain-barometers: pushed from runner
- `05:11:23`   justhodl-domain-barometers: pushed from runner
- `05:11:33`   justhodl-domain-barometers: pushed from runner
- `05:11:44`   justhodl-domain-barometers: pushed from runner
- `05:11:55`   justhodl-domain-barometers: pushed from runner
- `05:12:06`   justhodl-domain-barometers: pushed from runner
- `05:12:16`   justhodl-domain-barometers: pushed from runner
- `05:12:27`   justhodl-domain-barometers: pushed from runner
- `05:12:38`   justhodl-domain-barometers: pushed from runner
- `05:12:49`   justhodl-domain-barometers: pushed from runner
- `05:12:59`   justhodl-domain-barometers: pushed from runner
- `05:13:10`   justhodl-domain-barometers: pushed from runner
- `05:13:21`   justhodl-domain-barometers: pushed from runner
- `05:13:31`   justhodl-domain-barometers: pushed from runner
- `05:13:42`   justhodl-domain-barometers: pushed from runner
- `05:13:53`   justhodl-domain-barometers: pushed from runner
- `05:14:04`   justhodl-domain-barometers: pushed from runner
- `05:14:14`   justhodl-domain-barometers: pushed from runner
- `05:14:25`   justhodl-domain-barometers: pushed from runner
- `05:14:36`   justhodl-domain-barometers: pushed from runner
- `05:14:47`   justhodl-domain-barometers: pushed from runner
- `05:14:57`   justhodl-domain-barometers: pushed from runner
- `05:15:08`   justhodl-domain-barometers: pushed from runner
- `05:15:21`   invoke fnerr=None
- `05:15:21`   JPMB    dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   JPM2YY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   JPM3    dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   JPTANKAN dom=MACRO     pol=1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   JPCALLR dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `05:15:21`   BRINTR  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `05:15:21`   BRFER   dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   BRLUSD  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `05:15:21`   FIIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   ESIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   ITIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   CHIPYY  dom=MACRO     pol=1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   KRIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   BRIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `05:15:21`   IT10Y   dom=RISK      pol=-1 basis=brain_note status=LIVE
- `05:15:21`   ES10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   FI10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   CH10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   KR10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   CHINTR  dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   KRINTR  dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `05:15:21`   MACRO     52.5 NEUTRAL voting=102
- `05:15:21`   LIQUIDITY 36.7 TIGHTENING voting=53
- `05:15:21`   RISK      31.3 TIGHTENING voting=62
## C. risk-gate: carry leg cites JGB 10Y

- `05:15:29`   invoke fnerr=None
- `05:15:29`   posture=RISK_OFF sizing=0.45
- `05:15:29`   carry inputs head: ge_h": 0.2, "status": "OK"}, {"input": "jgb10y_carry_cost", "feed": "tradingview-vault(MOF)", "value": 2.778, "score_adj": -0.15, "note": "JGB 10Y par yield \u2014 yen carry funding cost; >2.5% -0.15, >4% jump -0.1 [tv-9fa576184567fa8f]", "age_h": 0.2, "status
- `05:15:29` ✗   vault marker v3.9
- `05:15:29` ✗   >=15 of 21 new symbols LIVE and sane
- `05:15:29` ✅   JGB curve monotone (sanity)
- `05:15:29` ✗   barometers v1.3 settled
- `05:15:29` ✅   barometers invoke clean
- `05:15:29` ✅   >=15 new symbols carry a brain-note polarity
- `05:15:29` ✅   risk-gate jp10y patch settled
- `05:15:29` ✅   risk-gate invoke clean
- `05:15:29` ✅   carry leg includes jgb10y_carry_cost
- `05:15:29` ✅   posture valid
- `05:15:29` ✗ FAILED: ['vault marker v3.9', '>=15 of 21 new symbols LIVE and sane', 'barometers v1.3 settled']
