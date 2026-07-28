# ops 4004 — CB expansion pipeline verify

**Status:** failure  
**Duration:** 14.8s  
**Finished:** 2026-07-28T12:42:08+00:00  

## Error

```
SystemExit: 1
```

## Data

| generated_at | jgb_curve | marker | monotone | n_live |
|---|---|---|---|---|
| 2026-07-28T12:39:23.889352+00:00 |  | tradingview-vault v3.10.1 ops4013 jpm3+2y |  | 480 |
|  | [1.249, 2.032, 2.778, 3.665, 3.969] |  | True |  |

## Log
## A. vault: poll for v3.9 write, then symbol sanity

- `12:41:54` ✅   v3.9 with >=8 new LIVE after ~0s
- `12:41:54`   JPMB    LIVE           v=-13.7 src=bank-of-japan sane=True
- `12:41:54`   JPM2YY  LIVE           v=2.23 src=bank-of-japan sane=True
- `12:41:54`   JPM3    LIVE           v=1597003700000000.0 src=fred_alias:MABMM sane=False
- `12:41:54`   JPTANKAN NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `12:41:54`   JPCALLR LIVE           v=0.977 src=bank-of-japan sane=True
- `12:41:54`   BRINTR  LIVE           v=14.25 src=bcb-brazil sane=True
- `12:41:54`   BRFER   LIVE           v=369065.0 src=bcb-brazil sane=True
- `12:41:54`   BRLUSD  LIVE           v=5.1005 src=bcb-brazil sane=True
- `12:41:54`   FIIPYY  LIVE           v=-7.22 src=fred_yoy:FINPROI sane=True
- `12:41:54`   ESIPYY  LIVE           v=-0.25 src=fred_yoy:ESPPROI sane=True
- `12:41:54`   ITIPYY  LIVE           v=-3.34 src=fred_yoy:ITAPROI sane=True
- `12:41:54`   CHIPYY  NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `12:41:54`   KRIPYY  LIVE           v=2.47 src=fred_yoy:KORPROI sane=True
- `12:41:54`   BRIPYY  LIVE           v=2.27 src=fred_yoy:BRAPROI sane=True
- `12:41:54`   IT10Y   LIVE           v=3.734 src=fred_alias:IRLTL sane=True
- `12:41:54`   ES10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `12:41:54`   FI10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `12:41:54`   CH10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `12:41:54`   KR10Y   NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
- `12:41:54`   CHINTR  NO_FREE_SOURCE v=None src=unresolved_econo sane=False
- `12:41:54`   KRINTR  NO_FREE_SOURCE v=None src=unresolved_tv_on sane=False
## B. barometers: classify + sign the new symbols

- `12:41:58`   invoke fnerr=None
- `12:41:58`   JPMB    dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   JPM2YY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   JPM3    dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   JPTANKAN dom=MACRO     pol=1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   JPCALLR dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `12:41:58`   BRINTR  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `12:41:58`   BRFER   dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   BRLUSD  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `12:41:58`   FIIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   ESIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   ITIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   CHIPYY  dom=MACRO     pol=1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   KRIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   BRIPYY  dom=MACRO     pol=1 basis=brain_note status=LIVE
- `12:41:58`   IT10Y   dom=RISK      pol=-1 basis=brain_note status=LIVE
- `12:41:58`   ES10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   FI10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   CH10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   KR10Y   dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   CHINTR  dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   KRINTR  dom=MACRO     pol=-1 basis=brain_note status=NO_FREE_SOURCE
- `12:41:58`   MACRO     51.2 NEUTRAL voting=107
- `12:41:58`   LIQUIDITY 35.8 TIGHTENING voting=53
- `12:41:58`   RISK      31.3 TIGHTENING voting=62
## C. risk-gate: carry leg cites JGB 10Y

- `12:42:08`   invoke fnerr=None
- `12:42:08`   posture=RISK_OFF sizing=0.45
- `12:42:08`   carry inputs head: ge_h": 0.0, "status": "OK"}, {"input": "jgb10y_carry_cost", "feed": "tradingview-vault(MOF)", "value": 2.778, "score_adj": -0.15, "note": "JGB 10Y par yield \u2014 yen carry funding cost; >2.5% -0.15, >4% jump -0.1 [tv-9fa576184567fa8f]", "age_h": 0.0, "status
- `12:42:08` ✅   vault marker v3.10
- `12:42:08` ✗   >=15 of 21 new symbols LIVE and sane
- `12:42:08` ✅   JGB curve monotone (sanity)
- `12:42:08` ✅   barometers v1.4 settled
- `12:42:08` ✅   barometers invoke clean
- `12:42:08` ✅   >=15 new symbols carry a brain-note polarity
- `12:42:08` ✅   risk-gate jp10y patch settled
- `12:42:08` ✅   risk-gate invoke clean
- `12:42:08` ✅   carry leg includes jgb10y_carry_cost
- `12:42:08` ✅   posture valid
- `12:42:08` ✗ FAILED: ['>=15 of 21 new symbols LIVE and sane']
