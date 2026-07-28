# ops 4004 — CB expansion pipeline verify

**Status:** success  
**Duration:** 132.6s  
**Finished:** 2026-07-28T04:37:01+00:00  

## Data

| generated_at | jgb_curve | marker | monotone | n_live |
|---|---|---|---|---|
| 2026-07-28T04:34:49.176872+00:00 |  | tradingview-vault v3.9.1 ops4006 alias-union |  | 469 |
|  | [1.249, 2.032, 2.778, 3.665, 3.969] |  | True |  |

## Log
## A. vault: poll for v3.9 write, then symbol sanity

- `04:36:20`   [3] marker=tradingview-vault v3.9 ops40 new_live=0
- `04:36:50` ✅   v3.9 with >=8 new LIVE after ~120s
- `04:36:50`   JP01Y   LIVE           v=1.249 src=mof-japan sane=True
- `04:36:50`   JP05Y   LIVE           v=2.032 src=mof-japan sane=True
- `04:36:50`   JP10Y   LIVE           v=2.778 src=mof-japan sane=True
- `04:36:50`   JP20Y   LIVE           v=3.665 src=mof-japan sane=True
- `04:36:50`   JP30Y   LIVE           v=3.969 src=mof-japan sane=True
- `04:36:50`   NOINTR  LIVE           v=4.29 src=norges-bank sane=True
- `04:36:50`   NO10Y   LIVE           v=4.302 src=norges-bank sane=True
- `04:36:50`   PEINTR  LIVE           v=4.25 src=bcrp-peru sane=True
- `04:36:50`   PENUSD  LIVE           v=3.406 src=bcrp-peru sane=True
- `04:36:50`   PEFER   LIVE           v=325836.331 src=bcrp-peru sane=True
## B. barometers: classify + sign the new symbols

- `04:36:54`   invoke fnerr=None
- `04:36:54`   JP01Y   dom=LIQUIDITY pol=-1 basis=brain_note status=LIVE
- `04:36:54`   JP05Y   dom=LIQUIDITY pol=-1 basis=brain_note status=LIVE
- `04:36:54`   JP10Y   dom=LIQUIDITY pol=-1 basis=brain_note status=LIVE
- `04:36:54`   JP20Y   dom=LIQUIDITY pol=-1 basis=brain_note status=LIVE
- `04:36:54`   JP30Y   dom=LIQUIDITY pol=-1 basis=brain_note status=LIVE
- `04:36:54`   NOINTR  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `04:36:54`   NO10Y   dom=RISK      pol=-1 basis=brain_note status=LIVE
- `04:36:54`   PEINTR  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `04:36:54`   PENUSD  dom=MACRO     pol=-1 basis=brain_note status=LIVE
- `04:36:54`   PEFER   dom=MACRO     pol=1 basis=brain_note status=LIVE
- `04:36:54`   MACRO     53.3 NEUTRAL voting=95
- `04:36:54`   LIQUIDITY 40.1 TIGHTENING voting=48
- `04:36:54`   RISK      31.6 TIGHTENING voting=61
## C. risk-gate: carry leg cites JGB 10Y

- `04:37:01`   invoke fnerr=None
- `04:37:01`   posture=RISK_OFF sizing=0.45
- `04:37:01`   carry inputs head: ge_h": 0.0, "status": "OK"}, {"input": "jgb10y_carry_cost", "feed": "tradingview-vault(MOF)", "value": 2.778, "score_adj": -0.15, "note": "JGB 10Y par yield \u2014 yen carry funding cost; >2.5% -0.15, >4% jump -0.1 [tv-9fa576184567fa8f]", "age_h": 0.0, "status
- `04:37:01` ✅   vault marker v3.9
- `04:37:01` ✅   >=8 of 10 new symbols LIVE and sane
- `04:37:01` ✅   JGB curve monotone (sanity)
- `04:37:01` ✅   barometers v1.3 settled
- `04:37:01` ✅   barometers invoke clean
- `04:37:01` ✅   >=8 new symbols carry a brain-note polarity
- `04:37:01` ✅   risk-gate jp10y patch settled
- `04:37:01` ✅   risk-gate invoke clean
- `04:37:01` ✅   carry leg includes jgb10y_carry_cost
- `04:37:01` ✅   posture valid
- `04:37:01` ✅ PASS_ALL — 10/10 new CB indicators LIVE; barometers signed 10; risk-gate carry cites JGB 10Y; posture RISK_OFF
