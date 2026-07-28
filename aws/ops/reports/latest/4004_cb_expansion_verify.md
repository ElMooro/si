# ops 4004 — CB expansion pipeline verify

**Status:** failure  
**Duration:** 136.0s  
**Finished:** 2026-07-28T04:12:46+00:00  

## Error

```
SystemExit: 1
```

## Data

| generated_at | jgb_curve | marker | monotone | n_live |
|---|---|---|---|---|
| 2026-07-28T04:10:31.263781+00:00 |  | tradingview-vault v3.9 ops4003 cb-expansion |  | 456 |
|  | [None, None, None, None, None] |  | False |  |

## Log
## A. vault: poll for v3.9 write, then symbol sanity

- `04:12:34` ✅   v3.9 vault after ~120s
- `04:12:34`   JP01Y   None           v=None src=None sane=False
- `04:12:34`   JP05Y   None           v=None src=None sane=False
- `04:12:34`   JP10Y   None           v=None src=None sane=False
- `04:12:34`   JP20Y   None           v=None src=None sane=False
- `04:12:34`   JP30Y   None           v=None src=None sane=False
- `04:12:34`   NOINTR  None           v=None src=None sane=False
- `04:12:34`   NO10Y   None           v=None src=None sane=False
- `04:12:34`   PEINTR  None           v=None src=None sane=False
- `04:12:34`   PENUSD  None           v=None src=None sane=False
- `04:12:34`   PEFER   None           v=None src=None sane=False
## B. barometers: classify + sign the new symbols

- `04:12:38`   invoke fnerr=None
- `04:12:38`   JP01Y   dom=None      pol=None basis=None status=None
- `04:12:38`   JP05Y   dom=None      pol=None basis=None status=None
- `04:12:38`   JP10Y   dom=None      pol=None basis=None status=None
- `04:12:38`   JP20Y   dom=None      pol=None basis=None status=None
- `04:12:38`   JP30Y   dom=None      pol=None basis=None status=None
- `04:12:38`   NOINTR  dom=None      pol=None basis=None status=None
- `04:12:38`   NO10Y   dom=None      pol=None basis=None status=None
- `04:12:38`   PEINTR  dom=None      pol=None basis=None status=None
- `04:12:38`   PENUSD  dom=None      pol=None basis=None status=None
- `04:12:38`   PEFER   dom=None      pol=None basis=None status=None
- `04:12:38`   MACRO     53.8 NEUTRAL voting=95
- `04:12:38`   LIQUIDITY 42.2 TIGHTENING voting=48
- `04:12:38`   RISK      33.9 TIGHTENING voting=61
## C. risk-gate: carry leg cites JGB 10Y

- `04:12:46`   invoke fnerr=None
- `04:12:46`   posture=RISK_OFF sizing=0.45
- `04:12:46`   carry inputs head: []
- `04:12:46` ✅   vault marker v3.9
- `04:12:46` ✗   >=8 of 10 new symbols LIVE and sane
- `04:12:46` ✗   JGB curve monotone (sanity)
- `04:12:46` ✅   barometers v1.3 settled
- `04:12:46` ✅   barometers invoke clean
- `04:12:46` ✗   >=8 new symbols carry a brain-note polarity
- `04:12:46` ✅   risk-gate jp10y patch settled
- `04:12:46` ✅   risk-gate invoke clean
- `04:12:46` ✗   carry leg includes jgb10y_carry_cost
- `04:12:46` ✅   posture valid
- `04:12:46` ✗ FAILED: ['>=8 of 10 new symbols LIVE and sane', 'JGB curve monotone (sanity)', '>=8 new symbols carry a brain-note polarity', 'carry leg includes jgb10y_carry_cost']
