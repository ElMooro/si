# ops 3963 — self-heal deploy justhodl-domain-barometers

**Status:** failure  
**Duration:** 14.2s  
**Finished:** 2026-07-27T04:45:26+00:00  

## Error

```
SystemExit: 1
```

## Data

| confidence | cron | domains | donor | generated_at | marker_in_source | n_symbols | own_notes_pct | role | runtime | schedule_state | tiers | zip_bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | True |  |  |  |  |  |  | 11307 |
|  |  |  | justhodl-tradingview |  |  |  |  | arn:aws:iam::857687956942:role/lambda-execution-role | python3.12 |  |  |  |
| {"HIGH": 273, "LOW": 143, "MEDIUM": 145} |  | {"LIQUIDITY": 101, "MACRO": 325, "RISK": 135} |  | 2026-07-27T04:45:24.450247+00:00 |  | 561 | 74.5 |  |  |  | {"T1a": 7, "T1a*": 2, "T1b": 3, "T2": 406, "T3f": 7, "T3": 30, "T4": 8, "T5": 98} |  |
|  | cron(20 12 * * ? *) |  |  |  |  |  |  |  |  | ENABLED |  |  |

## Log
## A. diagnose

- `04:45:12`   function DOES NOT EXIST — Deploy Lambdas never created it (confirms the 3962 failure was real, not a settle-timing artifact)
## B. create or update from the runner

- `04:45:13` ✅   create_function issued
## C. settle BY MARKER inside the deployed artifact

- `04:45:13`   [0] state=Pending upd=None
- `04:45:23` ✅   settled with marker after ~10s
## D. invoke

- `04:45:26`   status=200 fnerr=None
- `04:45:26`   payload={"ok": true, "n_symbols": 561, "barometers": {"MACRO": 56.0, "LIQUIDITY": 35.5, "RISK": 37.0}, "own_notes_pct": 74.5}
## E. verify the LIVE artifact

- `04:45:26`   MACRO     score=56.0 state=SUPPORTIVE gate=66.2 breadth=45.8 drivers=72 (+33/-39) disagree=False
- `04:45:26`   LIQUIDITY score=35.5 state=TIGHTENING gate=28.1 breadth=42.9 drivers=42 (+18/-24) disagree=False
- `04:45:26`   RISK      score=37.0 state=TIGHTENING gate=40.0 breadth=34.0 drivers=53 (+18/-35) disagree=False
## F. predictions

- `04:45:26`   crypto                   LEAN_BEARISH  -0.240 MEDIUM driver=liquidity
- `04:45:26`   credit_hy                LEAN_BEARISH  -0.232 MEDIUM driver=risk
- `04:45:26`   us_equity_small          LEAN_BEARISH  -0.218 MEDIUM driver=liquidity
- `04:45:26`   us_equity_large          LEAN_BEARISH  -0.198 MEDIUM driver=liquidity
- `04:45:26`   dollar                   LEAN_BULLISH  +0.198 MEDIUM driver=liquidity
- `04:45:26`   em_equity                LEAN_BEARISH  -0.180 MEDIUM driver=liquidity
- `04:45:26`   intl_developed_equity    LEAN_BEARISH  -0.158 LOW    driver=liquidity
- `04:45:26`   gold                     NEUTRAL       -0.053 LOW    driver=liquidity
- `04:45:26`   commodities              NEUTRAL       -0.019 LOW    driver=macro
- `04:45:26`   duration_treasuries      NEUTRAL       -0.019 LOW    driver=risk
## G. schedule cron(20 12) — after vault 11:35 / gate 11:05

- `04:45:26` ✅   schedule created
- `04:45:26` ✅   marker present in source
- `04:45:26` ✅   zip settled by marker
- `04:45:26` ✅   invoke clean
- `04:45:26` ✅   every symbol in one of the 3 domains
- `04:45:26` ✅   no T6 backstop
- `04:45:26` ✗   >=90% from his own notes
- `04:45:26` ✅   all three barometers scored
- `04:45:26` ✅   each barometer has >=5 live drivers
- `04:45:26` ✅   10 asset-class predictions
- `04:45:26` ✅   every prediction cites a brain note
- `04:45:26` ✅   every symbol carries evidence
- `04:45:26` ✅   grading honest
- `04:45:26` ✅   schedule ENABLED
- `04:45:26` ✗ FAILED: ['>=90% from his own notes']
