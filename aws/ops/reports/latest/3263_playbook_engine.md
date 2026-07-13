# ops 3263 — his playbook, extracted and evaluated

**Status:** success  
**Duration:** 16.5s  
**Finished:** 2026-07-13T14:18:40+00:00  

## Data

| fam_CONDITIONAL | fam_INVARIANT | fam_TIMING | fam_TURN | n_fails | n_rules | n_warns | source_notes | verdict |
|---|---|---|---|---|---|---|---|---|
| 276 | 260 | 1 | 26 |  | 563 |  | 3322 |  |
|  |  |  |  | 0 |  | 0 |  | PASS |

## Log
- `14:18:24`   zip: 74862 bytes
## 1. Lambda

- `14:18:24`   Lambda missing — creating
- `14:18:29` ✅   ✓ created justhodl-playbook-engine
- `14:18:30` ✅   ✓ Function URL: https://ygn3rwfckm3ya6g6l52637bq2i0fiyvq.lambda-url.us-east-1.on.aws/
## The rulebook

- `14:18:40`   [TIMING] UNTAGGED       [TV:UNTAGGED] ECONOMY CRASH LAGS YIELD CURVE INVERSION BY 30 MONTHS. AND DURING THAT 30 MONTHS W
- `14:18:40`   [CONDITIONAL] TVC:MOVE       [TV:TVC:MOVE] Move: US BOAML US Bond Market option volatility estimate index measures liquidity 
- `14:18:40`   [CONDITIONAL] TVC:US10Y      [TV:TVC:US10Y] US10Y reflects current economic conditions and market future inflations expectati
- `14:18:40`   [CONDITIONAL] FRED:FEDFUNDS  [TV:FRED:FEDFUNDS] WHEN INTEREST RATES ARE BEING CUT THAT’S GENERALLY NOT A POSITIVE TIME FOR A 
- `14:18:40`   [CONDITIONAL] FRED:DRTSCILM  [TV:FRED:DRTSCILM] Liquidity expands in a global growth environment: " A customer Alpha deposit 
- `14:18:40`   [CONDITIONAL] UNTAGGED       [TV:UNTAGGED] Inverted Dollar show you if investors are Risk On or Risk off. when 1/dxy goes up 
## Flagship — his yield-curve timing rule, live

- `14:18:40`   series: FRED T10Y2Y (10y-2y)
- `14:18:40`   most_recent_inversion_onset: 2024-09-05
- `14:18:40`   months_elapsed: 22.2
- `14:18:40`   khalid_lag_months: 30
- `14:18:40`   lag_marker_date: 2027-03-05
- `14:18:40` ✅ 563 tested rules extracted; flagship evaluated on live FRED data
