# ops 3824 — v1.1 recalibration: global recession ensemble + chokepoint day-two

**Status:** success  
**Duration:** 16.1s  
**Finished:** 2026-07-24T21:33:03+00:00  

## Data

| band | breadth_pct | excluded | gdp_covered | global_prob | n_countries |
|---|---|---|---|---|---|
| BENIGN — expansion intact | 35.0 | 1 | 84.1 | 27.3 | 33 |

## Log
## G0. KEY CONTRACT — live producer artifact

- `21:32:47` ✅   by_country: 34 countries, 34 with phase+gdp_weight
- `21:32:47` ✅     key 'phase' present
- `21:32:47` ✅     key 'cli_level' present
- `21:32:47` ✅     key 'gdp_weight' present
- `21:32:47` ✅     key 'six_month_change' present
- `21:32:47` ✅     key 'dist_200ma_pct' present
## 1. Deploy

- `21:32:48` ✅   FRED_API_KEY from justhodl-nowcast-desk
- `21:32:48`   zip: 88468 bytes
## 1. Lambda

- `21:32:48`   Lambda exists — updating
- `21:32:54` ✅   ✓ updated justhodl-global-recession
## 2. Zip-settle

- `21:33:00` ✅   settled after 5s
## 3. Schedule

- `21:33:00` ✅   Scheduler exists (ConflictException = success)
## 4. Invoke

- `21:33:02`   {'statusCode': 200, 'body': '{"ok": true, "global_pct": 27.3, "n": 33}'}
## 5. Verify the aggregation is REAL

- `21:33:02` ✅   global probability present = 27.3%
- `21:33:02` ✅   countries scored >= 15 = 33
- `21:33:02` ✅   GDP coverage > 0.5 = 84.1
- `21:33:02` ✅   weighted mean inside country range min 5.0 <= 27.3 <= max 81.7
- `21:33:02` ✅   contributions reconcile to global sum 27.26 vs 27.3
- `21:33:02` ✅   no country at 0 or 100 (nothing is certain) 
- `21:33:02` ✅   not saturated at ceiling (<=2 countries >=95%) = []
- `21:33:02` ✅   not saturated at floor (<=2 countries <=5%) = ['KOR', 'HUN']
- `21:33:02` ✅   dispersion is real (spread >= 25pp) = 76.7pp
- `21:33:02` ✅   excluded-not-imputed disclosed 
- `21:33:02` ✅   NOT-MacroMicro disclosure present 
- `21:33:02` ✅   US cross-check reported separately 
- `21:33:02` ✅   breadth published = 35.0% of covered GDP at risk
- `21:33:02`   ── top GDP contributors ──
- `21:33:02`     CHN  RECESSION  p= 72.6% w=18.0    contrib=15.54pp
- `21:33:02`     IND  RECESSION  p= 74.6% w=3.6     contrib=3.19pp
- `21:33:02`     USA  EXPANSION  p=  6.1% w=25.0    contrib=1.81pp
- `21:33:02`     IDN  RECESSION  p= 81.7% w=1.3     contrib=1.26pp
- `21:33:02`     MEX  AT_RISK    p= 45.6% w=1.5     contrib=0.81pp
- `21:33:02`     DEU  EXPANSION  p= 15.7% w=4.0     contrib=0.75pp
- `21:33:02`     BRA  AT_RISK    p= 20.3% w=1.9     contrib=0.46pp
- `21:33:02`     AUS  EXPANSION  p= 25.0% w=1.5     contrib=0.45pp
- `21:33:02`     US curve probit: 27.0% (10y-3m 0.73pp)
- `21:33:02`     Sahm: 0.07 — below trigger
## 6. Chokepoint day-two unattended re-read (pending since 3776)

- `21:33:03`   chokepoint generated_at = 2026-07-24T18:35:06.798301+00:00
- `21:33:03`   age = 3.0h
- `21:33:03` ✅   UNATTENDED RUN CONFIRMED — schedule is genuinely armed
- `21:33:03`   ledger 2,482,194 bytes, modified 2026-07-24 18:35:04+00:00
- `21:33:03` ✅ PASS_ALL 13/13
