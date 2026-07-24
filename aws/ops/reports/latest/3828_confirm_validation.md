# ops 3828 — v1.2.1 supplement validation: global recession ensemble + chokepoint day-two

**Status:** success  
**Duration:** 31.8s  
**Finished:** 2026-07-24T22:00:59+00:00  

## Data

| band | breadth_pct | confirm_counts | excluded | gdp_covered | global_prob | n_countries | oecd_usable | unconfirmed_share_pct |
|---|---|---|---|---|---|---|---|---|
| BENIGN — expansion intact | 35.0 | {"CONFIRMED": 0, "DIVERGENT": 1, "UNCONFIRMED": 32} | 1 | 84.1 | 28.9 | 33 | False | 84.3 |

## Log
## G0. KEY CONTRACT — live producer artifact

- `22:00:28` ✅   by_country: 34 countries, 34 with phase+gdp_weight
- `22:00:28` ✅     key 'phase' present
- `22:00:28` ✅     key 'cli_level' present
- `22:00:28` ✅     key 'gdp_weight' present
- `22:00:28` ✅     key 'six_month_change' present
- `22:00:28` ✅     key 'dist_200ma_pct' present
## 1. Deploy

- `22:00:29` ✅   FRED_API_KEY from justhodl-nowcast-desk
- `22:00:29`   zip: 90374 bytes
## 1. Lambda

- `22:00:29`   Lambda exists — updating
- `22:00:32` ✅   ✓ updated justhodl-global-recession
## 2. Zip-settle

- `22:00:43` ✅   settled after 10s
## 3. Schedule

- `22:00:43` ✅   Scheduler exists (ConflictException = success)
## 4. Invoke

- `22:00:57`   {'statusCode': 200, 'body': '{"ok": true, "global_pct": 28.9, "n": 33}'}
## 5. Verify the aggregation is REAL

- `22:00:58` ✅   global probability present = 28.9%
- `22:00:58` ✅   countries scored >= 15 = 33
- `22:00:58` ✅   GDP coverage > 0.5 = 84.1
- `22:00:58` ✅   weighted mean inside country range min 8.4 <= 28.9 <= max 72.1
- `22:00:58` ✅   contributions reconcile to global sum 28.93 vs 28.9
- `22:00:58` ✅   no country at 0 or 100 (nothing is certain) 
- `22:00:58` ✅   not saturated at ceiling (<=2 countries >=95%) = []
- `22:00:58` ✅   not saturated at floor (<=2 countries <=5%) = []
- `22:00:58` ✅   dispersion is real (spread >= 25pp) = 63.7pp
- `22:00:58` ✅   excluded-not-imputed disclosed 
- `22:00:58` ✅   NOT-MacroMicro disclosure present 
- `22:00:58` ✅   US cross-check reported separately 
- `22:00:58` ✅   breadth published = 35.0% of covered GDP at risk
- `22:00:58` ✅   confirmation block present 
- `22:00:58` ✅   every country carries a confirmation state = {'CONFIRMED': 0, 'DIVERGENT': 1, 'UNCONFIRMED': 32} vs 33 countries
- `22:00:58` ✅   unconfirmed exposure is quantified = 84.3% of headline
- `22:00:58` ✅   OECD staleness is decided, not assumed usable=False period=2024-01-01 age=30mo
- `22:00:58` ✅   no confirmation off an implausible index value = []
- `22:00:58` ✅   coverage verdict published = NONE — no usable independent leg exists right now (OECD CLI stale and no valid survey supp
- `22:00:58` ✅   dampening actually applied where unbacked at least one country pulled toward neutral, or all confirmed
- `22:00:58`     CHN: UNCONFIRMED — p=63.7% detail={"note": "no independent hard-data leg available \u2014 this country rests on equity momentum alone"}
- `22:00:58`     IND: UNCONFIRMED — p=65.4% detail={"note": "no independent hard-data leg available \u2014 this country rests on equity momentum alone"}
- `22:00:58`   ── top GDP contributors ──
- `22:00:58`     CHN  RECESSION  p= 63.7% w=18.0    contrib=13.63pp [UNCONFIRMED]
- `22:00:58`     USA  EXPANSION  p= 15.3% w=25.0    contrib=4.55pp [DIVERGENT]
- `22:00:58`     IND  RECESSION  p= 65.4% w=3.6     contrib=2.8pp [UNCONFIRMED]
- `22:00:58`     IDN  RECESSION  p= 72.1% w=1.3     contrib=1.11pp [UNCONFIRMED]
- `22:00:58`     DEU  EXPANSION  p= 19.3% w=4.0     contrib=0.92pp [UNCONFIRMED]
- `22:00:58`     MEX  AT_RISK    p= 42.5% w=1.5     contrib=0.76pp [UNCONFIRMED]
- `22:00:58`     BRA  AT_RISK    p= 23.2% w=1.9     contrib=0.52pp [UNCONFIRMED]
- `22:00:58`     GBR  EXPANSION  p= 12.4% w=3.3     contrib=0.49pp [UNCONFIRMED]
- `22:00:58`     US curve probit: 27.0% (10y-3m 0.73pp)
- `22:00:58`     Sahm: 0.07 — below trigger
## 6. Chokepoint day-two unattended re-read (pending since 3776)

- `22:00:59`   chokepoint generated_at = 2026-07-24T18:35:06.798301+00:00
- `22:00:59`   age = 3.4h
- `22:00:59` ✅   UNATTENDED RUN CONFIRMED — schedule is genuinely armed
- `22:00:59`   ledger 2,482,194 bytes, modified 2026-07-24 18:35:04+00:00
- `22:00:59` ✅ PASS_ALL 20/20
