# ops 3839 — v1.3 hard legs (port throughput + credit impulse): global recession ensemble + chokepoint day-two

**Status:** success  
**Duration:** 15.6s  
**Finished:** 2026-07-25T01:31:53+00:00  

## Data

| band | breadth_pct | confirm_counts | excluded | gdp_covered | global_prob | n_countries | oecd_usable | unconfirmed_share_pct |
|---|---|---|---|---|---|---|---|---|
| WATCH — pockets of stress | 35.0 | {"CONFIRMED": 4, "DIVERGENT": 4, "UNCONFIRMED": 25} | 1 | 84.1 | 32.0 | 33 | False | 27.4 |

## Log
## G0. KEY CONTRACT — live producer artifact

- `01:31:37` ✅   by_country: 34 countries, 34 with phase+gdp_weight
- `01:31:37` ✅     key 'phase' present
- `01:31:37` ✅     key 'cli_level' present
- `01:31:37` ✅     key 'gdp_weight' present
- `01:31:37` ✅     key 'six_month_change' present
- `01:31:37` ✅     key 'dist_200ma_pct' present
## 1. Deploy

- `01:31:38` ✅   FRED_API_KEY from justhodl-nowcast-desk
- `01:31:38`   zip: 91700 bytes
## 1. Lambda

- `01:31:38`   Lambda exists — updating
- `01:31:45` ✅   ✓ updated justhodl-global-recession
## 2. Zip-settle

- `01:31:50` ✅   settled after 5s
## 3. Schedule

- `01:31:50` ✅   Scheduler exists (ConflictException = success)
## 4. Invoke

- `01:31:52`   {'statusCode': 200, 'body': '{"ok": true, "global_pct": 32.0, "n": 33}'}
## 5. Verify the aggregation is REAL

- `01:31:52` ✅   global probability present = 32.0%
- `01:31:52` ✅   countries scored >= 15 = 33
- `01:31:52` ✅   GDP coverage > 0.5 = 84.1
- `01:31:52` ✅   weighted mean inside country range min 8.2 <= 32.0 <= max 72.3
- `01:31:52` ✅   contributions reconcile to global sum 31.95 vs 32.0
- `01:31:52` ✅   no country at 0 or 100 (nothing is certain) 
- `01:31:52` ✅   not saturated at ceiling (<=2 countries >=95%) = []
- `01:31:52` ✅   not saturated at floor (<=2 countries <=5%) = []
- `01:31:52` ✅   dispersion is real (spread >= 25pp) = 64.1pp
- `01:31:52` ✅   excluded-not-imputed disclosed 
- `01:31:52` ✅   NOT-MacroMicro disclosure present 
- `01:31:52` ✅   US cross-check reported separately 
- `01:31:52` ✅   breadth published = 35.0% of covered GDP at risk
- `01:31:52` ✅   confirmation block present 
- `01:31:52` ✅   every country carries a confirmation state = {'CONFIRMED': 4, 'DIVERGENT': 4, 'UNCONFIRMED': 25} vs 33 countries
- `01:31:52` ✅   unconfirmed exposure is quantified = 27.4% of headline
- `01:31:52` ✅   OECD staleness is decided, not assumed usable=False period=2024-01-01 age=30mo
- `01:31:52` ✅   no confirmation off an implausible index value = []
- `01:31:52` ✅   coverage verdict published = PARTIAL — only 4 of 33 countries have an independent check; the rest rest on equity moment
- `01:31:52` ✅   CONFIRMED count > 0 (hard legs are working) confirmed=4 divergent=4
- `01:31:52` ✅   at least 3 countries on a hard leg = ['CHN', 'USA', 'JPN', 'DEU', 'AUS', 'KOR', 'CHL', 'ESP']
- `01:31:52` ✅   unconfirmed share fell below 84.3% (v1.2.1 baseline) = 27.4%
- `01:31:52`     CHN: CONFIRMED p=72.3% {"source": "port throughput (physical)", "n_ports": 5, "median_yoy_pct": -31.8, "worst_port": "Shanghai", "note": "physical trade volume \u2014 fully 
- `01:31:52`     USA: DIVERGENT p=16.8% {"source": "port throughput (physical)", "n_ports": 4, "median_yoy_pct": -7.3, "worst_port": "Los Angeles-Long Beach", "note": "physical trade volume 
- `01:31:52`     JPN: DIVERGENT p=16.5% {"source": "port throughput (physical)", "n_ports": 5, "median_yoy_pct": -5.0, "worst_port": "Tokyo", "note": "physical trade volume \u2014 fully inde
- `01:31:52`     DEU: CONFIRMED p=16.2% {"source": "port throughput (physical)", "n_ports": 3, "median_yoy_pct": 2.5, "worst_port": "Hamburg", "note": "physical trade volume \u2014 fully ind
- `01:31:52`     AUS: DIVERGENT p=29.2% {"source": "port throughput (physical)", "n_ports": 11, "median_yoy_pct": -7.9, "worst_port": "Abbot Point", "note": "physical trade volume \u2014 ful
- `01:31:52`     KOR: DIVERGENT p=15.4% {"source": "port throughput (physical)", "n_ports": 3, "median_yoy_pct": -2.7, "worst_port": "Busan", "note": "physical trade volume \u2014 fully inde
- `01:31:52`     CHL: CONFIRMED p=48.4% {"source": "port throughput (physical)", "n_ports": 11, "median_yoy_pct": -29.4, "worst_port": "Lirquen", "note": "physical trade volume \u2014 fully 
- `01:31:52`     ESP: CONFIRMED p=8.2% {"source": "port throughput (physical)", "n_ports": 2, "median_yoy_pct": 0.0, "worst_port": "Valencia", "note": "physical trade volume \u2014 fully in
- `01:31:52` ✅   dampening actually applied where unbacked at least one country pulled toward neutral, or all confirmed
- `01:31:52`     CHN: CONFIRMED — p=72.3% detail={"source": "port throughput (physical)", "n_ports": 5, "median_yoy_pct": -31.8, "worst_port": "Shanghai", "note": "physical trade volume \u2
- `01:31:52`     IND: UNCONFIRMED — p=64.0% detail={"note": "no independent hard-data leg available \u2014 this country rests on equity momentum alone"}
- `01:31:52`   ── top GDP contributors ──
- `01:31:52`     CHN  RECESSION  p= 72.3% w=18.0    contrib=15.47pp [CONFIRMED]
- `01:31:52`     USA  EXPANSION  p= 16.8% w=25.0    contrib=4.99pp [DIVERGENT]
- `01:31:52`     IND  RECESSION  p= 64.0% w=3.6     contrib=2.74pp [UNCONFIRMED]
- `01:31:52`     IDN  RECESSION  p= 68.8% w=1.3     contrib=1.06pp [UNCONFIRMED]
- `01:31:52`     JPN  EXPANSION  p= 16.5% w=4.2     contrib=0.82pp [DIVERGENT]
- `01:31:52`     MEX  AT_RISK    p= 44.2% w=1.5     contrib=0.79pp [UNCONFIRMED]
- `01:31:52`     DEU  EXPANSION  p= 16.2% w=4.0     contrib=0.77pp [CONFIRMED]
- `01:31:52`     BRA  AT_RISK    p= 25.7% w=1.9     contrib=0.58pp [UNCONFIRMED]
- `01:31:52`     US curve probit: 27.0% (10y-3m 0.73pp)
- `01:31:52`     Sahm: 0.07 — below trigger
## 6. Chokepoint day-two unattended re-read (pending since 3776)

- `01:31:53`   chokepoint generated_at = 2026-07-24T18:35:06.798301+00:00
- `01:31:53`   age = 6.9h
- `01:31:53` ✅   UNATTENDED RUN CONFIRMED — schedule is genuinely armed
- `01:31:53`   ledger 2,482,194 bytes, modified 2026-07-24 18:35:04+00:00
- `01:31:53` ✅ PASS_ALL 23/23
