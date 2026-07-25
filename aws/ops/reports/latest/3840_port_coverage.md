# ops 3840 — v1.3.1 self-maintaining port->ISO map: global recession ensemble + chokepoint day-two

**Status:** success  
**Duration:** 16.5s  
**Finished:** 2026-07-25T01:39:02+00:00  

## Data

| band | breadth_pct | confirm_counts | excluded | gdp_covered | global_prob | n_countries | oecd_usable | unconfirmed_share_pct |
|---|---|---|---|---|---|---|---|---|
| WATCH — pockets of stress | 35.0 | {"CONFIRMED": 4, "DIVERGENT": 5, "UNCONFIRMED": 24} | 1 | 84.1 | 32.0 | 33 | False | 27.1 |

## Log
## G0. KEY CONTRACT — live producer artifact

- `01:38:45` ✅   by_country: 34 countries, 34 with phase+gdp_weight
- `01:38:45` ✅     key 'phase' present
- `01:38:45` ✅     key 'cli_level' present
- `01:38:45` ✅     key 'gdp_weight' present
- `01:38:45` ✅     key 'six_month_change' present
- `01:38:45` ✅     key 'dist_200ma_pct' present
## 1. Deploy

- `01:38:46` ✅   FRED_API_KEY from justhodl-nowcast-desk
- `01:38:46`   zip: 92324 bytes
## 1. Lambda

- `01:38:46`   Lambda exists — updating
- `01:38:53` ✅   ✓ updated justhodl-global-recession
## 2. Zip-settle

- `01:38:58` ✅   settled after 5s
## 3. Schedule

- `01:38:59` ✅   Scheduler exists (ConflictException = success)
## 4. Invoke

- `01:39:01`   {'statusCode': 200, 'body': '{"ok": true, "global_pct": 32.0, "n": 33}'}
## 5. Verify the aggregation is REAL

- `01:39:01` ✅   global probability present = 32.0%
- `01:39:01` ✅   countries scored >= 15 = 33
- `01:39:01` ✅   GDP coverage > 0.5 = 84.1
- `01:39:01` ✅   weighted mean inside country range min 8.2 <= 32.0 <= max 72.3
- `01:39:01` ✅   contributions reconcile to global sum 31.96 vs 32.0
- `01:39:01` ✅   no country at 0 or 100 (nothing is certain) 
- `01:39:01` ✅   not saturated at ceiling (<=2 countries >=95%) = []
- `01:39:01` ✅   not saturated at floor (<=2 countries <=5%) = []
- `01:39:01` ✅   dispersion is real (spread >= 25pp) = 64.1pp
- `01:39:01` ✅   excluded-not-imputed disclosed 
- `01:39:01` ✅   NOT-MacroMicro disclosure present 
- `01:39:01` ✅   US cross-check reported separately 
- `01:39:01` ✅   breadth published = 35.0% of covered GDP at risk
- `01:39:01` ✅   confirmation block present 
- `01:39:01` ✅   every country carries a confirmation state = {'CONFIRMED': 4, 'DIVERGENT': 5, 'UNCONFIRMED': 24} vs 33 countries
- `01:39:01` ✅   unconfirmed exposure is quantified = 27.1% of headline
- `01:39:01` ✅   OECD staleness is decided, not assumed usable=False period=2024-01-01 age=30mo
- `01:39:01` ✅   no confirmation off an implausible index value = []
- `01:39:01` ✅   coverage verdict published = PARTIAL — only 4 of 33 countries have an independent check; the rest rest on equity moment
- `01:39:01` ✅   CONFIRMED count > 0 (hard legs are working) confirmed=4 divergent=5
- `01:39:01`     ports mapped to 14 countries
- `01:39:01` ⚠     still unmapped port countries: {'taiwan province of china': 6, 'qatar': 4, 'peru': 2, 'panama': 1, 'costa rica': 1, 'dominican republic': 1, 'sri lanka': 1, 'the netherlands': 1}
- `01:39:01` ✅   port coverage >= 8 countries = 14
- `01:39:01` ✅   at least 6 countries on a hard leg = ['CHN', 'USA', 'JPN', 'DEU', 'AUS', 'KOR', 'CHL', 'ESP', 'FIN']
- `01:39:01` ✅   unconfirmed share fell below 84.3% (v1.2.1 baseline) = 27.1%
- `01:39:01`     CHN: CONFIRMED p=72.3% {"source": "port throughput (physical)", "n_ports": 5, "median_yoy_pct": -31.8, "worst_port": "Shanghai", "note": "physical trade volume \u2014 fully 
- `01:39:01`     USA: DIVERGENT p=16.8% {"source": "port throughput (physical)", "n_ports": 4, "median_yoy_pct": -7.3, "worst_port": "Los Angeles-Long Beach", "note": "physical trade volume 
- `01:39:01`     JPN: DIVERGENT p=16.5% {"source": "port throughput (physical)", "n_ports": 5, "median_yoy_pct": -5.0, "worst_port": "Tokyo", "note": "physical trade volume \u2014 fully inde
- `01:39:01`     DEU: CONFIRMED p=16.2% {"source": "port throughput (physical)", "n_ports": 3, "median_yoy_pct": 2.5, "worst_port": "Hamburg", "note": "physical trade volume \u2014 fully ind
- `01:39:01`     AUS: DIVERGENT p=29.2% {"source": "port throughput (physical)", "n_ports": 11, "median_yoy_pct": -7.9, "worst_port": "Abbot Point", "note": "physical trade volume \u2014 ful
- `01:39:01`     KOR: DIVERGENT p=15.4% {"source": "port throughput (physical)", "n_ports": 3, "median_yoy_pct": -2.7, "worst_port": "Busan", "note": "physical trade volume \u2014 fully inde
- `01:39:01`     CHL: CONFIRMED p=48.4% {"source": "port throughput (physical)", "n_ports": 11, "median_yoy_pct": -29.4, "worst_port": "Lirquen", "note": "physical trade volume \u2014 fully 
- `01:39:01`     ESP: CONFIRMED p=8.2% {"source": "port throughput (physical)", "n_ports": 2, "median_yoy_pct": 0.0, "worst_port": "Valencia", "note": "physical trade volume \u2014 fully in
- `01:39:01` ✅   dampening actually applied where unbacked at least one country pulled toward neutral, or all confirmed
- `01:39:01`     CHN: CONFIRMED — p=72.3% detail={"source": "port throughput (physical)", "n_ports": 5, "median_yoy_pct": -31.8, "worst_port": "Shanghai", "note": "physical trade volume \u2
- `01:39:01`     IND: UNCONFIRMED — p=64.0% detail={"note": "no independent hard-data leg available \u2014 this country rests on equity momentum alone"}
- `01:39:01`   ── top GDP contributors ──
- `01:39:01`     CHN  RECESSION  p= 72.3% w=18.0    contrib=15.47pp [CONFIRMED]
- `01:39:01`     USA  EXPANSION  p= 16.8% w=25.0    contrib=4.99pp [DIVERGENT]
- `01:39:01`     IND  RECESSION  p= 64.0% w=3.6     contrib=2.74pp [UNCONFIRMED]
- `01:39:01`     IDN  RECESSION  p= 68.8% w=1.3     contrib=1.06pp [UNCONFIRMED]
- `01:39:01`     JPN  EXPANSION  p= 16.5% w=4.2     contrib=0.82pp [DIVERGENT]
- `01:39:01`     MEX  AT_RISK    p= 44.2% w=1.5     contrib=0.79pp [UNCONFIRMED]
- `01:39:01`     DEU  EXPANSION  p= 16.2% w=4.0     contrib=0.77pp [CONFIRMED]
- `01:39:01`     BRA  AT_RISK    p= 25.7% w=1.9     contrib=0.58pp [UNCONFIRMED]
- `01:39:01`     US curve probit: 27.0% (10y-3m 0.73pp)
- `01:39:01`     Sahm: 0.07 — below trigger
## 6. Chokepoint day-two unattended re-read (pending since 3776)

- `01:39:02`   chokepoint generated_at = 2026-07-24T18:35:06.798301+00:00
- `01:39:02`   age = 7.1h
- `01:39:02` ✅   UNATTENDED RUN CONFIRMED — schedule is genuinely armed
- `01:39:02`   ledger 2,482,194 bytes, modified 2026-07-24 18:35:04+00:00
- `01:39:02` ✅ PASS_ALL 24/24
