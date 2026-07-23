# ops 3771 — carry ev_sales/pe into cap_rows (catch-up leg)

**Status:** success  
**Duration:** 44.6s  
**Finished:** 2026-07-23T18:37:49+00:00  

## Data

| catchup_max | catchup_min | catchup_negative | catchup_positive | industries_grouped | invoke_seconds | invoke_status | ledger_note | scored | version | with_catchup |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 23.6 | 200 |  |  |  |  |
|  |  |  |  | 141 |  |  | accretes each run | 1771 | 4.0.1 | 1653 |
| 300.0 | -100.0 | 972 | 581 |  |  |  |  |  |  |  |

## Log
## G0 — prove producer emits, consumer drops

- `18:37:04` ✅ G0.producer_computes :: evaluate() computes ev_sales
- `18:37:04` ✅ G0.producer_returns :: evaluate() returns ev_sales/pe
- `18:37:04` ✅ G0.consumer_drops :: cap_rows does NOT carry ev_sales — the confirmed root cause
- `18:37:04` ✅ G0.consumer_reads :: v4 catch-up block reads ev_sales from cap_rows
## Fix — carry the fields

- `18:37:04` ✅ FIX.anchor :: cap_rows tail anchor unique
- `18:37:04` ✅ ev_sales/pe now carried into cap_rows (v4.0.1)
## Deploy

- `18:37:04`   zip: 99305 bytes
## 1. Lambda

- `18:37:04`   Lambda exists — updating
- `18:37:09` ✅   ✓ updated justhodl-chokepoint
- `18:37:25` ✅ settled attempt 1
- `18:37:25` ✅ DEPLOY.settled :: v4.0.1 live
## Invoke + verify the leg is alive

- `18:37:49` ✅ LIVE.v401 :: version=4.0.1
- `18:37:49` ✅ FIX.catchup_alive :: catchup populated on 1653 names (was 0)
- `18:37:49` ✅ SANITY.two_sided :: 581 below / 972 above industry median — not a one-way artifact
- `18:37:49` ✅ SANITY.median_sane :: median catchup -13.8% is plausible
## TOP UNDERVALUED — all industries (with catch-up)

- `18:37:49`   CRUS   Cirrus Logic, Inc.     Semiconductors           score=74.7   gap=+28.7 catchup=    177% (EV/S+P/E) legs=3 STRUCTURALLY_UNDERVALUED
- `18:37:49`   ATAT   Atour Lifestyle Holdin Travel Lodging           score=73.4   gap=+55.6 catchup=    300% (EV/S+P/E) legs=2 WATCH
- `18:37:49`   OMAB   Grupo Aeroportuario de Airlines, Airports & Air score=71.6   gap=+43.9 catchup=    148% (EV/S+P/E) legs=2 WATCH
- `18:37:49`   VNT    Vontier Corporation    Industrial - Machinery   score=67.7   gap=+32.6 catchup=    188% (EV/S+P/E) legs=2 WATCH
- `18:37:49`   TEO    Telecom Argentina S.A. Telecommunications Servi score=66.8   gap=+35.2 catchup=    300% (EV/S) legs=2 WATCH
- `18:37:49`   NVEC   NVE Corporation        Semiconductors           score=65.5   gap=+64.8 catchup=    -61% (EV/S+P/E) legs=3 STRUCTURALLY_UNDERVALUED
- `18:37:49`   GDDY   GoDaddy Inc.           Software - Infrastructur score=64.9   gap=+13.1 catchup=    142% (EV/S+P/E) legs=3 WATCH
- `18:37:49`   TGS    Transportadora de Gas  Oil & Gas Integrated     score=64.3   gap=+61.7 catchup=    300% (EV/S+P/E) legs=1 WATCH
- `18:37:49`   PEGA   Pegasystems Inc.       Software - Application   score=62.5   gap=+17.3 catchup=     90% (EV/S+P/E) legs=2 WATCH
- `18:37:49`   FDS    FactSet Research Syste Financial - Data & Stock score=62.4   gap=+42.0 catchup=     72% (EV/S+P/E) legs=3 STRUCTURALLY_UNDERVALUED
- `18:37:49`   AOS    A. O. Smith Corporatio Industrial - Machinery   score=61.5   gap=+37.0 catchup=     84% (EV/S+P/E) legs=3 STRUCTURALLY_UNDERVALUED
- `18:37:49`   ANF    Abercrombie & Fitch Co Apparel - Retail         score=61.1   gap= +7.3 catchup=    139% (EV/S+P/E) legs=2 WATCH
- `18:37:49`   PHI    PLDT Inc.              Telecommunications Servi score=59.5   gap=+27.0 catchup=    300% (EV/S+P/E) legs=1 WATCH
- `18:37:49`   BIPC   Brookfield Infrastruct Regulated Gas            score=59.2   gap=+43.6 catchup=     80% (EV/S) legs=2 WATCH
- `18:37:49`   MORN   Morningstar, Inc.      Financial - Data & Stock score=58.6   gap=+30.0 catchup=     64% (EV/S+P/E) legs=3 STRUCTURALLY_UNDERVALUED
## BY INDUSTRY — median catch-up now populated

- `18:37:49`   Independent Power Producers      n=6   LOW    med_gap=  +6.7 med_catchup=      91% undervalued=0
- `18:37:49`   Insurance - Diversified          n=16  MEDIUM med_gap=  +0.6 med_catchup=     -19% undervalued=0
- `18:37:49`   Medical - Healthcare Plans       n=9   MEDIUM med_gap=  -2.8 med_catchup=     -19% undervalued=0
- `18:37:49`   Insurance - Life                 n=18  MEDIUM med_gap=  -6.5 med_catchup=     -26% undervalued=0
- `18:37:49`   Manufacturing - Tools & Accessor n=6   LOW    med_gap=  -6.6 med_catchup=       0% undervalued=0
- `18:37:49`   Financial - Data & Stock Exchang n=10  MEDIUM med_gap=  -8.0 med_catchup=      -4% undervalued=3
- `18:37:49`   Drug Manufacturers - General     n=17  MEDIUM med_gap=  -8.3 med_catchup=      -5% undervalued=0
- `18:37:49`   Travel Services                  n=10  MEDIUM med_gap=  -8.4 med_catchup=       0% undervalued=0
- `18:37:49`   Semiconductors                   n=74  HIGH   med_gap=  -9.4 med_catchup=     -23% undervalued=2
- `18:37:49`   Banks                            n=6   LOW    med_gap=  -9.6 med_catchup=       0% undervalued=1
- `18:37:49`   REIT - Specialty                 n=11  MEDIUM med_gap= -11.0 med_catchup=     -11% undervalued=0
- `18:37:49`   Banks - Diversified              n=22  HIGH   med_gap= -11.6 med_catchup=      -3% undervalued=0
- `18:37:49`   Airlines, Airports & Air Service n=12  MEDIUM med_gap= -11.6 med_catchup=     -15% undervalued=0
- `18:37:49`   Regulated Gas                    n=10  MEDIUM med_gap= -12.9 med_catchup=      -3% undervalued=0
## Additive contract

- `18:37:49` ✅ ADDITIVE.structural_names :: present
- `18:37:49` ✅ ADDITIVE.industry_leaders :: present
- `18:37:49` ✅ ADDITIVE.all_chokepoints :: present
- `18:37:49` ✅ ADDITIVE.hidden_chokepoint_book :: present
- `18:37:49` ✅ ADDITIVE.cheap_chokepoint_book :: present
## VERDICT

- `18:37:49` ✅ PASS_ALL — catch-up leg alive; all four asks now live in the feed
- `18:37:49` NEXT: page rewrite — leaderboard on top, industry-first layout, catch-up column.
