# ops 6824 - 13F book 1.6.0 first light 2026-11-17T22:06+00:00

**Status:** success  
**Duration:** 23.9s  
**Finished:** 2026-11-17T22:06:44+00:00  

## Data

| baupost_13f_value | book_bytes | invariant_failures | managers | read_x1000 | successions_cohort | tickers | version | xom_holders |
|---|---|---|---|---|---|---|---|---|
| $5.05B | 2441991 | 0 | 18 | BAUPOST | 50 | 7446 | 1.6.0 | 7 (prior 7) |

## Log
## 1. deployed code + one run

- `22:06:21` justhodl-13f-book: modified 2026-11-17T22:01:48, memory 2048, timeout 300, state Active, update Successful, sha HG9KxMUJn9du
- `22:06:44` invoke 22.0s: {"statusCode": 200, "key": "data/13f-book.json", "bytes": 2441991, "managers": 18, "cohort": 15, "master_resolved": 9110, "master_identities": 9259, "run_seconds": 20.8, "errors": 0}
- `22:06:44` book: version 1.6.0 contract 13f-book.v2 generated 2026-11-17T22:06:22+00:00 run 20.8s | 2441991 bytes (was 2374599) cache max-age=300 | period 2026-09-30 cohort 15/18 errors 0
## 2. value units

- `22:06:44` AQR           current 2026-09-30 dollars x1 (3821 of 3821 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (3680 of 3680 dollars, 0 thousands) | flags []
- `22:06:44` BAUPOST       current 2026-09-30 thousands x1000 (0 of 21 dollars, 21 thousands) | prior 2026-06-30 thousands x1000 (0 of 23 dollars, 23 thousands) | flags ['UNITS_THOUSANDS']
- `22:06:44` BERKSHIRE     current 2026-09-30 dollars x1 (30 of 30 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (29 of 29 dollars, 0 thousands) | flags []
- `22:06:44` BRIDGEWATER   current 2026-09-30 dollars x1 (974 of 974 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (997 of 997 dollars, 0 thousands) | flags []
- `22:06:44` CITADEL       current 2026-09-30 dollars x1 (4311 of 4313 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (4269 of 4270 dollars, 0 thousands) | flags []
- `22:06:44` COATUE        current 2026-09-30 dollars x1 (72 of 72 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (65 of 65 dollars, 0 thousands) | flags []
- `22:06:44` DURATION      current 2026-09-30 dollars x1 (2299 of 2299 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (2297 of 2297 dollars, 0 thousands) | flags []
- `22:06:44` ELLIOTT       current 2026-09-30 dollars x1 (17 of 17 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (17 of 17 dollars, 0 thousands) | flags []
- `22:06:44` GREENLIGHT    current 2023-12-31 not_compared x1 (0 of 0 dollars, 0 thousands) | prior 2023-09-30 not_compared x1 (0 of 0 dollars, 0 thousands) | flags []
- `22:06:44` LONE_PINE     current 2026-09-30 dollars x1 (31 of 31 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (34 of 34 dollars, 0 thousands) | flags []
- `22:06:44` MILLENNIUM    current 2026-09-30 dollars x1 (3705 of 3711 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (3551 of 3554 dollars, 0 thousands) | flags []
- `22:06:44` PERSHING      current 2026-03-31 not_compared x1 (0 of 0 dollars, 0 thousands) | prior 2025-12-31 not_compared x1 (0 of 0 dollars, 0 thousands) | flags []
- `22:06:44` POINT72       current 2026-09-30 dollars x1 (1895 of 1895 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (1898 of 1898 dollars, 0 thousands) | flags []
- `22:06:44` RENAISSANCE   current 2026-09-30 dollars x1 (3014 of 3014 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (2953 of 2954 dollars, 0 thousands) | flags []
- `22:06:44` SCION         current 2025-09-30 not_compared x1 (0 of 0 dollars, 0 thousands) | prior 2025-06-30 not_compared x1 (0 of 0 dollars, 0 thousands) | flags []
- `22:06:44` SOROS         current 2026-09-30 dollars x1 (233 of 233 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (238 of 238 dollars, 0 thousands) | flags []
- `22:06:44` TIGER_GLOBAL  current 2026-09-30 dollars x1 (45 of 45 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (46 of 46 dollars, 0 thousands) | flags []
- `22:06:44` TWO_SIGMA     current 2026-09-30 dollars x1 (3628 of 3629 dollars, 0 thousands) | prior 2026-06-30 dollars x1 (3647 of 3648 dollars, 0 thousands) | flags []
- `22:06:44` **BAUPOST: 13F value $5.05B (prior $5.42B, -6.8%) | equity value $5.05B | cover page as filed 5045853 x1000 = $5.05B, gap $0K, reconciles True | status CURRENT flags ['UNITS_THOUSANDS'] | in cohort True**
- `22:06:44`     top lines: AMZN $932.8M 18.49% · ELV $495.8M 9.83% · QSR $480.1M 9.51% · GPC $389.5M 7.72% · FERG $349.9M 6.93% · GOOG $323.7M 6.41% · WCC $258.4M 5.12% · V $252.0M 4.99%
- `22:06:44`     in the cohort tearsheet: $5.05B = 0.2% of the cohort's 13F value $2492.80B
## 3. every manager's tearsheet

- `22:06:44` CITADEL       2026-09-30 vs 2026-06-30 CURRENT | 13F value $899.60B (prior $875.01B) lines 13843 | new 2429 inc 5940 red 5289 absent 2158 flat 47 reclass 138 | cover ok True (x1)
- `22:06:44` AQR           2026-09-30 vs 2026-06-30 CURRENT | 13F value $337.41B (prior $286.85B) lines 4057 | new 279 inc 2239 red 1385 absent 122 flat 115 reclass 39 | cover ok True (x1)
- `22:06:44` BERKSHIRE     2026-09-30 vs 2026-06-30 CURRENT | 13F value $304.20B (prior $299.25B) lines 30 | new 1 inc 4 red 6 absent 0 flat 19 reclass 0 | cover ok True (x1)
- `22:06:44` MILLENNIUM    2026-09-30 vs 2026-06-30 CURRENT | 13F value $302.25B (prior $276.29B) lines 6103 | new 941 inc 2459 red 2163 absent 634 flat 502 reclass 38 | cover ok True (x1)
- `22:06:44` DURATION      2026-09-30 vs 2026-06-30 CURRENT | 13F value $188.79B (prior $191.39B) lines 2300 | new 50 inc 1031 red 792 absent 51 flat 418 reclass 9 | cover ok True (x1)
- `22:06:44` TWO_SIGMA     2026-09-30 vs 2026-06-30 CURRENT | 13F value $141.91B (prior $138.12B) lines 4830 | new 797 inc 1918 red 1849 absent 984 flat 238 reclass 28 | cover ok True (x1)
- `22:06:44` POINT72       2026-09-30 vs 2026-06-30 CURRENT | 13F value $90.04B (prior $90.68B) lines 3856 | new 838 inc 1414 red 1416 absent 905 flat 167 reclass 21 | cover ok True (x1)
- `22:06:44` RENAISSANCE   2026-09-30 vs 2026-06-30 CURRENT | 13F value $73.42B (prior $72.62B) lines 3151 | new 577 inc 1133 red 1406 absent 566 flat 10 reclass 25 | cover ok True (x1)
- `22:06:44` COATUE        2026-09-30 vs 2026-06-30 CURRENT | 13F value $54.50B (prior $48.63B) lines 73 | new 14 inc 14 red 11 absent 7 flat 34 reclass 0 | cover ok True (x1)
- `22:06:44` ELLIOTT       2026-09-30 vs 2026-06-30 CURRENT | 13F value $28.64B (prior $22.67B) lines 39 | new 11 inc 8 red 7 absent 1 flat 13 reclass 0 | cover ok True (x1)
- `22:06:44` BRIDGEWATER   2026-09-30 vs 2026-06-30 CURRENT | 13F value $22.99B (prior $24.38B) lines 974 | new 163 inc 479 red 324 absent 186 flat 3 reclass 5 | cover ok True (x1)
- `22:06:44` TIGER_GLOBAL  2026-09-30 vs 2026-06-30 CURRENT | 13F value $20.17B (prior $23.98B) lines 45 | new 3 inc 5 red 16 absent 4 flat 20 reclass 1 | cover ok True (x1)
- `22:06:44` LONE_PINE     2026-09-30 vs 2026-06-30 CURRENT | 13F value $15.33B (prior $16.36B) lines 31 | new 9 inc 9 red 12 absent 12 flat 1 reclass 0 | cover ok True (x1)
- `22:06:44` PERSHING      2026-03-31 vs 2025-12-31 NOTICE  | 13F value $13.71B (prior $15.53B) lines 11 | new 1 inc 1 red 6 absent 1 flat 3 reclass 0 | cover ok True (x1)
- `22:06:44` SOROS         2026-09-30 vs 2026-06-30 CURRENT | 13F value $8.51B (prior $8.14B) lines 263 | new 98 inc 46 red 84 absent 101 flat 35 reclass 0 | cover ok True (x1)
- `22:06:44` BAUPOST       2026-09-30 vs 2026-06-30 CURRENT | 13F value $5.05B (prior $5.42B) lines 21 | new 2 inc 6 red 4 absent 4 flat 9 reclass 0 | cover ok True (x1000)
- `22:06:44` GREENLIGHT    2023-12-31 vs 2023-09-30 DATED   | 13F value $2.05B (prior $1.76B) lines 40 | new 7 inc 20 red 4 absent 6 flat 9 reclass 0 | cover ok True (x1)
- `22:06:44` SCION         2025-09-30 vs 2025-06-30 DATED   | 13F value $1.38B (prior $578.3M) lines 8 | new 7 inc 1 red 0 absent 14 flat 0 reclass 0 | cover ok True (x1)
- `22:06:44` cohort tearsheet: 15 members, 13F value $2492.80B (prior $2379.78B), 39616 lines
- `22:06:44` concentrated tearsheet: 6 members, 13F value $427.88B (prior $416.31B), 239 lines
## 4. successions - one security under a new CUSIP or class text

- `22:06:44` **cohort: 50 merged** ({"not compared": 34, "same_cusip": 8, "same_quantity": 1, "unit_price": 7})
- `22:06:44` XOM    30231G102 -> 30233Q108 | unit price x1.191 unit_price | holders 7 (prior 7) | new [] absent [] more ['AQR', 'TWO_SIGMA'] fewer ['BRIDGEWATER', 'CITADEL', 'DURATION', 'MILLENNIUM', 'POINT72'] flat 0 | value $3.02B (prior $3.08B) net est -$650.4M | DISTRIBUTING
- `22:06:44` FWONK  class COM LBTY ONE S C -> COM SER C | unit price x0.971 same_cusip | holders 7 (prior 7) | new [] absent [] more ['AQR', 'CITADEL', 'POINT72'] fewer ['DURATION', 'MILLENNIUM', 'RENAISSANCE', 'TIGER_GLOBAL'] flat 0 | value $1.08B (prior $923.0M) net est $187.8M | CONTESTED
- `22:06:44` TEVA   881624209 -> M8769Q102 | unit price x1.169 unit_price | holders 6 (prior 7) | new [] absent ['POINT72'] more ['AQR', 'CITADEL', 'DURATION'] fewer ['BRIDGEWATER', 'MILLENNIUM', 'TWO_SIGMA'] flat 0 | value $304.1M (prior $345.1M) net est -$93.2M | CONTESTED
- `22:06:44` OKE    682680103 -> 30609A109 | unit price x0.981 unit_price | holders 5 (prior 4) | new ['MILLENNIUM'] absent [] more [] fewer ['AQR', 'CITADEL', 'DURATION'] flat 1 | value $213.9M (prior $224.9M) net est -$6.7M | DISTRIBUTING
- `22:06:44` NXT    class CLASS A COM -> COM | unit price x0.654 same_cusip | holders 6 (prior 7) | new [] absent ['TWO_SIGMA'] more ['BRIDGEWATER', 'CITADEL', 'DURATION', 'MILLENNIUM'] fewer ['AQR', 'POINT72'] flat 0 | value $209.2M (prior $239.1M) net est $52.2M | CONTESTED
- `22:06:44` CLBK   197641103 -> 197914104 | unit price x0.521 QTY NOT COMPARED | holders 6 (prior 5) | new ['MILLENNIUM'] absent [] more [] fewer [] flat 5 | value $136.8M (prior $27.2M) net est $5.9M | ACCUMULATING
- `22:06:44` GTES   G39108108 -> G39104107 | unit price x0.937 unit_price | holders 6 (prior 7) | new [] absent ['TWO_SIGMA'] more ['DURATION', 'MILLENNIUM'] fewer ['AQR', 'CITADEL', 'POINT72', 'RENAISSANCE'] flat 0 | value $126.7M (prior $213.3M) net est -$73.1M | DISTRIBUTING
- `22:06:44` YPF    class SPON ADR CL D -> SPONSORED ADS | unit price x1.102 same_cusip | holders 4 (prior 4) | new [] absent [] more ['BRIDGEWATER', 'MILLENNIUM', 'TWO_SIGMA'] fewer ['CITADEL'] flat 0 | value $95.4M (prior $69.0M) net est $19.4M | ACCUMULATING
- `22:06:44` MRX    G5S37H101 -> G5T40M104 | unit price x1.145 unit_price | holders 6 (prior 3) | new ['CITADEL', 'MILLENNIUM', 'POINT72'] absent [] more ['AQR'] fewer ['RENAISSANCE', 'TWO_SIGMA'] flat 0 | value $81.7M (prior $94.8M) net est -$26.8M | ACCUMULATING
- `22:06:44` FRNM   G70077105 -> 35661P100 | unit price x1.448 QTY NOT COMPARED | holders 5 (prior 2) | new ['POINT72', 'RENAISSANCE', 'TWO_SIGMA'] absent [] more [] fewer [] flat 2 | value $74.6M (prior $856K) net est $51.3M | ACCUMULATING
- `22:06:44` WVE    Y95308105 -> 94352A104 | unit price x0.637 same_quantity | holders 7 (prior 7) | new [] absent [] more ['CITADEL', 'RENAISSANCE', 'TWO_SIGMA'] fewer ['AQR', 'MILLENNIUM', 'POINT72'] flat 1 | value $44.6M (prior $64.5M) net est $3.5M | CONTESTED
- `22:06:44` TGLS   G87264100 -> 87877F103 | unit price x0.759 QTY NOT COMPARED | holders 5 (prior 4) | new ['MILLENNIUM'] absent [] more [] fewer [] flat 4 | value $22.7M (prior $9.0M) net est $8.6M | ACCUMULATING
- `22:06:44` FWONA  class COM LBTY ONE S A -> COM SER A | unit price x0.968 same_cusip | holders 4 (prior 4) | new [] absent [] more ['AQR'] fewer ['CITADEL', 'MILLENNIUM', 'RENAISSANCE'] flat 0 | value $22.0M (prior $27.6M) net est -$4.7M | DISTRIBUTING
- `22:06:44` UROY   91702V101 -> 91703D100 | unit price x1.453 QTY NOT COMPARED | holders 4 (prior 5) | new [] absent ['POINT72'] more [] fewer [] flat 4 | value $21.5M (prior $7.9M) net est -$660K | DISTRIBUTING
- `22:06:44` boards cohort/all: identities 8984 held 7904 | newly present 3563 dropped 3260 increased 4919 decreased 4837 | value $1551.06B prior $1495.44B
- `22:06:44` **concentrated: 1 merged** ({"same_cusip": 1})
- `22:06:44` FWONK  class COM LBTY ONE S C -> COM SER C | unit price x0.971 same_cusip | holders 1 (prior 1) | new [] absent [] more [] fewer ['TIGER_GLOBAL'] flat 0 | value $126.1M (prior $216.8M) net est -$84.5M | DISTRIBUTING
- `22:06:44` boards concentrated/all: identities 194 held 174 | newly present 28 dropped 27 increased 37 decreased 46 | value $415.32B prior $407.93B
- `22:06:44` cohort Energy sector: value $87.55B (prior $76.56B) | in $10.16B out $11.79B net -$1.63B | BALANCED
## 5. the live ticker table + the period snapshot

- `22:06:44` data/13f-tickers.json: 13f-tickers.v1 generated 2026-11-17T22:06:22+00:00 period 2026-09-30 | 7446 tickers (7398 before this run) 1150129 bytes cache max-age=600 | gained 48: ['ALIT', 'BRTX', 'CALC', 'CANG', 'CCCT', 'CCCTU', 'CLBK', 'CLGN', 'CRIS', 'CURX', 'DAICW', 'EPOW', 'FWONA', 'FWONK', 'FWONK 2.25 08/15/27', 'GAME', 'GAUZ', 'GLNDW', 'GOSS', 'GTES', 'INCR', 'IPXG', 'JCSE', 'JZ', 'KIDZ', 'LFT', 'MRX', 'NFE', 'NRDY', 'NXT', 'OFAL', 'OKE', 'ONFO', 'PMI', 'PRPL', 'RBKB', 'RBNE', 'SCNI', 'SGLY', 'SNAL', 'TEVA', 'TOP', 'UROY', 'WVE', 'XCH', 'XOM', 'XPRO', 'YPF'] | lost 0: []
- `22:06:44` XOM    holders 7 (prior 7, delta 0) | value $3.02B (prior $3.08B) | in $59.1M out $709.5M net -$650.4M | bull 2 bear 5 DISTRIBUTING | Energy / Oil & Gas Integrated
- `22:06:44` XPRO   holders 4 (prior 3, delta 1) | value $21.2M (prior $13.6M) | in $6.6M out $0K net $6.6M | bull 3 bear 0 ACCUMULATING | Energy / Oil & Gas Equipment & Services
- `22:06:44` TEVA   holders 6 (prior 7, delta -1) | value $304.1M (prior $345.1M) | in $29.1M out $122.3M net -$93.2M | bull 3 bear 4 CONTESTED | Healthcare / Drug Manufacturers - Specialty & Generic
- `22:06:44` OKE    holders 5 (prior 4, delta 1) | value $213.9M (prior $224.9M) | in $15.2M out $22.0M net -$6.7M | bull 1 bear 3 DISTRIBUTING | Energy / Oil & Gas Midstream
- `22:06:44` GTES   holders 6 (prior 7, delta -1) | value $126.7M (prior $213.3M) | in $13.3M out $86.4M net -$73.1M | bull 2 bear 5 DISTRIBUTING | Industrials / Industrial - Machinery
- `22:06:44` FWONK  holders 7 (prior 7, delta 0) | value $1.08B (prior $923.0M) | in $318.4M out $130.6M net $187.8M | bull 3 bear 4 CONTESTED | Communication Services / Entertainment
- `22:06:44` WVE    holders 7 (prior 7, delta 0) | value $44.6M (prior $64.5M) | in $13.8M out $10.3M net $3.5M | bull 3 bear 3 CONTESTED | Healthcare / Biotechnology
- `22:06:44` NFE    holders 3 (prior 3, delta 0) | value $540K (prior $2.5M) | in $0K out $0K net $0K | bull 0 bear 0 QUIET | Energy / Oil & Gas Midstream
- `22:06:44` period snapshot data/13f-book/periods/2026-09-30.json: {"written": true, "reason": "value basis changed"} | basis 2 identities 9253 bytes 653651 | XOM ['XOM', 'stock', 'equities', 7, 3016139308.0, 7, '30233Q108']
- `22:06:44` ✅ LIVE: 13f-book 1.6.0 - Baupost reads $5.05B and adds up to its cover page; 50 successions are one identity each; Exxon has its row in the ticker table (7 holders, 7 before)
