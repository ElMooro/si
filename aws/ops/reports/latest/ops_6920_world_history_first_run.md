# ops 6920 - the World tab's histories, first compile by the deployed engine, 2026-11-22T18:33+00:00

**Status:** success  
**Duration:** 28.1s  
**Finished:** 2026-11-22T18:34:14+00:00  

## Data

| cds | cds_reported | countries | failed_checks | measures | publications_folded | seconds | series_asked | yield | yield_reported |
|---|---|---|---|---|---|---|---|---|---|
| 25 | 38 | 44 | 0 | 137 | 71 | 25 | 30 | 30 | 44 |

## Log
## 1. Guard

- `18:33:47` justhodl-ciss-stress updated 2026-11-22T18:32:48 | as in this checkout: {'world_history.py': True, 'sovereign_desk.py': True, 'lambda_function.py': True} | timeout 900 s
- `18:33:48` before: data/ciss-sovereign-world.json does not exist yet
- `18:33:48` before: data/ciss-research/sovereign/world-state.json does not exist yet
## 2. One invoke, the world histories only

- `18:34:13` invoke: HTTP 200, function error None, 25s | {"world": {"published": true, "generated_at": "2026-11-22T18:33:49.410226+00:00", "coverage": {"countries": 44, "measures": 137, "by_source": {"oecd": 25, "reported": 82, "dtcc": 25, "ecb": 5}, "errors": 0, "status": "measured"}, "sets": {"yield": 30, "cds": 25, "yield_reported": 44, "cds_reported": 38}, "publications_folded": 71, "series_asked": 30, "seconds": 24.5}}
## 3. Read back

- `18:34:14` document 726085 bytes written 18:34:14 | record 485220 bytes written 18:34:13
- `18:34:14` contract sovereign-world.v1 1.0.0 | generated 2026-11-22T18:33:49.410226+00:00 | coverage {"by_source": {"dtcc": 25, "ecb": 5, "oecd": 25, "reported": 82}, "countries": 44, "errors": 0, "measures": 137, "status": "measured"} | errors {}
- `18:34:14` the document carries its own digest: True
- `18:34:14` sets: {"yield": 30, "cds": 25, "yield_reported": 44, "cds_reported": 38} | sources {"dtcc": 25, "ecb": 5, "oecd": 25, "reported": 82}
- `18:34:14`   yield           not held for: Turkey, Brazil, Colombia, Vietnam, Peru, Indonesia, India, Thailand, Philippines, Taiwan, Hong Kong, Malaysia, China, Singapore
- `18:34:14`   cds             not held for: Russia, Poland, Czech Republic, Vietnam, India, Hungary, Greece, Taiwan, New Zealand, Canada, Hong Kong, Portugal, Finland, Ireland, Singapore, Sweden, Denmark, Switzerland, Norway
- `18:34:14`   yield_reported  not held for: nobody
- `18:34:14`   cds_reported    not held for: Russia, Czech Republic, Vietnam, Peru, Thailand, Taiwan
- `18:34:14` every series against its measure: 137 measures, 0 disagree []
- `18:34:14` the table's figures of the day at the end of the reported series: 82 checked, 0 differ []
- `18:34:14`   WY_UNITED_STATES  oecd | 1953-04-30 .. 2026-10-31 | 883 points | last 5.13 | current
- `18:34:14`   WY_GERMANY        oecd | 1956-05-31 .. 2026-10-31 | 846 points | last 3.4159 | current
- `18:34:14`   WY_FRANCE         oecd | 1960-01-31 .. 2026-10-31 | 802 points | last 4.89 | current
- `18:34:14`   WC_TURKEY         dtcc | 2024-09-03 .. 2026-11-20 | 526 points | last 260.38 | current
- `18:34:14`   WC_BRAZIL         dtcc | 2024-09-03 .. 2026-11-20 | 542 points | last 179.5 | current
- `18:34:14`   RY_TURKEY         reported | 2026-09-26 .. 2026-11-22 | 53 points | last 33.55 | current
- `18:34:14`   RC_TURKEY         reported | 2026-09-26 .. 2026-11-22 | 53 points | last 244.51 | current
- `18:34:14`   RY_VIETNAM        reported | 2026-09-26 .. 2026-11-22 | 53 points | last 4.568 | current
- `18:34:14` the warehouse, the desk document, the world table and the CDS desk's file are as before: True
- `18:34:14` ✅ the deployed engine published the World tab's histories; they agree with the table and with themselves, and nothing else was touched
