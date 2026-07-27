# ops 3984 — pure verifier: is the census finally whole?

**Status:** failure  
**Duration:** 125.2s  
**Finished:** 2026-07-27T21:28:08+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | artifacts | artifacts_fresh_48h | artifacts_stale | artifacts_truncated_by_time_budget | conflicts | cron | elapsed_s | families | fred_us10y | fully_attributed | gap_candidates | generated_at | keyed_paths | marker | metric_directory_n | mislabels | page_bytes | page_markers | parse_errors | scalar_paths | schedule_state | source_families |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 153.7 |  |  |  |  | 2026-07-27T21:25:31.650235+00:00 |  | data-census v1.6 ops3985 deterministic |  |  |  |  |  |  |  |  |
| 0.5 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 2016 | 113 | 1 | 38406 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | 250156 |  |  |
|  |  |  |  |  |  |  |  |  |  | 697 |  |  | True |  | 697 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {"OTHER": 404, "FMP": 30, "FRED": 26, "YAHOO": 10, "ECB": 7, "MOEA-TAIWAN": 4, "GNEWS": 1} |  |  |  |  |  |  |  |  |  |  |  |  |  | 7 |
|  |  |  |  |  |  |  |  |  | {"name": "US10Y", "value": -1.0, "live": true, "pulled_from": "fred_alias:DGS10", "engine": "justhodl-domain-barometers", "artifact": "data/domain-barometers.json", "path": "symbols[US10Y].polarity"} |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 1 |  |  |  |  |  | 1 |  |  |  |  | 32 |  |  |  |  |  |  |
|  |  |  |  |  |  | cron(45 12 * * ? *) |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ENABLED |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14181 | 5/5 |  |  |  |  |

## Log
## A. the artifact (short poll for the v1.6 write)

- `21:26:04`   [0] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:26:35`   [1] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:27:06`   [2] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
- `21:27:37`   [3] marker=data-census v1.1 ops3978 4mb-cap — waiting 30s
## B. totals + directory

- `21:28:07`   DIR ADT INC                              = 1784008233.288516 live=True from=figi eng=justhodl-13f-positions
- `21:28:07`   DIR AFLAC INC                            = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
- `21:28:07`   DIR API GROUP CORP                       = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
- `21:28:07`   DIR ASE TECHNOLOGY HLDG CO LTD           = 1784008233.288516 live=True from=sec eng=justhodl-13f-positions
- `21:28:07`   DIR ABBOTT LABORATORIES                  = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
- `21:28:07`   DIR ABERCROMBIE & FITCH CO               = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
## C. by_source — the locating scheme

- `21:28:07`   FRED: BAMLC0A0CM = 1.1862 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:28:07`   FRED: BAMLC0A4CBBB = -1.0 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:28:07`   FRED: BAMLEMPBPUBSICRPIEY = -1.0 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:28:07`   FRED: BAMLH0A0HYM2 = 0.1953 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:28:07`   FRED: BAMLH0A0HYM2SYTW = 0.2463 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:28:07`   FRED: BAMLH0A3HYC = -1.0 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
## D. detectors (first run with a fed keyed walk)

- `21:28:07`   MISLABEL v=-0.25 ['TW', 'US'] ['domain-barometers.json:predictions.asset_classes.us_equity_larg', 'rotation-dashboard.json:layer1_regime.prior.equity_em']
- `21:28:07`   MISLABEL v=1211.0 ['CN', 'WW'] ['global-business-cycle.json:by_country.CHN.history_n', 'digest-2026-06-04.json:message_chars']
- `21:28:07`   MISLABEL v=1.6 ['KR', 'WW'] ['global-business-cycle.json:by_country.KOR.gdp_weight', 'master-ranker.json:top_tickers[AMZN].details.future_intel.r']
- `21:28:07`   MISLABEL v=0.33 ['CN', 'WW'] ['global-business-cycle.json:by_country.CHN.composite_pct', '13f-clone-alpha.json:managers.CITADEL.hit_rate']
- `21:28:07`   MISLABEL v=1.45 ['GB', 'WW'] ['global-business-cycle.json:by_country.GBR.z_5y', 'global-business-cycle.json:by_country.SWE.z_5y']
- `21:28:07`   GAP CNINTR    notes=2 -> ['interbank_rate.latest_pct', 'interbank_rate.change_3m_pp']
## E. schedule + page

- `21:28:08` ✅   artifact is v1.5
- `21:28:08` ✅   written after the v1.6 invoke
- `21:28:08` ✅   >=100 artifacts walked
- `21:28:08` ✅   >=2000 scalar paths
- `21:28:08` ✅   metric directory >=50
- `21:28:08` ✅   >=50 fully attributed (name+source+engine)
- `21:28:08` ✅   keyed-list walk landed
- `21:28:08` ✅   by_source >=5 families
- `21:28:08` ✗   FRED >=40 metrics
- `21:28:08` ✅   US 10Y locatable under FRED
- `21:28:08` ✅   detectors present
- `21:28:08` ✅   schedule ENABLED
- `21:28:08` ✅   page v3 live at edge
- `21:28:08` ✗ FAILED: ['FRED >=40 metrics']
