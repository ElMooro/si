# ops 3984 — pure verifier: is the census finally whole?

**Status:** failure  
**Duration:** 783.5s  
**Finished:** 2026-07-27T21:50:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_min | artifacts | artifacts_fresh_48h | artifacts_stale | artifacts_truncated_by_time_budget | conflicts | cron | elapsed_s | families | fred_us10y | fully_attributed | gap_candidates | generated_at | keyed_paths | marker | metric_directory_n | mislabels | page_bytes | page_markers | parse_errors | scalar_paths | schedule_state | source_families |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 148.1 |  |  |  |  | 2026-07-27T21:30:43.437249+00:00 |  | data-census v1.7 ops3986 vault-full |  |  |  |  |  |  |  |  |
| 6.7 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 2016 | 112 | 1 | 38408 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | 250289 |  |  |
|  |  |  |  |  |  |  |  |  |  | 697 |  |  | True |  | 697 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {"OTHER": 404, "FMP": 30, "FRED": 26, "YAHOO": 10, "ECB": 7, "MOEA-TAIWAN": 4, "GNEWS": 1, "FLEET-INTERNAL": 1} |  |  |  |  |  |  |  |  |  |  |  |  |  | 8 |
|  |  |  |  |  |  |  |  |  | {"name": "US10Y", "value": -1.0, "live": true, "pulled_from": "fred_alias:DGS10", "engine": "justhodl-domain-barometers", "artifact": "data/domain-barometers.json", "path": "symbols[US10Y].polarity"} |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 1 |  |  |  |  |  | 1 |  |  |  |  | 33 |  |  |  |  |  |  |
|  |  |  |  |  |  | cron(45 12 * * ? *) |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ENABLED |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14183 | 5/5 |  |  |  |  |

## Log
## A. the artifact (short poll for the v1.6 write)

- `21:37:26`   [0] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:37:56`   [1] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:38:26`   [2] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:38:56`   [3] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:39:26`   [4] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:39:56`   [5] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:40:26`   [6] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:40:57`   [7] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:41:27`   [8] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:41:57`   [9] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:42:27`   [10] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:42:57`   [11] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:43:27`   [12] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:43:57`   [13] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:44:28`   [14] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:44:58`   [15] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:45:28`   [16] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:45:58`   [17] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:46:28`   [18] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:46:58`   [19] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:47:28`   [20] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:47:58`   [21] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:48:29`   [22] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:48:59`   [23] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:49:29`   [24] marker=data-census v1.7 ops3986 vault-full — waiting 30s
- `21:49:59`   [25] marker=data-census v1.7 ops3986 vault-full — waiting 30s
## B. totals + directory

- `21:50:29`   DIR ADT INC                              = 1784008233.288516 live=True from=figi eng=justhodl-13f-positions
- `21:50:29`   DIR AFLAC INC                            = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
- `21:50:29`   DIR API GROUP CORP                       = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
- `21:50:29`   DIR ASE TECHNOLOGY HLDG CO LTD           = 1784008233.288516 live=True from=sec eng=justhodl-13f-positions
- `21:50:29`   DIR ABBOTT LABORATORIES                  = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
- `21:50:29`   DIR ABERCROMBIE & FITCH CO               = 1783985607.781589 live=True from=sec eng=justhodl-13f-positions
## C. by_source — the locating scheme

- `21:50:29`   FRED: BAMLC0A0CM = 1.1862 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:50:29`   FRED: BAMLC0A4CBBB = -1.0 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:50:29`   FRED: BAMLEMPBPUBSICRPIEY = -1.0 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:50:29`   FRED: BAMLH0A0HYM2 = 0.1953 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:50:29`   FRED: BAMLH0A0HYM2SYTW = 0.2463 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
- `21:50:29`   FRED: BAMLH0A3HYC = -1.0 live=True pulled from fred_2nd_chance through justhodl-domain-barometers
## D. detectors (first run with a fed keyed walk)

- `21:50:29`   MISLABEL v=-0.25 ['TW', 'US'] ['domain-barometers.json:predictions.asset_classes.us_equity_larg', 'rotation-dashboard.json:layer1_regime.prior.equity_em']
- `21:50:29`   MISLABEL v=1211.0 ['CN', 'WW'] ['global-business-cycle.json:by_country.CHN.history_n', 'digest-2026-06-04.json:message_chars']
- `21:50:29`   MISLABEL v=1.6 ['KR', 'WW'] ['global-business-cycle.json:by_country.KOR.gdp_weight', 'master-ranker.json:top_tickers[AMZN].details.future_intel.r']
- `21:50:29`   MISLABEL v=0.33 ['CN', 'WW'] ['global-business-cycle.json:by_country.CHN.composite_pct', '13f-clone-alpha.json:managers.CITADEL.hit_rate']
- `21:50:29`   MISLABEL v=1.45 ['GB', 'WW'] ['global-business-cycle.json:by_country.GBR.z_5y', 'global-business-cycle.json:by_country.SWE.z_5y']
- `21:50:29`   GAP CNINTR    notes=2 -> ['interbank_rate.latest_pct', 'interbank_rate.change_3m_pp']
## E. schedule + page

- `21:50:29` ✗   artifact is v1.5
- `21:50:29` ✗   written after the v1.6 invoke
- `21:50:29` ✅   >=100 artifacts walked
- `21:50:29` ✅   >=2000 scalar paths
- `21:50:29` ✅   metric directory >=50
- `21:50:29` ✅   >=50 fully attributed (name+source+engine)
- `21:50:29` ✅   keyed-list walk landed
- `21:50:29` ✅   by_source >=5 families
- `21:50:29` ✗   FRED >=40 metrics
- `21:50:29` ✅   US 10Y locatable under FRED
- `21:50:29` ✅   US10Y shows the YIELD not an internal (0<v<25)
- `21:50:29` ✗   US10Y pulled from DGS10 through the vault engine
- `21:50:29` ✗   no tried_at junk in the directory
- `21:50:29` ✗   FRED >=150 after the vault-full walk
- `21:50:29` ✅   detectors present
- `21:50:29` ✅   schedule ENABLED
- `21:50:29` ✅   page v3 live at edge
- `21:50:29` ✗ FAILED: ['artifact is v1.5', 'written after the v1.6 invoke', 'FRED >=40 metrics', 'US10Y pulled from DGS10 through the vault engine', 'no tried_at junk in the directory', 'FRED >=150 after the vault-full walk']
