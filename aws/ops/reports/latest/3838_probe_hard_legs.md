# ops 3838 — PROBE: per-country hard legs for confirmation

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-07-25T01:25:52+00:00  

## Data

| candidates | usable_containers |
|---|---|
| 6 | 4 |

## Log
## ── data/portwatch.json

- `01:25:52`   generated_at=2026-07-24T12:10:50.938521+00:00 top-level=['ok', 'version', 'generated_at', 'chokepoints', 'disruptions', 'errors', 'attribution', 'ref_n', 'daily_layer', 'daily_rows', 'metric_field', 'pids_seen']
- `01:25:52` ✅   'ports' list-of-rows · 89 rows
- `01:25:52`     row fields: ['id', 'name', 'country', 'latest_7d_avg', 'prev_30d_avg', 'baseline_1y', 'z', 'vs_baseline_pct', 'yoy_pct', 'n_days', 'last_date', 'status']
- `01:25:52`     sample: {"id": "port1188", "name": "Shanghai", "country": "China", "latest_7d_avg": 58.7, "prev_30d_avg": 85.1, "baseline_1y": 110.3, "z": -2.43, "vs_baseline_pct": -46.8, "yoy_pct": -50.7, "n_days": 394, "last_date": "2026-07-17", "status": "DISRUPTED"}
- `01:25:52` ✅   'ref_search' list-of-rows · 7 rows
- `01:25:52`     row fields: ['name', 'full', 'country']
- `01:25:52`     sample: {"name": "Jeddah", "full": "Jeddah, Saudi Arabia", "country": "Saudi Arabia"}
- `01:25:52` ✅   'ports_ref_sample' list-of-rows · 14 rows
- `01:25:52`     row fields: ['name', 'country']
- `01:25:52`     sample: {"name": "Shanghai", "country": "China"}
- `01:25:52`     change-like: chokepoints[0].z = -1.59
- `01:25:52`     change-like: chokepoints[0].yoy_pct = -87.8
- `01:25:52`     change-like: ports[0].z = -2.43
- `01:25:52`     change-like: ports[0].yoy_pct = -50.7
- `01:25:52`     change-like: exporters[0].avg_z = -1.53
- `01:25:52`     change-like: worst.z = -1.59
## ── data/trade-nowcast.json

- `01:25:52`   generated_at=2026-07-24T12:50:05.846368+00:00 top-level=['ok', 'version', 'generated_at', 'series', 'errors', 'bdi', 'cpb_wtm', 'rate_pressure', 'verdict', 'plain']
- `01:25:52` ⚠   no country-keyed container found
## ── data/china-liquidity.json

- `01:25:52`   generated_at=2026-07-24T14:30:32.592973+00:00 top-level=['schema_version', 'method', 'generated_at', 'elapsed_s', 'fred_failed', 'series_resolved', 'regime', 'regime_read', 'money', 'credit_impulse', 'interbank_rate', 'currency']
- `01:25:52` ⚠   no country-keyed container found
## ── data/freight-pulse.json

- `01:25:52`   generated_at=2026-07-24T11:50:18.082314+00:00 top-level=['ok', 'version', 'generated_at', 'series', 'errors', 'composite', 'verdict', 'inflections', 'n_live', 'method']
- `01:25:52` ⚠   no country-keyed container found
## ── data/air-cargo.json

- `01:25:52`   generated_at=2026-07-24T10:40:40.873037+00:00 top-level=['ok', 'version', 'generated_at', 'airport', 'errors', 'attribution', 'fetch_via', 'xlsx_bytes', 'rows_parsed', 'cols', 'tonnes', 'tonnes_k']
- `01:25:52` ⚠   no country-keyed container found
## ── data/global-business-cycle.json

- `01:25:52`   generated_at=2026-07-24T12:00:58.752299+00:00 top-level=['schema_version', 'engine_type', 'generated_at', 'elapsed_sec', 'countries_with_fresh_data', 'countries_total', 'methodology', 'by_country', 'aggregate', 'interpretation']
- `01:25:52` ✅   'by_country' dict-keyed-by-code · 34 codes: ['USA', 'CHN', 'JPN', 'DEU', 'IND', 'GBR', 'FRA', 'ITA', 'CAN', 'BRA', 'KOR', 'AUS', 'ESP', 'MEX']
- `01:25:52`     per-country fields: ['iso3', 'iso2', 'yahoo_symbol', 'yahoo_symbol_primary', 'country_name', 'region', 'gdp_weight', 'months_stale', 'phase', 'cli_level', 'composite_pct', 'six_month_change', 'mom_change', 'yoy_change', 'three_month_change', 'dist_200ma_pct']
- `01:25:52`     sample USA: {"iso3": "USA", "iso2": "US", "yahoo_symbol": "^GSPC", "yahoo_symbol_primary": "^GSPC", "country_name": "United States", "region": "North America", "gdp_weight": 25.0, "months_stale": 0, "phase": "EXPANSION", "cli_level": 120, "composite_pct": 139.58, "six_mon
- `01:25:52`     change-like: by_country.USA.yoy_change = 17.413
- `01:25:52`     change-like: by_country.USA.z_5y = 0.3
- `01:25:52`     change-like: by_country.CHN.yoy_change = 8.676
- `01:25:52`     change-like: by_country.CHN.z_5y = 0.27
- `01:25:52`     change-like: by_country.JPN.yoy_change = 62.969
- `01:25:52`     change-like: by_country.JPN.z_5y = 1.97
- `01:25:52`     change-like: by_country.DEU.yoy_change = 2.948
- `01:25:52`     change-like: by_country.DEU.z_5y = -0.8
- `01:25:52`     change-like: by_country.IND.yoy_change = -7.886
- `01:25:52`     change-like: by_country.IND.z_5y = -1.85
- `01:25:52`     change-like: by_country.GBR.yoy_change = 16.837
- `01:25:52`     change-like: by_country.GBR.z_5y = 1.16
## Verdict

- `01:25:52` ✅ 4 usable country-keyed container(s) found — a confirmation leg is buildable; wire ops follows
