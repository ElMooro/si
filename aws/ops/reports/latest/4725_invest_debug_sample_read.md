# ops 4725 — invoke + read _debug_sample_leg_read

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-08-15T21:44:56+00:00  

## Data

| actual | dig_result | doc_is_none | expected | function_error | invoke_elapsed_s | status_code |
|---|---|---|---|---|---|---|
|  |  |  |  | None | 1.6 | 200 |
| 47.96 | 47.96 | False | 47.96 |  |  |  |

## Log
## Invoke

- `21:44:56` ✅   invoke succeeded in 1.6s
- `21:44:56`   handler response body: {"ok": true, "confirmed": 0, "gates_pass": 0, "picks": 0}
## data/invest.json _debug_sample_leg_read

- `21:44:56`   source = 'fleet:data/asia-leads.json:korea_exports.yoy_pct'
- `21:44:56`   parsed_key = 'data/asia-leads.json'
- `21:44:56`   parsed_path = 'korea_exports.yoy_pct'
- `21:44:56`   doc_is_none = False
- `21:44:56`   doc_top_level_keys = ['disclaimer', 'elapsed_s', 'engine', 'generated_at', 'korea_exports', 'korea_flash', 'korea_flash_tape', 'methodology', 'siblings', 'sources', 'taiwan_exports', 'taiwan_orders', 'version']
- `21:44:56`   dig_result = 47.96
- `21:44:56`   read_leg_value_result = 47.96
## Cross-check against known-good values

- `21:44:56` ✅   MATCHES -- resolves correctly inside the real Lambda too. If tier1 still showed 0 available legs, the bug is downstream of read_leg_value (in run_tier1's loop, LegResult construction, or confirm_indicator), not in fleet_io/S3 access at all.
## Also: current tier1 status counts for context

- `21:44:56`     copper_demand_pulse              INSUFFICIENT_DATA legs 0/0/3
- `21:44:56`     korea_semiconductor_exports      INSUFFICIENT_DATA legs 0/0/3
- `21:44:56`     taiwan_export_orders             INSUFFICIENT_DATA legs 0/0/2
- `21:44:56`     china_credit_impulse             INSUFFICIENT_DATA legs 0/0/2
- `21:44:56`     global_port_freight_pulse        INSUFFICIENT_DATA legs 0/0/2
- `21:44:56`     grid_buildout_pulse              INSUFFICIENT_DATA legs 0/0/2
- `21:44:56`     lumber_housing_pulse             INSUFFICIENT_DATA legs 0/0/2
