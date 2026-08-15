# ops 4720 — re-smoke-test justhodl-invest after field fixes

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-08-15T21:17:47+00:00  

## Data

| function_error | invoke_elapsed_s | n_confirmed | n_conflicting | n_gates_pass | n_industry_gates | n_insufficient | n_leading_indicators | n_stock_picks | n_turning | status_code |
|---|---|---|---|---|---|---|---|---|---|---|
| None | 1.5 |  |  |  |  |  |  |  |  | 200 |
|  |  | 0 | 0 | 0 | 0 | 7 | 7 | 0 | 0 |  |

## Log
## Invoke

- `21:17:46` ✅   invoke succeeded in 1.5s
- `21:17:46`   handler response body: {"ok": true, "confirmed": 0, "gates_pass": 0, "picks": 0}
## data/invest.json

- `21:17:47` ✅   5023 bytes, schema=invest/0.1, generated_at=2026-08-15T21:17:46.166665+00:00
- `21:17:47`   per-indicator status:
- `21:17:47`     copper_demand_pulse              INSUFFICIENT_DATA legs 0/0/3
- `21:17:47`     korea_semiconductor_exports      INSUFFICIENT_DATA legs 0/0/3
- `21:17:47`     taiwan_export_orders             INSUFFICIENT_DATA legs 0/0/2
- `21:17:47`     china_credit_impulse             INSUFFICIENT_DATA legs 0/0/2
- `21:17:47`     global_port_freight_pulse        INSUFFICIENT_DATA legs 0/0/2
- `21:17:47`     grid_buildout_pulse              INSUFFICIENT_DATA legs 0/0/2
- `21:17:47`     lumber_housing_pulse             INSUFFICIENT_DATA legs 0/0/2
- `21:17:47`   per-industry gate:
## Verdict

- `21:17:47` ✅ justhodl-invest ran end-to-end against live data with no crash.
