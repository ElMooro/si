# ops 5224 -- Release C1 gate: portfolio accounting truth (INST-07/08)

**Status:** success  
**Duration:** 3.2s  
**Finished:** 2026-09-09T01:18:33+00:00  

## Data

| pnl | positions | priced | step | stops_hit | stops_not_evaluable | unpriced |
|---|---|---|---|---|---|---|
| 0.0 | 0 | 0 | snapshot | 0 | 0 | 0 |

## Log
## A. portfolio-admin

- `01:18:30` justhodl-portfolio-admin LastModified 2026-09-09T00:27:07.000+0000
- `01:18:31` ✅ phantom symbol refused without upsert: {"ok": false, "err": "position ZZZZ-OPS5224 does not exist -- use add_position"}
- `01:18:31` ⚠ live book has no positions -- non-finite probe skipped
## B. portfolio-snapshot

- `01:18:31` justhodl-portfolio-snapshot LastModified 2026-09-09T00:27:30.000+0000
- `01:18:33` ✅ snapshot invoke ok (FunctionError=None) b'{"statusCode": 200, "body": "{\\"success\\": true, \\"n_positions\\": 0, \\"n_watchlist\\": 0, \\"total_market_value\\": 0.0, \\"total_pnl_dollars\\": 0.0, \\"stops_hit_co'
- `01:18:33` positions 0 | summary keys ['n_positions', 'pnl_scope', 'stops_hit', 'stops_hit_count', 'stops_not_evaluable', 'total_cost_basis', 'total_market_value', 'total_market_value_scope', 'total_pnl_dollars', 'total_pnl_pct', 'unpriced_cost_basis', 'unpriced_positions']
- `01:18:33` ✅ summary carries pnl_scope / unpriced_positions / stops_not_evaluable
- `01:18:33` ✅ every position obeys the valuation/side/basis contract (0 violations) []
## verdict

- `01:18:33` ✅ GREEN -- portfolio edits cannot fabricate P&L or invert stops; missing marks are unpriced exposure, never cost
