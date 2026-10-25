# ops 6647 - justhodl-polygon-bank: declare, first run + 1 kick, data.html card, rerouted engines

**Status:** success  
**Duration:** 770.4s  
**Finished:** 2026-10-25T16:56:21+00:00  

## Data

| calls | http | key_status | kick | phase | puts | retries | seconds | unchanged | universes |
|---|---|---|---|---|---|---|---|---|---|
| 3032 | {"ok": 383, "err:AttributeError": 1, "http403": 66, "http429": 624, "empty": 60} | ok | 1 | BACKFILL | 51 | 1898 | 760.8 | 0 | {"us_tickers": 13262, "us_etfs": 5796, "chart": 330, "asset_classes": 5, "snapshot_indices": 14} |

## Log
## 1. function + schedule

- `16:43:31` justhodl-polygon-bank: runtime python3.12, memory 1536, timeout 900, state Active, modified 2026-10-25T16:31:31
- `16:43:31` env keys: ['POLYGON_API_KEY', 'POLYGON_BANK_BUDGET_S', 'POLYGON_BANK_RPS', 'POLYGON_BANK_WORKERS', 'S3_BUCKET'] (values never printed)
- `16:43:31` schedule justhodl-polygon-bank-daily: cron(41 22 * * ? *) ENABLED tz UTC target justhodl-polygon-bank
## 2. deployed schedule manifest := committed (additions-only)

- `16:43:31` S3 1029 entries, repo 1030; added [('scheduler', 'justhodl-polygon-bank-daily')] removed 0 changed 0
- `16:43:32` ✅ manifest uploaded (+1), backup backups/schedule-manifest/20261025T164331Z/
## 3. first run + backfill kicks

- `16:43:32` kick 1: async invoke 202 at 16:43:32
- `16:56:20` endpoints served: ['dividends', 'etf-global-fund-flows', 'market-status-now', 'snapshot-gainers', 'snapshot-losers', 'snapshot-stocks-all', 'tickers-crypto', 'tickers-fx', 'tickers-indices', 'tickers-otc', 'tickers-stocks-active', 'tickers-stocks-inactive']
- `16:56:20` endpoints refused (recorded, not guessed): [['grouped-crypto', 'http429'], ['grouped-fx', 'http429'], ['market-holidays', 'err:AttributeError'], ['snapshot-indices', 'http403']]
- `16:56:20` universes: {'us_tickers': 13262, 'us_etfs': 5796, 'chart': 330, 'asset_classes': 5, 'snapshot_indices': 14}
- `16:56:20` per-dataset (first 40): {"market-status-now": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "market-holidays": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": "err:AttributeError"}, "ticker-types": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "exchanges": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "conditions": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "tickers-stocks-active": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "tickers-stocks-inactive": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "tickers-otc": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "tickers-crypto": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "tickers-fx": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "tickers-indices": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "snapshot-stocks-all": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "snapshot-gainers": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "snapshot-losers": {"objects": 2, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "snapshot-indices": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": "http403"}, "grouped-crypto": {"objects": 0, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "http429"}, "grouped-fx": {"objects": 0, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "http429"}, "dividends": {"objects": 1, "lap": 0, "cursor": 0, "universe_size": null, "last_verdict": "ok"}, "splits": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "ipos": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "short-interest": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "short-volume": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "ticker-details": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "ticker-events": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "related-companies": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "options-contracts": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "options-contracts-expired": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "minute-bars": {"objects": 0, "lap": 0, "cursor": null, "universe_size": null, "last_verdict": null}, "etf-global-fund-flows": {"objects": 30, "lap": 0, "cursor": 30, "univ
- `16:56:20` ✅ polygon bank: 51 objects, 26.6 MB, 12/31 datasets banked, phase BACKFILL (fmp-backfill.yml + the daily 22:41 UTC run continue the backfill)
## 4. data.html card (extra-providers.json, add-only) + catalog refresh

- `16:56:20` ✅ extra-providers.json += polygon-bank (13 entries)
- `16:56:20` justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)
## 5. the two engines rerouted off the not-entitled Polygon trade/quote endpoints are deployed

- `16:56:21` justhodl-implied-prob modified 2026-10-25T16:31:59 (snapshot route replaces /v2/last/trade)
- `16:56:21` justhodl-trade-journal modified 2026-10-25T16:32:23 (snapshot route replaces /v2/last/trade)
- `16:56:21` ✅ ops 6647 complete
