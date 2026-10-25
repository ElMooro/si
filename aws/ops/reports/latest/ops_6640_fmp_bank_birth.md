# ops 6640 - justhodl-fmp-bank: declare, first run, backfill kicks, data.html card, chain proof

**Status:** success  
**Duration:** 1507.8s  
**Finished:** 2026-10-25T16:26:22+00:00  

## Data

| calls | dated_pending | eod_cursor | key_status | kick | mb | objects | phase | retries | seconds |
|---|---|---|---|---|---|---|---|---|---|
| 171 | 1881 | 2000-01-01 | ok | 1 | 0.0 | None | BACKFILL | 103 | 740.9 |
| 190 | 1821 | 2000-01-01 | ok | 2 | 0.0 | None | BACKFILL | 113 | 735.1 |

## Log
## 1. function + schedule

- `16:01:14` justhodl-fmp-bank: runtime python3.12, memory 2048, timeout 900, state Active, modified 2026-10-25T15:38:50
- `16:01:14` env keys: ['FMP_BANK_BUDGET_S', 'FMP_EOD_FLOOR', 'FMP_KEY', 'FMP_YEAR_FLOOR', 'S3_BUCKET'] (values never printed)
- `16:01:15` schedule justhodl-fmp-bank-daily: cron(13 23 * * ? *) ENABLED tz UTC target justhodl-fmp-bank
## 2. deployed schedule manifest := committed (additions-only)

- `16:01:15` S3 1028 entries, repo 1028; added [] removed 0 changed 0
- `16:01:15` ✅ manifest already equal
## 3. first run + backfill kicks

- `16:01:16` kick 1: async invoke 202 at 16:01:16
- `16:13:48` kick 2: async invoke 202 at 16:13:48
- `16:26:20` endpoints served: ['balance-sheet-statement-bulk', 'balance-sheet-statement-growth-bulk', 'cash-flow-statement-bulk', 'cash-flow-statement-growth-bulk', 'dcf-bulk', 'earnings-surprises-bulk', 'etf-holder-bulk?part=0', 'etf-holder-bulk?part=1', 'etf-holder-bulk?part=2', 'income-statement-bulk', 'income-statement-growth-bulk', 'key-metrics-bulk', 'key-metrics-ttm-bulk', 'peers-bulk', 'price-target-summary-bulk', 'profile-bulk?part=0', 'profile-bulk?part=1', 'profile-bulk?part=2', 'profile-bulk?part=3', 'rating-bulk', 'ratios-bulk', 'ratios-ttm-bulk', 'scores-bulk', 'upgrades-downgrades-consensus-bulk']
- `16:26:20` endpoints NOT served (recorded, not guessed): [('etf-holder-bulk?part=3', 'err:too_large'), ('financial-growth-bulk', 'http404'), ('profile-bulk?part=4', 'http400')]
- `16:26:20` ✅ bank: 145 objects, 1700.8 MB, phase BACKFILL, EOD cursor 2000-01-01, dated pending 1821 (the daily 23:13 UTC run continues the backfill)
## 4. data.html card (extra-providers.json, add-only) + catalog refresh

- `16:26:20` ✅ extra-providers.json += fmp-bank (12 entries)
- `16:26:20` justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)
## 5. the fmp_book -> fmp_bank chain is in the fleet

- `16:26:22` justhodl-fmp-ratios bundle: fmp_bank.py=True fmp_book.py=True
- `16:26:22` ✅ ops 6640 complete
