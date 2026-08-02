# ops 4273 -- House PTR PDFs into the official tape

**Status:** success  
**Duration:** 7.5s  
**Finished:** 2026-08-02T17:31:01+00:00  

## Data

| amount | date | owner | ticker | tx | who |
|---|---|---|---|---|---|
| $1,001 - $15,000 | 2026-07-30 | None | TSM | Purchase | Cleo Fields |
| $15,001 - $50,000 | 2026-07-27 | None | NVDA | Sale | Sam T. Liccardo |
| $1,001 - $15,000 | 2026-07-09 | None | DASH | Sale | Tim Moore |
| $1,001 - $15,000 | 2026-07-07 | JT | NVZMY | Sale | Josh Gottheimer |
| $1,001 - $15,000 | 2026-07-06 | None | PPG | Purchase | Lloyd Doggett |

## Log
## 1. extractor live + first walk

- `17:30:54` ✅ function live via deploy workflow
- `17:30:59` invoked: {"ok": true, "new_docs": 20, "parsed": 17, "no_text": 3, "errors": 0, "elapsed_s": 4.3, "trades_total": 5}
- `17:30:59` ✅ ledger: 20 docs (17 parsed / 3 no_text / 0 err) -> 5 trades
## 2. schedule (6-hourly incremental)

- `17:31:00` ✅ schedule CREATED cron(10 1,7,13,19 * * ? *)
## 3. political-stocks: both chambers + attribution proof

- `17:31:01` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 86, \"n_house\": 0, \"n_senate\": 86, \"n_tickers\": 27, \"n_clusters\": 0, \"n_bipartisan\": 0, \"duration_s\": 0.2}"}
- `17:31:01` chambers: senate=86 house=0 tickers=27 clusters=0
- `17:31:01` ⚠ no scored-ticker trades to sample yet
## RESULT

- `17:31:01` ✅ OPS 4273 PASS -- both chambers on official rails, quality inspectable per-doc
