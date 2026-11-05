# ops 6755 - 13F book automatic runs of 2026-11-05 (read-only check-in) 2026-11-05T16:54+00:00

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-11-05T16:54:29+00:00  

## Data

| generated | handoff | learned_rows | problems | resolved_pct | scheduled | starts | trigger | unclassified | version |
|---|---|---|---|---|---|---|---|---|---|
| 2026-11-05T16:37:28+00:00 | True | 2554 | 0 | 99.6 | True | 3 | scheduled | 1625 | 1.3.0 |

## Log
## 1. the live book

- `16:54:26` data/13f-book.json: 1821297 bytes, modified 2026-11-05T16:37:47+00:00 | version 1.3.0 contract 13f-book.v2 | generated 2026-11-05T16:37:28+00:00 | run 18.4s | trigger {"kind": "scheduled", "source_generated_at": null} | errors 0
- `16:54:26` master: 8835/9059 resolved = 99.60% of value | typed-only 43 (0.3% of value) | no_match 179 ambiguous 45 error 0 pending 0 | unmapped 0.00% of value | refresh: due 0 requests 0 changed 0 partial 0 deadline False
- `16:54:26` review: class sources {"none": 1625, "table": 653, "fmp": 351} | learned rows 2554 last run {"fund_like_unknown_to_table": 0, "looked_up": 0, "banked": 0, "classed": 0, "unknown_label": 0, "errors": 0, "labels": {}, "seconds": 0.0, "learned_total": 2554, "skipped": false} | unclassified ETFs 1625 = $1.61B (table 2026-11-04, 692 tickers) | unmapped held 108 | other 2
- `16:54:26` rotation cohort: now $1505.90B prior $1282.14B identities 8036 | equities $1482.84B (98.47%, BALANCED); bonds $19.08B (1.27%, ACCUMULATING); cash $801M (0.05%, BALANCED); gold $390M (0.03%, BALANCED); silver $66M (0.0%, BALANCED); commodities $94M (0.01%, BALANCED); crypto $645M (0.04%, DISTRIBUTING); volatility $26M (0.0%, ACCUMULATING)
- `16:54:26` rotation concentrated: now $402.52B prior $343.48B identities 175 | equities $402.52B (100.0%, DISTRIBUTING); bonds $0 (0.0%, DISTRIBUTING); crypto $0 (0.0%, DISTRIBUTING); funds_unclassified $0 (0.0%, DISTRIBUTING)
- `16:54:26` tickers block: {"key": "data/13f-tickers.json", "written": true, "n_tickers": 6673, "bytes": 1045430, "error": null}
- `16:54:26` season: {"period": null, "next_period": null, "deadline": null, "filed": null, "n_filed": null, "n_cohort": null, "detector": null}
- `16:54:26` data/13f-tickers.json: 1045430 bytes, modified 2026-11-05T16:37:46+00:00
## 2. learned ETF classes

- `16:54:26` data/13f-etf-classes-learned.json: 407441 bytes, modified 2026-11-05T16:23:04+00:00 | n_rows 2554 | last run {"fund_like_unknown_to_table": 154, "looked_up": 154, "banked": 20, "classed": 18, "unknown_label": 2, "errors": 0, "labels": {"Multi-Asset": 2, "Equity": 12, "Fixed Income": 3, "Leveraged/Inverse": 2, "US Equity": 1}, "seconds": 4.3, "learned_total": 2554, "skipped": false} | keys ['by_ticker', 'contract', 'engine', 'generated_at', 'last_run', 'n_rows', 'recheck_days', 'source', 'version', 'vocabulary']
## 3. today's invocations (CloudWatch, paginated) and the Scheduler

- `16:54:27`   15:45:35 13f-book: 18 managers, cohort 15, concentrated 6, cohort equity $1490.0B, master 8835/9059 resolved (99.6% of value), 1821524 bytes, 25.4s, errors 0
- `16:54:27`   16:23:09 13f-book: 18 managers, cohort 15, concentrated 6, cohort equity $1490.0B, master 8835/9059 resolved (99.6% of value), 1821432 bytes, 21.9s, errors 0
- `16:54:27`   16:37:46 13f-book: 18 managers, cohort 15, concentrated 6, cohort equity $1490.0B, master 8835/9059 resolved (99.6% of value), 1821297 bytes, 18.4s, errors 0
- `16:54:27` book lines today: 3 | trigger kinds seen: {}
- `16:54:28` book START lines today: 3 -> 15:45:09, 16:22:47, 16:37:28
- `16:54:28`   research 15:45:08 [13f] book hand-off fired (justhodl-13f-book, async)
- `16:54:28`   research 16:22:46 [13f] book hand-off fired (justhodl-13f-book, async)
- `16:54:28` research hand-off lines today: 2
- `16:54:29` chain_guard refusal-shaped lines today: 0
- `16:54:29` schedule justhodl-13f-book-daily: cron(37 16 * * ? *) ENABLED tz UTC
- `16:54:29` justhodl-13f-book: modified 2026-11-04T20:57:55 state Active sha zKqnOMmUqxj1
- `16:54:29` ✅ 13F book automatic runs verified for 2026-11-05: 3 invocations (scheduled True, hand-off True), live book 1.3.0 generated 2026-11-05T16:37:28+00:00 by trigger scheduled
