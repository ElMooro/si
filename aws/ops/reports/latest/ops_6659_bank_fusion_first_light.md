# ops 6659 - bank fusion wave 1: first light

**Status:** success  
**Duration:** 851.4s  
**Finished:** 2026-10-26T05:12:18+00:00  

## Log
## 0. the factor book

- `04:58:07` factor book: symbols 27215 updated 2026-10-26T04:50:58.823489+00:00 as_of {"fmp-bank": "2026-10-26T02:55:44.008955+00:00", "fmp-walker": "2026-10-26T03:03:34.986368+00:00", "polygon-bank": "2026-10-26T04:44:17.818169+00:00", "float": "2026-10-25", "polygon-short-interest": "2026-09-30", "polygon-tape": "2026-10-23"} coverage {"profile": 27215, "scores": 16138, "rating": 11532, "dcf": 9385, "price_target": 4250, "consensus": 6943, "key_metrics_ttm": 16896, "ratios_ttm": 16897, "peers": 23807, "etf_holders": 9431, "growth": 14161, "cf_growth": 13887, "surprises": 8414, "float": 27081, "short": 1643, "short_volume": 878, "returns": 12033}
## 1. deployed versions + kicks

- `04:58:08` justhodl-magic-formula: modified 2026-10-26T04:46:03, 1536 MB, 900 s, desc Greenblatt Magic Formula Screener: rank S&P 500 (ex-fin/utils/REITs) by Earnings
- `04:58:08`    kick -> 202
- `04:58:08` justhodl-divcut-warning: modified 2026-10-26T04:46:29, 768 MB, 540 s, desc Dividend cut early-warning engine. 5-signal cut-risk model: payout > 100%, FCF/d
- `04:58:08`    kick -> 202
- `04:58:09` justhodl-deep-value-screener: modified 2026-10-26T04:46:46, 1024 MB, 300 s, desc 
- `04:58:09`    kick -> 202
- `04:58:09` justhodl-equity-ftd: modified 2026-10-26T04:47:03, 512 MB, 600 s, desc Equity FTD v1.0.1 min-base (ops 3514)
- `04:58:09`    kick -> 202
- `04:58:10` justhodl-stock-valuations: modified 2026-10-26T04:47:20, 640 MB, 600 s, desc 
- `04:58:10`    kick -> 202
- `04:58:10` justhodl-analyst-consensus: modified 2026-10-26T04:47:44, 1024 MB, 600 s, desc Analyst Consensus engine — price targets, recs, beats, upgrade pulse
- `04:58:10`    kick -> 202
- `04:58:11` justhodl-quality-on-sale: modified 2026-10-26T04:48:12, 512 MB, 120 s, desc Quality-on-Sale Detector. Buffett 5-condition AND gate: Predictability 5-star + 
- `04:58:11`    kick -> 202
- `04:58:11` justhodl-beneish: modified 2026-10-26T04:48:38, 1024 MB, 600 s, desc Beneish M-Score (GuruFocus fraud-detection gap-closer): 8-variable composite ide
- `04:58:11`    kick -> 202
- `04:58:12` justhodl-insider-radar: modified 2026-10-26T04:48:56, 512 MB, 300 s, desc 
- `04:58:12`    kick -> 202
- `04:58:12` justhodl-eps-revision-velocity: modified 2026-10-26T04:49:13, 1024 MB, 300 s, desc 
- `04:58:12`    kick -> 202
## 2. outputs after the kicks

- `05:12:14` == justhodl-magic-formula (data/magic-formula.json) fresh output
- `05:12:14`    version 1.1.0 | universe S&P 500 + US common stocks >= $200M from the factor book (1200 candidates, bank as of 2026-10-26), ex-Financials/Utiliti | n_universe 1425 n_book 1200 | eligible 910 | regime ABUNDANT_OPPORTUNITY
- `05:12:14`    price_source counts {'factor-book close as of ': 910} | top5 [('YALA', 116.5, 214.1), ('RRR', 21.0, 481.6), ('VRRM', 53.9, 89.7), ('PXED', 26.6, 125.2), ('PGYWW', 23.6, 155.0)]
- `05:12:15`    logs: nothing matched (no error, no fusion line)
- `05:12:15` == justhodl-divcut-warning (data/divcut-warning.json) output NOT refreshed within 14 min - showing the previous one
- `05:12:15`    version v1.0.0 | state QUIET | universe 82 | basis None | flagged 0 high 0
- `05:12:15`    price_source counts {} | with bank_context 0/0 | top []
- `05:12:15`    logs: ['universe: 82 curated + 394 factor-book candidates = 446', 'universe: 82 curated + 394 factor-book candidates = 446', 'universe: 82 curated + 394 factor-book candidates = 446']
- `05:12:15` == justhodl-deep-value-screener (data/deep-value.json) fresh output
- `05:12:15`    method deep_value_screener_v1.1 | universe 500 book 500 | basis factor book whole-market net-cash candidates (500) + unified universe / seeds | qualifying 2 tier_a 0 | statuses {"ok": 2, "no_quote": 0, "below_min_mcap": 0, "no_balance": 492, "below_min_net_cash": 4, "no_income": 2, "deadline_skip": 0}
- `05:12:15`    price_source counts {'factor-book close as of ': 2} | with bank_context 2/2 | top [('AVAL', 20.0, 'FINANCIAL_BOOK_EXCLUDED'), ('SUPV', 20.0, 'FINANCIAL_BOOK_EXCLUDED')]
- `05:12:15`    logs: ['[deep-value] factor book candidates: 500 (bank as of 2026-10-26T02:55:44.008955+00:00)']
- `05:12:15` == justhodl-equity-ftd (data/equity-ftd.json) output NOT refreshed within 14 min - showing the previous one
- `05:12:15`    version 1.0.1 | ok True | candidates 192 with squeeze None | book_as_of null | vol_source {'None': 30}
- `05:12:15`    top spikes [('RETO', 320.75, None, None), ('SLI', 203.49, None, None), ('ALLO', 188.63, None, None), ('OBX', 99.62, None, None), ('GLND', 97.51, None, None), ('CTNT', 90.95, None, None)]
- `05:12:16`    logs: nothing matched (no error, no fusion line)
- `05:12:16` == justhodl-stock-valuations (data/stock-valuations.json) output NOT refreshed within 14 min - showing the previous one
- `05:12:16`    version 1.5.1 | hp_src fallback_union(deep-value,overlap,insider,cheap_candidates) | hp_logged ['CRBG'] | n_serious 1 | keys ['diagnostics', 'duration_s', 'engine', 'generated_at', 'heatmap', 'hist_coverage', 'hp', 'hp_coverage', 'hp_logged', 'hp_src', 'hp_universe', 'industries', 'methodology', 'n_deep_discount', 'n_industries', 'n_serious']
- `05:12:16`    diag (hp): []
- `05:12:17`    logs: nothing matched (no error, no fusion line)
- `05:12:17` == justhodl-analyst-consensus (data/analyst-consensus.json) fresh output
- `05:12:17`    method consensus_engine_v1.1 | universe 649 | from_book 644 | basis 194 curated + 569 factor-book analyst-covered names (>= 3 targets last month, >= $300M; bank as of 2026-10-26) | with data 649
- `05:12:17`    top5 [('Q', 76.2, 59.56), ('ELMT', 76.2, 28.79), ('KTOS', 75.8, 79.5), ('ARGX', 75.7, 31.42), ('AVGO', 75.6, 86.52)]
- `05:12:17`    logs: ['[consensus] universe: 194 curated + 569 factor-book = 649']
- `05:12:17` == justhodl-quality-on-sale (data/quality-on-sale.json) fresh output
- `05:12:17`    version 1.1.0 | regime DRY | 5-of-5 0 | book_screen available True n 3 as_of {"fmp-bank": "2026-10-26T02:55:44.008955+00:00", "fmp-walker
- `05:12:17`    book candidates [('MU', 91.33, -30.0757), ('SII', 91.19, -37.965), ('SEZL', 90.44, -32.492)]
- `05:12:17`    logs: nothing matched (no error, no fusion line)
- `05:12:17` == justhodl-beneish (data/beneish.json) fresh output
- `05:12:17`    version 1.1.0 | top-50 state MODERATE_FRAUD_RISK (STATIC_TOP50_SPX (50 scored)) | analyzed 2968 | market candidates 3177 scored 2968 red 463 (15.6%)
- `05:12:17`    market top red flags [('SBR', 42.336), ('PBT', 32.367), ('IDYA', 12.448), ('HBNB', 9.86), ('ENVA', 8.488), ('RZLV', 7.831), ('QUBT', 7.766), ('CGON', 7.205)]
- `05:12:18`    logs: nothing matched (no error, no fusion line)
- `05:12:18` == justhodl-insider-radar (data/insider-radar.json) output NOT refreshed within 14 min - showing the previous one
- `05:12:18`    version 1.1.1 | source_used fmp | n_raw 1000 | buys 40 clusters 6 | diag []
- `05:12:18`    logs: nothing matched (no error, no fusion line)
- `05:12:18` == justhodl-eps-revision-velocity (data/eps-revision-velocity.json) fresh output
- `05:12:18`    method received_annual_targets_and_same_target_changes | universe 400 book None | basis None | qualifying None | price_source {}
- `05:12:18`    logs: ['[ERROR] ValueError: All provider populations unavailable; preserve previous publication\nTraceback (most recent call last):\n\xa0\xa0File "/var/task/lambda_function.py", line 620, in lambda_handler\n\xa0\xa0\xa0\xa0raise ValueError(\'All prov', '[ERROR] ValueError: All provider populations unavailable; preserve previous publication\nTraceback (most recent call last):\n\xa0\xa0File "/var/task/lambda_function.py", line 620, in lambda_handler\n\xa0\xa0\xa0\xa0raise ValueError(\'All prov']
- `05:12:18` ⚠    justhodl-eps-revision-velocity raised or timed out - read the log group
- `05:12:18` ✅ ops 6659 complete
