# ops 6662 - bank fusion wave 1: second light (after the first-light fixes)

**Status:** success  
**Duration:** 866.3s  
**Finished:** 2026-10-26T05:58:56+00:00  

## Log
## 0. the factor book

- `05:44:30` factor book: symbols 27215 updated 2026-10-26T04:50:58.823489+00:00 as_of {"fmp-bank": "2026-10-26T02:55:44.008955+00:00", "fmp-walker": "2026-10-26T03:03:34.986368+00:00", "polygon-bank": "2026-10-26T04:44:17.818169+00:00", "float": "2026-10-25", "polygon-short-interest": "2026-09-30", "polygon-tape": "2026-10-23"} coverage {"profile": 27215, "scores": 16138, "rating": 11532, "dcf": 9385, "price_target": 4250, "consensus": 6943, "key_metrics_ttm": 16896, "ratios_ttm": 16897, "peers": 23807, "etf_holders": 9431, "growth": 14161, "cf_growth": 13887, "surprises": 8414, "float": 27081, "short": 1643, "short_volume": 878, "returns": 12033}
## 1. deployed versions + kicks

- `05:44:31` justhodl-magic-formula: modified 2026-10-26T05:40:04, 1536 MB, 900 s, desc Greenblatt Magic Formula Screener: rank S&P 500 (ex-fin/utils/REITs) by Earnings
- `05:44:31`    kick -> 202
- `05:44:31` justhodl-divcut-warning: modified 2026-10-26T05:40:35, 1024 MB, 540 s, desc Dividend cut early-warning engine (v1.1.1): 5-signal cut-risk model over curated
- `05:44:31`    kick -> 202
- `05:44:32` justhodl-deep-value-screener: modified 2026-10-26T05:40:56, 1024 MB, 300 s, desc 
- `05:44:32`    kick -> 202
- `05:44:32` justhodl-equity-ftd: modified 2026-10-26T05:41:16, 512 MB, 600 s, desc Equity FTD v1.0.1 min-base (ops 3514)
- `05:44:32`    kick -> 202
- `05:44:33` justhodl-stock-valuations: modified 2026-10-26T05:41:45, 1536 MB, 600 s, desc Stock valuations desk: sector-relative multiples, 5y history, HP small/mid-cap u
- `05:44:33`    kick -> 202
- `05:44:33` justhodl-analyst-consensus: modified 2026-10-26T05:42:14, 1024 MB, 600 s, desc Analyst Consensus engine — price targets, recs, beats, upgrade pulse
- `05:44:33`    kick -> 202
- `05:44:33` justhodl-quality-on-sale: modified 2026-10-26T05:42:49, 512 MB, 120 s, desc Quality-on-Sale Detector. Buffett 5-condition AND gate: Predictability 5-star + 
- `05:44:33`    kick -> 202
- `05:44:34` justhodl-beneish: modified 2026-10-26T05:43:20, 1024 MB, 600 s, desc Beneish M-Score (GuruFocus fraud-detection gap-closer): 8-variable composite ide
- `05:44:34`    kick -> 202
- `05:44:34` justhodl-insider-radar: modified 2026-10-26T05:43:41, 512 MB, 300 s, desc 
- `05:44:34`    kick -> 202
## 2. outputs after the kicks

- `05:58:51` == justhodl-magic-formula (data/magic-formula.json) fresh output
- `05:58:51`    version 1.1.0 | universe S&P 500 + US common stocks >= $200M from the factor book (1200 candidates, bank as of 2026-10-26), ex-Financials/Utiliti | n_universe 1425 n_book 1200 | eligible 703 | regime ABUNDANT_OPPORTUNITY
- `05:58:51`    price_source counts {'factor-book close as of ': 703} | top5 [('YALA', 116.5, 214.1), ('RRR', 21.0, 481.6), ('PXED', 26.6, 125.2), ('PGYWW', 23.6, 155.0), ('PGY', 17.8, 155.0)]
- `05:58:51`    REPORT dur 230591.49 ms | max mem 767 / 1536 MB
- `05:58:51`    logs: nothing matched (no error, no fusion line)
- `05:58:51` == justhodl-divcut-warning (data/divcut-warning.json) fresh output
- `05:58:51`    version v1.1.1 | state NORMAL | universe 314 | basis curated list (82) + factor book dividend payers at risk (250; bank as of 2026-10-26) | flagged 102 high 0
- `05:58:51`    price_source counts {'factor-book close as of ': 30} | with bank_context 30/30 | top [('AD', 1, 63.32), ('UZF', 1, 63.32), ('UZD', 1, 63.32), ('UZE', 1, 63.32), ('OXLCM', 1, 50.0)]
- `05:58:52`    REPORT dur 27331.55 ms | max mem 402 / 1024 MB
- `05:58:52`    logs: ['universe: 82 curated + 250 factor-book candidates = 314']
- `05:58:52` == justhodl-deep-value-screener (data/deep-value.json) fresh output
- `05:58:52`    method deep_value_screener_v1.1 | universe 500 book 500 | basis factor book whole-market net-cash candidates (500) + unified universe / seeds | qualifying 367 tier_a 47 | statuses {"ok": 367, "no_quote": 0, "below_min_mcap": 0, "no_balance": 2, "below_min_net_cash": 131, "no_income": 0, "deadline_skip": 0}
- `05:58:52`    price_source counts {'factor-book close as of ': 367} | with bank_context 367/367 | top [('KT', 100, 'DEEP_VALUE_TIER_A'), ('GRVY', 100, 'MARGINAL'), ('AOSL', 100, 'DEEP_VALUE_TIER_B'), ('SKM', 100, 'DEEP_VALUE_TIER_A'), ('HMC', 100, 'DEEP_VALUE_TIER_A')]
- `05:58:53`    REPORT dur 38878.32 ms | max mem 372 / 1024 MB
- `05:58:53`    logs: ['[deep-value] starting v1.1 (bank fusion), max_tickers=500, budget=240s', '[deep-value] factor book candidates: 500 (bank as of 2026-10-26T02:55:44.008955+00:00)']
- `05:58:53` == justhodl-equity-ftd (data/equity-ftd.json) output NOT refreshed within 14 min - showing the previous one
- `05:58:53`    version 1.0.1 | ok True | candidates 192 with squeeze None | book_as_of null | vol_source {'None': 30}
- `05:58:53`    top spikes [('RETO', 320.75, None, None), ('SLI', 203.49, None, None), ('ALLO', 188.63, None, None), ('OBX', 99.62, None, None), ('GLND', 97.51, None, None), ('CTNT', 90.95, None, None)]
- `05:58:54`    REPORT dur 8497.65 ms | max mem 192 / 512 MB
- `05:58:54`    logs: nothing matched (no error, no fusion line)
- `05:58:54` == justhodl-stock-valuations (data/stock-valuations.json) fresh output
- `05:58:54`    version 1.6.0 | hp_src factor_book_small_mid_caps | hp_logged ['PRDO'] | n_serious 1 | keys ['diagnostics', 'duration_s', 'engine', 'generated_at', 'heatmap', 'hist_coverage', 'hp', 'hp_coverage', 'hp_logged', 'hp_src', 'hp_universe', 'industries', 'methodology', 'n_deep_discount', 'n_industries', 'n_serious']
- `05:58:54`    diag (hp): []
- `05:58:54`    REPORT dur 70323.91 ms | max mem 1011 / 1536 MB
- `05:58:54`    logs: nothing matched (no error, no fusion line)
- `05:58:54` == justhodl-analyst-consensus (data/analyst-consensus.json) fresh output
- `05:58:54`    method consensus_engine_v1.1 | universe 649 | from_book 644 | basis 194 curated + 569 factor-book analyst-covered names (>= 3 targets last month, >= $300M; bank as of 2026-10-26) | with data 649
- `05:58:54`    top5 [('Q', 76.2, 59.56), ('ELMT', 76.2, 28.79), ('KTOS', 75.8, 79.5), ('ARGX', 75.7, 31.42), ('AVGO', 75.6, 86.52)]
- `05:58:54`    REPORT dur 5968.92 ms | max mem 158 / 1024 MB
- `05:58:55`    logs: ['[consensus] universe: 194 curated + 569 factor-book = 649']
- `05:58:55` == justhodl-quality-on-sale (data/quality-on-sale.json) fresh output
- `05:58:55`    version 1.1.0 | regime DRY | 5-of-5 0 | book_screen available True n 3 as_of {"fmp-bank": "2026-10-26T02:55:44.008955+00:00", "fmp-walker
- `05:58:55`    book candidates [('MU', 91.33, -30.0757), ('SII', 91.19, -37.965), ('SEZL', 90.44, -32.492)]
- `05:58:55`    REPORT dur 1600.87 ms | max mem 112 / 512 MB
- `05:58:55`    logs: nothing matched (no error, no fusion line)
- `05:58:55` == justhodl-beneish (data/beneish.json) fresh output
- `05:58:55`    version 1.1.0 | top-50 state MODERATE_FRAUD_RISK (STATIC_TOP50_SPX (50 scored)) | analyzed 2968 | market candidates 3177 scored 2968 red 463 (15.6%)
- `05:58:55`    market top red flags [('SBR', 42.336), ('PBT', 32.367), ('IDYA', 12.448), ('HBNB', 9.86), ('ENVA', 8.488), ('RZLV', 7.831), ('QUBT', 7.766), ('CGON', 7.205)]
- `05:58:55`    REPORT dur 18240.34 ms | max mem 484 / 1024 MB
- `05:58:55`    logs: nothing matched (no error, no fusion line)
- `05:58:56` == justhodl-insider-radar (data/insider-radar.json) output NOT refreshed within 14 min - showing the previous one
- `05:58:56`    version 1.1.1 | source_used fmp | n_raw 1000 | buys 40 clusters 6 | diag []
- `05:58:56`    REPORT dur 300000.00 ms | max mem 512 / 512 MB
- `05:58:56`    REPORT dur 300000.00 ms | max mem 512 / 512 MB
- `05:58:56`    logs: nothing matched (no error, no fusion line)
## 3. verdict

- `05:58:56` justhodl-magic-formula: fresh
- `05:58:56` justhodl-divcut-warning: fresh
- `05:58:56` justhodl-deep-value-screener: fresh
- `05:58:56` ⚠ justhodl-equity-ftd: not refreshed
- `05:58:56` justhodl-stock-valuations: fresh
- `05:58:56` justhodl-analyst-consensus: fresh
- `05:58:56` justhodl-quality-on-sale: fresh
- `05:58:56` justhodl-beneish: fresh
- `05:58:56` ⚠ justhodl-insider-radar: not refreshed
- `05:58:56` ✅ ops 6662 complete
