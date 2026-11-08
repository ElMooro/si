# ops 6763 - the factor book behind the leaders desk's Sunday refusal 2026-11-08 (manifest, leaders funnel over the live matrix, bank-factors log; read-only)

**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-11-08T11:25:52+00:00  

## Data

| book_symbols | candidates | common | dv_null | manifest_age_h | mcap_null | no_cap | no_industry | no_tape | rows |
|---|---|---|---|---|---|---|---|---|---|
| 27109 | 153 | 5331 | 26896 | 8.5 | 0 | 4 | 0 | 5174 | 27109 |

## Log
## A. factor book manifest + matrix objects

- `11:25:48` manifest.json: 15660 B, last modified 2026-11-08T02:57:37+00:00 (8.5 h)
- `11:25:48` matrix/index.json.gz: 426068 B, last modified 2026-11-08T02:57:36+00:00 (8.5 h)
- `11:25:48` matrix/core.json.gz: 9920731 B, last modified 2026-11-08T02:57:33+00:00 (8.5 h)
- `11:25:48` matrix/cols/mcap.json.gz: 133955 B, last modified 2026-11-08T02:57:34+00:00 (8.5 h)
- `11:25:48` matrix/cols/dollar_vol_20d.json.gz: 2072 B, last modified 2026-11-08T02:57:36+00:00 (8.5 h)
- `11:25:49` matrix/cols/industry.json.gz: 49101 B, last modified 2026-11-08T02:57:33+00:00 (8.5 h)
- `11:25:49` matrix/cols/is_common.json.gz: 5225 B, last modified 2026-11-08T02:57:34+00:00 (8.5 h)
- `11:25:49` manifest keys: ['as_of', 'common_stocks', 'composites', 'coverage', 'debt_or_structured', 'engine', 'etfs', 'fields', 'funds', 'last_run', 'matrix', 'notes', 'objects', 'otc', 'pages', 'pages_bytes', 'preferreds', 'prefix', 'ranked', 'read_with', 'schema', 'schema_found', 'shells', 'source_columns', 'sources', 'symbols', 'typed_by_polygon', 'universe', 'updated_at', 'version', 'warrants_units']
- `11:25:49` as_of: {"finra-short-interest": "2026-10-15", "float": "2026-11-07", "fmp-bank": "2026-11-07T10:17:17.859945+00:00", "fmp-walker": "2026-11-07T10:25:11.790180+00:00", "polygon-bank": "2026-11-07T22:51:21.796042+00:00", "polygon-short-interest": "2026-10-15", "polygon-tape": "2026-11-07"}
- `11:25:49` symbols 27109 | generated 2026-11-08T02:50:45.044248+00:00 | version 1.0.7
- `11:25:49` coverage (selected): {"profile": 27109, "returns": 266}
- `11:25:49` sources: {"profile": {"files": 4}, "polygon_types": {"reference_tickers": 13236, "typed": 12921}, "scores": {"files": 1, "rows": 16089}, "rating": {"files": 1, "rows": 13552}, "dcf": {"files": 1, "rows": 9207}, "price_target": {"files": 1, "rows": 4254}, "consensus": {"files": 1, "rows": 6940}, "key_metrics_ttm": {"files": 1, "rows": 16768}, "ratios_ttm": {"files": 1, "rows": 16769}, "peers": {"files": 1, "rows": 24206}, "etf_holders": {"files": 4, "rows": 1821355}, "growth": {"files": 2, "years": [2026, 2025]}, "cf_growth": {"files": 2, "years": [2026, 2025]}, "surprises": {"files": 2, "years": [2026, 2025]}, "float": {"symbols": 88683}, "earnings-calendar": {"months": 2, "symbols": 25656}, "dividends-calendar": {"months": 2, "symbols": 8110}, "short": {"banked_tickers": 13192, "in_universe": 12521}, "short_volume": {"banked_tickers": 12438, "in_universe": 11803}, "finra_short": {"source": "rese
- `11:25:49` schema_found: {"mcap": "profile.marketCap", "price": "profile.price", "beta": "profile.beta", "avg_volume": "profile.averageVolume", "last_dividend": "profile.lastDividend", "altman_z": "scores.altmanZScore", "piotroski": "scores.piotroskiScore", "rating_score": null, "dcf_score": "rating.discountedCashFlowScore", "roe_score": "rating.returnOnEquityScore", "roa_score": "rating.returnOnAssetsScore", "de_score": "rating.debtToEquityScore", "pe_score": "rating.priceToEarningsScore", "pb_score": "rating.priceToBookScore", "dcf": "dcf.dcf", "pt_avg_1m": "price_target.lastMonthAvgPriceTarget", "pt_avg_1q": "price_target.lastQuarterAvgPriceTarget", "pt_avg_1y": "price_target.lastYearAvgPriceTarget", "pt_n_1m": "price_target.lastMonthCount", "pt_n_1q": "price_target.lastQuarterCount", "strong_buy": "consensus.strongBuy", "buy": "consensus.buy", "hold": "consensus.hold", "sell": "consensus.sell", "strong_sell"
## B. the leaders funnel over the live matrix (the engine's own conditions)

- `11:25:50` funnel: {"candidates": 153, "common": 5331, "dv_null": 26896, "industry_blank": 0, "is_common_false": 21777, "is_common_none": 0, "is_common_true": 5332, "mcap_null": 0, "no_cap": 4, "no_industry": 0, "no_tape": 5174, "rows": 27109}
- `11:25:50` Saturday's run (ops 6762 log): 27112 rows, 5329 common, 673 leaders; Sunday's refusal: 27109 rows, 58 leaders
- `11:25:50` ⚠ the live book yields only 153 leader candidates from 5331 common rows: mcap missing on 4, industry blank on 0, dollar_vol_20d missing on 5174 - the leaders desk refuses (correctly) until the book is whole again
- `11:25:50` sample of common rows missing mcap or dollar_vol_20d: [{"symbol": "A", "name": "Agilent Technologies, Inc.", "industry": "Medical - Diagnostics & Research", "mcap": 49276345480.0, "dollar_vol_20d": null, "country": "US"}, {"symbol": "AA", "name": "Alcoa Corporation", "industry": "Aluminum", "mcap": 10437368818.0, "dollar_vol_20d": null, "country": "US"}, {"symbol": "AACG", "name": "ATA Creativity Global", "industry": "Education & Training Services", "mcap": 9459558.0, "dollar_vol_20d": null, "country": "CN"}, {"symbol": "AADX", "name": "Applied Aerospace & Defense, Inc.", "industry": "Aerospace & Defense", "mcap": 1667045319.0, "dollar_vol_20d": null, "country": "US"}, {"symbol": "AAL", "name": "American Airlines Group Inc.", "industry": "Airlines, Airports & Air Services", "mcap": 8733305240.0, "dollar_vol_20d": null, "country": "US"}, {"symbol": "AAME", "name": "Atlantic American Corporation", "industry": "Insurance - Life", "mcap": 22640923.0, "dollar_vol_20d": null, "country": "US"}, {"symbol": "AAMI", "name": "Acadian Asset Management", "industry": "Asset Management", "mcap": 3228182043.0, "dollar_vol_20d": null, "country": "US"}, {"symbol": "AAOI", "name": "Applied Optoelectronics, Inc.", "industry": "Communication Equipment", "
## C. justhodl-bank-factors: last two log streams (redacted)

- `11:25:51` stream 4c3caceee241ddc51e76: last event 2026-11-08T02:57+00:00, 13 events, last invocation 12 lines
- `11:25:51`   11-08 02:50:45 START RequestId: 166aefe3-58ca-4dd0-9fa6-1de23909ca86 Version: $LATEST
- `11:25:51`   11-08 02:50:45 [bank-factors] v1.0.7 start; budget 840s
- `11:25:51`   11-08 02:50:50 [bank-factors] universe 27109 US-listed symbols (5s)
- `11:25:51`   11-08 02:52:03 [bank-factors] staged: {"profile": {"files": 4}, "polygon_types": {"reference_tickers": 13236, "typed": 12921}, "scores": {"files": 1, "rows": 16089}, "rating": {"files": 1, "rows": 13552}, "dcf": {"files": 1, "rows": 9207}, "price_target": {"files": 1, "rows": 4254}, "consensus": {"files": 1, "rows": 6940}, "key_metrics_ttm": {"files": 1, "rows": 16768}, "ratios_ttm": {"files": 1, "rows": 16769}, "peers": {"files": 1, "rows": 24206}, "etf_holders": {"files": 4, "rows": 1821355}, "growth": {"files": 2, "years": [2026, 2025]}, "cf_growth": {"files": 2, "years": [2026, 2025]}, "surprises": {"fil
- `11:25:51`   11-08 02:54:03 [bank-factors] float 88683, short 12521, short-volume 11803, finra short 23678, calendars 25656/8110 (198s)
- `11:25:51`   11-08 02:54:39 [bank-factors] returns: {"sessions": 253, "last_session": "2026-11-07", "symbols": 271, "seconds": 35.7} (234s)
- `11:25:51`   11-08 02:55:21 [bank-factors] pass 1: 27109 rows (277s)
- `11:25:51`   11-08 02:55:23 [bank-factors] ranks done (279s)
- `11:25:51`   11-08 02:57:26 [bank-factors] pass 2: 27109 written, 0 unchanged, 0 failed, 84.5 MB (402s)
- `11:25:51`   11-08 02:57:36 [bank-factors] done: 27109 symbols, 27109 written, 0 unchanged, 0 failed, 84.5 MB, 411s
- `11:25:51`   11-08 02:57:36 END RequestId: 166aefe3-58ca-4dd0-9fa6-1de23909ca86
- `11:25:51`   11-08 02:57:36 REPORT RequestId: 166aefe3-58ca-4dd0-9fa6-1de23909ca86 Duration: 411682.10 ms Billed Duration: 412198 ms Memory Size: 3008 MB Max Memory Used: 1281 MB Init Duration: 515.29 ms XRAY TraceId: 1-6aefe384-547c79076542938f18e01000 SegmentId: 2abb91da35570e55 Sampled: true
- `11:25:52` stream 43098e62469f24f23978: last event 2026-11-07T02:57+00:00, 13 events, last invocation 12 lines
- `11:25:52`   11-07 02:50:44 START RequestId: 166aee91-d8ca-4dd0-9fa6-1de23909ca86 Version: $LATEST
- `11:25:52`   11-07 02:50:45 [bank-factors] v1.0.7 start; budget 840s
- `11:25:52`   11-07 02:50:50 [bank-factors] universe 27112 US-listed symbols (6s)
- `11:25:52`   11-07 02:52:04 [bank-factors] staged: {"profile": {"files": 4}, "polygon_types": {"reference_tickers": 13241, "typed": 12920}, "scores": {"files": 1, "rows": 16083}, "rating": {"files": 1, "rows": 13848}, "dcf": {"files": 1, "rows": 9204}, "price_target": {"files": 1, "rows": 4252}, "consensus": {"files": 1, "rows": 6941}, "key_metrics_ttm": {"files": 1, "rows": 16765}, "ratios_ttm": {"files": 1, "rows": 16766}, "peers": {"files": 1, "rows": 24211}, "etf_holders": {"files": 4, "rows": 1824299}, "growth": {"files": 2, "years": [2026, 2025]}, "cf_growth": {"files": 2, "years": [2026, 2025]}, "surprises": {"fil
- `11:25:52`   11-07 02:54:02 [bank-factors] float 88675, short 12521, short-volume 10300, finra short 23678, calendars 25626/8072 (198s)
- `11:25:52`   11-07 02:54:41 [bank-factors] returns: {"sessions": 253, "last_session": "2026-11-06", "symbols": 12561, "seconds": 37.7} (236s)
- `11:25:52`   11-07 02:55:24 [bank-factors] pass 1: 27112 rows (280s)
- `11:25:52`   11-07 02:55:26 [bank-factors] ranks done (282s)
- `11:25:52`   11-07 02:57:43 [bank-factors] pass 2: 27112 written, 0 unchanged, 0 failed, 86.6 MB (418s)
- `11:25:52`   11-07 02:57:54 [bank-factors] done: 27112 symbols, 27112 written, 0 unchanged, 0 failed, 86.6 MB, 429s
- `11:25:52`   11-07 02:57:54 END RequestId: 166aee91-d8ca-4dd0-9fa6-1de23909ca86
- `11:25:52`   11-07 02:57:54 REPORT RequestId: 166aee91-d8ca-4dd0-9fa6-1de23909ca86 Duration: 429480.68 ms Billed Duration: 430013 ms Memory Size: 3008 MB Max Memory Used: 1284 MB Init Duration: 531.68 ms XRAY TraceId: 1-6aee9204-75c392a815a22f617fbf189b SegmentId: c8cc4f0c38b7fa30 Sampled: true
- `11:25:52` ✅ read-only; nothing invoked, nothing changed
