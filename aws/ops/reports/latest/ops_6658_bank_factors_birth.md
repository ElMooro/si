# ops 6658 - justhodl-bank-factors: declare, first join, data.html card, read back

**Status:** success  
**Duration:** 323.2s  
**Finished:** 2026-10-26T04:28:25+00:00  

## Data

| failed | mb | notes | seconds | step | symbols | unchanged | written |
|---|---|---|---|---|---|---|---|
| 0 | 77.2 | [] | 303.5 | first join | 27215 | 0 | 27215 |

## Log
## 1. function + schedule

- `04:23:02` justhodl-bank-factors: runtime python3.12, memory 3008, timeout 900, state Active, modified 2026-10-26T04:19:01
- `04:23:02` schedule justhodl-bank-factors-daily: cron(50 2 * * ? *) ENABLED tz UTC target justhodl-bank-factors
## 2. deployed schedule manifest := committed (additions-only)

- `04:23:03` S3 1037 entries, repo 1037; added [] removed 0 changed 0
- `04:23:03` ✅ manifest already equal
## 3. first join (async invoke, poll the state)

- `04:23:03` async invoke 202 at 04:23:03
- `04:28:24` manifest: symbols 27215 universe 27215 common 6880 etfs 5773 otc 10248
- `04:28:24` as_of: {"fmp-bank": "2026-10-26T02:55:44.008955+00:00", "fmp-walker": "2026-10-26T03:03:34.986368+00:00", "polygon-bank": "2026-10-26T04:16:12.399330+00:00", "float": "2026-10-25", "polygon-short-interest": "2026-09-30", "polygon-tape": "2026-10-23"}
- `04:28:24` coverage (symbols with the block): {"profile": 27215, "scores": 16138, "rating": 11532, "dcf": 9385, "price_target": 4250, "consensus": 6943, "key_metrics_ttm": 16896, "ratios_ttm": 16897, "peers": 23807, "etf_holders": 9431, "growth": 14161, "cf_growth": 13887, "surprises": 8414, "float": 27081, "short": 1434, "short_volume": 776, "returns": 12033}
- `04:28:24` sources: {"profile": {"files": 4}, "scores": {"files": 1, "rows": 16138}, "rating": {"files": 1, "rows": 11532}, "dcf": {"files": 1, "rows": 9385}, "price_target": {"files": 1, "rows": 4253}, "consensus": {"files": 1, "rows": 6954}, "key_metrics_ttm": {"files": 1, "rows": 16896}, "ratios_ttm": {"files": 1, "rows": 16897}, "peers": {"files": 1, "rows": 24317}, "etf_holders": {"files": 4, "rows": 1804412}, "growth": {"files": 2, "years": [2026, 2025]}, "cf_growth": {"files": 2, "years": [2026, 2025]}, "surprises": {"files": 2, "years": [2026, 2025]}, "float": {"symbols": 88206}, "earnings-calendar": {"months": 1, "symbols": 10091}, "dividends-calendar": {"months": 1, "symbols": 4776}, "short": {"banked_tickers": 1524, "in_universe": 1434}, "short_volume": {"banked_tickers": 828, "in_universe": 776}, "returns": {"sessions": 253, "last_session": "2026-10-23", "symbols": 12544, "seconds": 38.6}}
- `04:28:24` schema_found: 85 fields resolved, unresolved: ['rating_score']
- `04:28:24` schema_found detail: {"mcap": "profile.marketCap", "price": "profile.price", "beta": "profile.beta", "avg_volume": "profile.averageVolume", "last_dividend": "profile.lastDividend", "altman_z": "scores.altmanZScore", "piotroski": "scores.piotroskiScore", "rating_score": null, "dcf_score": "rating.discountedCashFlowScore", "roe_score": "rating.returnOnEquityScore", "roa_score": "rating.returnOnAssetsScore", "de_score": "rating.debtToEquityScore", "pe_score": "rating.priceToEarningsScore", "pb_score": "rating.priceToBookScore", "dcf": "dcf.dcf", "pt_avg_1m": "price_target.lastMonthAvgPriceTarget", "pt_avg_1q": "price_target.lastQuarterAvgPriceTarget", "pt_avg_1y": "price_target.lastYearAvgPriceTarget", "pt_n_1m": "price_target.lastMonthCount", "pt_n_1q": "price_target.lastQuarterCount", "strong_buy": "consensus.strongBuy", "buy": "consensus.buy", "hold": "consensus.hold", "sell": "consensus.sell", "strong_sell": "consensus.strongSell", "ev": "key_metrics_ttm.enterpriseValueTTM", "ev_ebitda": "key_metrics_ttm.evToEBITDATTM", "ev_sales": "key_metrics_ttm.evToSalesTTM", "fcf_yield": "key_metrics_ttm.freeCashFlowYieldTTM", "earnings_yield": "key_metrics_ttm.earningsYieldTTM", "roic": "key_metrics_ttm.returnOnInvestedCapitalTTM", "roe": "key_metrics_ttm.returnOnEquityTTM", "roa": "key_metrics_ttm.returnOnAssetsTTM", "net_debt_ebitda": "key_metrics_ttm.netDebtToEBITDATTM", "capex_to_revenue": "key_metrics_ttm.capexToRevenueTTM", "rd_to_revenue": "key_metrics_ttm.researchAndDevelopementToRevenueTTM", "sbc_
## 4. data.html card (extra-providers.json, add-only) + catalog refresh

- `04:28:24` ✅ extra-providers.json += factor-book (18 entries)
- `04:28:24` justhodl-provider-catalog invoked async (card lands within ~10 min; daily 06:20 otherwise)
## 5. read back

- `04:28:24` AAPL: sector Technology mcap 5074761240500.0 | altman 13.64556013271688 piotroski 9 rating B | dcf_upside -60.168 pt_upside -7.2054 consensus 0.5536 | pe 39.74 ev/ebitda 30.291967388731763 roic 0.5186736691216669 | growth rev 0.0642551178283274 | beat4q 1.0 | float 14569078010 SI%float 0.8289 dtc 3.03 | ret_21d 3.5142 from_52w_high -1.1387 | composites {"growth": 62.33, "momentum": 84.14, "quality": 84.35, "squeeze": 25.47, "street": 49.94, "value": 28.58}
- `04:28:24` AAPL blocks present: ['profile', 'scores', 'rating', 'dcf', 'price_target', 'consensus', 'key_metrics_ttm', 'ratios_ttm', 'growth', 'cf_growth', 'surprises', 'float', 'short', 'short_volume', 'etf_holders', 'peers', 'returns']
- `04:28:24` NVDA: sector Technology mcap 5084136000000.0 | altman 38.2704667875976 piotroski 6 rating A- | dcf_upside 8.3373 pt_upside 24.0377 consensus 0.7468 | pe 26.53584905660377 ev/ebitda 21.807113478755337 roic 0.5950424906817126 | growth rev 0.654735357900948 | beat4q 1.0 | float 23133108000 SI%float None dtc None | ret_21d -6.0647 from_52w_high -13.3172 | composites {"growth": 85.55, "momentum": 71.5, "quality": 85.61, "squeeze": null, "street": 72.4, "value": 32.75}
- `04:28:24` NVDA blocks present: ['profile', 'scores', 'rating', 'dcf', 'price_target', 'consensus', 'key_metrics_ttm', 'ratios_ttm', 'growth', 'cf_growth', 'surprises', 'float', 'etf_holders', 'peers', 'returns']
- `04:28:25` GME: sector Consumer Cyclical mcap 12521714572.0 | altman 2.80984143655072 piotroski 4 rating A- | dcf_upside -83.7159 pt_upside -100.0 consensus -0.1111 | pe 12.472361809045225 ev/ebitda 11.174797068628365 roic 0.03813993621959424 | growth rev -0.05051007062516349 | beat4q 1.0 | float 421403631 SI%float None dtc None | ret_21d -0.7994 from_52w_high -12.2968 | composites {"growth": 63.78, "momentum": 71.6, "quality": 61.37, "squeeze": null, "street": 19.2, "value": 56.23}
- `04:28:25` GME blocks present: ['profile', 'scores', 'rating', 'dcf', 'price_target', 'consensus', 'key_metrics_ttm', 'ratios_ttm', 'growth', 'cf_growth', 'surprises', 'float', 'etf_holders', 'peers', 'returns']
- `04:28:25` SPY: sector Financial Services mcap 806466076436.0 | altman None piotroski None rating None | dcf_upside None pt_upside None consensus None | pe None ev/ebitda None roic None | growth rev None | beat4q None | float 0 SI%float None dtc None | ret_21d -0.9398 from_52w_high -2.7699 | composites {"growth": null, "momentum": null, "quality": null, "squeeze": null, "street": null, "value": null}
- `04:28:25` SPY blocks present: ['profile', 'float', 'etf_holders', 'peers', 'returns']
- `04:28:25` matrix: n 27215, fields 191; screen Altman Z < 1.8 & mcap > $300M -> 1796 names, e.g. [('AAISX', 0.6353703634865435, 536380422.0), ('AAL', 0.6650874399732987, 8016456700.0), ('AAP', 1.7914479823663751, 2833968000.0), ('AAPG', -2.2986155133125314, 1435354417.0), ('AAT', 0.6045804142568113, 1311593712.0), ('ABCB', 0.382455798727891, 5545071159.0), ('ABCVX', 1.0310908962647487, 1068376505.0), ('ABHYX', -0.41723033759541284, 513249500.0), ('ABR', 0.14364904961891206, 544749486.0), ('ABR-PD', 0.24272812323367898, 562934364.0), ('ABR-PE', 0.2427281228675639, 564002803.0), ('ABR-PF', 0.261439275968467, 557132841.0)]
- `04:28:25` matrix fill (non-null of 27215): {"altman_z": 15900, "pe": 16868, "ev_ebitda": 16870, "pt_upside_pct": 4250, "consensus_score": 6943, "revenue_growth": 14161, "beat_rate_4q": 8414, "float_shares": 24823, "short_pct_float": 931, "ret_21d": 11747, "composite_value": 6329}
- `04:28:25` ✅ ops 6658 complete
