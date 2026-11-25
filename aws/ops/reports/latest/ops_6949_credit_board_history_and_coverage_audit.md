# ops 6949 - audit for Credit Before Equity 3.0: history and coverage the warehouse can give (read-only), 2026-11-25T15:28+00:00

**Status:** success  
**Duration:** 57.5s  
**Finished:** 2026-11-25T15:28:59+00:00  

## Log
## 1. The bond desk's feed: dates listed, and whether an older month-end is still served

- `15:28:06` USIG latest: 11696 rows as of 2026-11-24, 6.5 MB; dates listed (4): ['2026-11-24', '2026-10-30', '2026-09-30', '2025-12-31']
- `15:28:06` USIG data points the feed carries (29): ['accrualDate', 'asOfDate', 'assetClass', 'countryOfRisk', 'couponRate', 'currencyCode', 'cusip', 'dateList', 'duration', 'exchange', 'holdingPercent', 'isin', 'issueDate', 'issueName', 'localFxRate', 'marketCurrencyCode', 'marketValue', 'maturityDate', 'modifiedDuration', 'notionalValue', 'parValue', 'realDuration', 'realYieldToMaturity', 'sectorName', 'sedol', 'unitPrice', 'yieldToCall', 'yieldToMaturity', 'yieldToWorst']
- `15:28:11` USHY latest: 1881 rows as of 2026-11-24, 1.4 MB; dates listed (4): ['2026-11-24', '2026-10-30', '2026-09-30', '2025-12-31']
- `15:28:15` CEMB latest: 882 rows as of 2026-11-24, 1.0 MB; dates listed (4): ['2026-11-24', '2026-10-30', '2026-09-30', '2025-12-31']
- `15:28:19` USHY asked 20260930 -> 1895 rows as of 2026-09-30 (1888 with a yield, 1895 with a price, 1895 with a duration), 1.4 MB
- `15:28:23` USHY asked 20251231 -> 1929 rows as of 2025-12-31 (1927 with a yield, 1929 with a price, 1929 with a duration), 1.4 MB
- `15:28:27` USHY asked 20251128 -> 1930 rows as of 2025-11-28 (1928 with a yield, 1930 with a price, 1930 with a duration), 1.4 MB
- `15:28:30` USHY asked 20250630 -> 1903 rows as of 2025-06-30 (1900 with a yield, 1903 with a price, 1903 with a duration), 1.4 MB
- `15:28:35` USHY asked 20241231 -> 1877 rows as of 2024-12-31 (1874 with a yield, 1877 with a price, 1877 with a duration), 1.4 MB
- `15:28:38` USHY asked 20231229 -> 1847 rows as of 2023-12-29 (1843 with a yield, 1847 with a price, 1847 with a duration), 1.4 MB
- `15:28:42` USHY asked 20221230 -> 1913 rows as of 2022-12-30 (1913 with a yield, 1913 with a price, 1913 with a duration), 1.4 MB
- `15:28:46` USHY asked 20201231 -> 2044 rows as of 2020-12-31 (2040 with a yield, 2044 with a price, 2044 with a duration), 1.5 MB
## 2. The CDS desk's bank

- `15:28:49` bank: 2.5 MB gzipped, keys ['days', 'entity_map', 'meta', 'series', 'version'], 594 days ['2024-09-02'] .. ['2026-11-24'], 1350 series
- `15:28:49` one series (NISSAN MOTOR): {"2025-09-16": {"n": 2, "n5": 1, "np": 1, "s": 312.01, "sb": "derived", "lo": 312.01, "hi": 312.01, "nm": 270.0, "cs": 0.0, "cap": 0.0, "q": 0, "amb": 0, "cv": {"3Y": 293.75, "5Y": 312.01}, "as": "single_branch", "aq": "firm", "pts": null, "cd": null}, "2025-09-24": {"n": 5, "n5": 5, "np": 2, "s": 359.21, "sb": "derived", "lo": 359.21, "hi": 359.21, "nm": 1380.0, "cs": 0.0, "cap": 0.0, "q": 0, "amb": 3, "cv": {"5Y": 359.21}, "as": "single_branch", "aq": "firm", "pts": null, "cd": {"100": [148.9, 53.5]}}, "2025-09-25": {"n": 1, "n5": 1, "np": 1, "s": 354.09, "sb": "derived", "lo": 354.09, "hi":
- `15:28:49` one meta (NISSAN MOTOR): {"kind": "corp", "raw": "NISSAN MOTOR CO.;LTD.", "ccy": {"JPY": 1308}}
- `15:28:49` series by number of points: over 400: 39, 200-400: 213, 60-200: 230, under 60: 868; most [(565, 'IDX:ITRAXX EUROPE CROSSOVER'), (565, 'IDX:ITRAXX EUROPE'), (563, 'IDX:ITRAXX EUROPE SENIOR FINANCIALS'), (555, 'IDX:CDX.NA.HY'), (554, 'IDX:CDX.NA.IG')]; least [(1, 'ABTB MID ATLANTIC RPS ONLY'), (1, 'ABTB LOUISIANA RPS ONLY'), (1, 'AA METALS')]
- `15:28:49` public history: 2.8 MB, keys ['as_of', 'engine', 'generated_at', 'long', 'names'], names 614
- `15:28:49` one name: ["REPUBLIC OF ECUADOR", {"points": [["2026-03-20", 492.65], ["2026-03-24", 471.98], ["2026-03-31", 500.0], ["2026-04-01", 491.69], ["2026-04-08", 436.0], ["2026-05-14", 395.63], ["2026-06-04", 392.14], ["2026-06-10", 371.61], ["2026-06-11", 359.72], ["2026-06-12", 348.2], ["2026-06-16", 350.45], ["2026-06-23", 369.47], ["2026-06-29", 364.31], ["2026-06-30", 357.07], ["2026-07-07", 364.58], ["2026-07-13", 369.18], ["2026-07-17", 382.44], ["2026-07-24", 385.18], ["2026-07-27", 397.96], ["2026-07-3
## 3. The factor book: what a credit analyst reads, and for how many common stocks

- `15:28:49` factor book columns (200): ['altman_z', 'asset_turnover', 'avg_surprise_pct_4q', 'avg_volume', 'beat_rate_4q', 'beta', 'book_value_per_share', 'buy', 'buyback_growth', 'capex_to_revenue', 'cash_conversion_cycle', 'cash_per_share', 'cik', 'close', 'composite_growth', 'composite_momentum', 'composite_quality', 'composite_squeeze', 'composite_street', 'composite_value', 'consensus_score', 'country', 'current_ratio', 'days_to_cover', 'dcf', 'dcf_score', 'dcf_upside_pct', 'de_score', 'debt_to_assets', 'debt_to_equity', 'dio', 'dividend_yield', 'dollar_vol_20d', 'dpo', 'dso', 'earnings_yield', 'ebitda_growth', 'ebitda_margin', 'effective_tax_rate', 'eps_growth', 'etf_holder_count', 'etf_weight_sum', 'ev', 'ev_ebitda', 'ev_sales', 'exchange', 'fcf_growth', 'fcf_per_share', 'fcf_yield', 'finra_days_to_cover', 'finra_short_interest', 'finra_si_change_pct', 'finra_vs_polygon_si_pct', 'float_shares', 'free_float_pct', 'from_52w_high_pct', 'graham_net_net', 'graham_number', 'gross_margin', 'gross_profit_growth', 'high_52w', 'hold', 'income_quality', 'industry', 'interest_coverage', 'inventory_turnover', 'invested_capital', 'ipo_date', 'is_adr', 'is_common', 'is_debt_or_structured', 'is_etf', 'is_fund', 'is_otc', 'is_preferred', 'is_shell', 'is_warrant_unit', 'last_dividend', 'last_surprise_pct', 'low_52w', 'mcap', 'n_peers', 'name', 'net_current_asset_value', 'net_debt_ebitda', 'net_income_growth', 'net_margin', 'operating_income_growth', 'operating_margin', 'outstanding_shares', 'p_fcf', 'p_ocf', 'payout_ratio', 'pb', 'pb_score', 'pct_altman_z', 'pct_beat_rate_4q', 'pct_consensus_score', 'pct_days_to_cover', 'pct_dcf_upside_pct', 'pct_dividend_yield', 'pct_dollar_vol_20d', 'pct_earnings_yield', 'pct_eps_growth', 'pct_ev_ebitda', 'pct_fcf_yield', 'pct_float_shares', 'pct_from_52w_high_pct', 'pct_gross_margin', 'pct_mcap', 'pct_net_debt_ebitda', 'pct_operating_margin', 'pct_p_fcf', 'pct_pb', 'pct_pe', 'pct_piotroski', 'pct_ps', 'pct_pt_upside_pct', 'pct_ret_126d', 'pct_ret_21d', 'pct_ret_252d', 'pct_ret_63d', 'pct_revenue_growth', 'pct_roe', 'pct_roic', 'pct_short_pct_float', 'pct_volatility_20d', 'pe', 'pe_score', 'peg', 'piotroski', 'price', 'ps', 'pt_avg_1m', 'pt_avg_1q', 'pt_avg_1y', 'pt_n_1m', 'pt_n_1q', 'pt_upside_pct', 'quick_ratio', 'rating_score', 'rd_to_revenue', 'ret_126d', 'ret_1d', 'ret_21d', 'ret_252d', 'ret_5d', 'ret_63d', 'revenue_growth', 'revenue_per_share', 'roa', 'roa_score', 'roe', 'roe_score', 'roic', 'sbc_to_revenue', 'sector', 'security_type', 'sell', 'shares_growth', 'short_interest', 'short_pct_float', 'short_volume_ratio', 'spct_altman_z', 'spct_beat_rate_4q', 'spct_consensus_score', 'spct_days_to_cover', 'spct_dcf_upside_pct', 'spct_dividend_yield', 'spct_dollar_vol_20d', 'spct_earnings_yield', 'spct_eps_growth', 'spct_ev_ebitda', 'spct_fcf_yield', 'spct_float_shares', 'spct_from_52w_high_pct', 'spct_gross_margin', 'spct_mcap', 'spct_net_debt_ebitda', 'spct_operating_margin', 'spct_p_fcf', 'spct_pb', 'spct_pe', 'spct_piotroski', 'spct_ps', 'spct_pt_upside_pct', 'spct_ret_126d', 'spct_ret_21d', 'spct_ret_252d', 'spct_ret_63d', 'spct_revenue_growth', 'spct_roe', 'spct_roic', 'spct_short_pct_float', 'spct_volatility_20d', 'strong_buy', 'strong_sell', 'tangible_asset_value', 'volatility_20d', 'working_capital']
- `15:28:57` factor book: 27198 rows, 5260 listed common stocks (not OTC), 386 of them ADRs, as of {"fmp-bank": "2026-11-24T23:25:22.770664+00:00", "fmp-walker": "2026-11-24T23:38:19.426145+00:00", "polygon-bank": "2026-11-24T22:53:02.090430+00:00", "float": "2026-11-24", "polygon-short-interest": "2026-11-13", "finra-short-interest": "2026-10-30", "polygon-tape": "2026-11-24"}
- `15:28:57`   altman_z                            5048 of 5260 common (96%) middle 1.482044892308002, p5 -14.511968631914446, p95 14.176329798394752
- `15:28:57`   beta                                5260 of 5260 common (100%) middle 0.939, p5 -0.24, p95 2.877
- `15:28:57`   buyback_growth                      5091 of 5260 common (97%) middle 0.0, p5 -4.92797695262484, p95 1.0
- `15:28:57`   capex_to_revenue                    5158 of 5260 common (98%) middle 0.020979071615191032, p5 0.0, p95 0.5619916465157178
- `15:28:57`   cash_conversion_cycle               5158 of 5260 common (98%) middle 27.53347493053456, p5 -1608.1748481966356, p95 396.2672290935877
- `15:28:57`   cash_per_share                      5158 of 5260 common (98%) middle 2.6760215708047563, p5 0.06738865216831005, p95 108.61069087241553
- `15:28:57`   consensus_score                     4325 of 5260 common (82%) middle 0.5882, p5 0.0, p95 1.0
- `15:28:57`   current_ratio                       5158 of 5260 common (98%) middle 1.719443942824465, p5 0.1884850055276062, p95 17.382425307557117
- `15:28:57`   dcf                                 3140 of 5260 common (60%) middle 14.358328066783145, p5 -16.842381439606967, p95 277.0854159984569
- `15:28:57`   dcf_score                           5150 of 5260 common (98%) middle 3.0, p5 1.0, p95 5.0
- `15:28:57`   dcf_upside_pct                      2497 of 5260 common (47%) middle -13.18, p5 -95.205, p95 275.7784
- `15:28:57`   de_score                            5150 of 5260 common (98%) middle 2.0, p5 1.0, p95 5.0
- `15:28:57`   debt_to_assets                      5158 of 5260 common (98%) middle 0.19297167536891216, p5 0.0, p95 0.754694055506715
- `15:28:57`   debt_to_equity                      5158 of 5260 common (98%) middle 0.3338926252265364, p5 -1.2989279840578145, p95 4.237783741189207
- `15:28:57`   dividend_yield                      5158 of 5260 common (98%) middle 0.0, p5 0.0, p95 0.07086614
- `15:28:57`   ebitda_growth                       5098 of 5260 common (97%) middle 0.052614271621177246, p5 -3.4537598003950025, p95 2.2989639959032537
- `15:28:57`   ebitda_margin                       5158 of 5260 common (98%) middle 0.09284327297390206, p5 -8.886371184232075, p95 0.6340112433862434
- `15:28:57`   ev                                  5151 of 5260 common (98%) middle 1173968041.0, p5 -2286941.565, p95 110093345763.0
- `15:28:57`   ev_ebitda                           5151 of 5260 common (98%) middle 6.82177984314924, p5 -25.64067084024522, p95 38.749669908983535
- `15:28:57`   ev_sales                            5151 of 5260 common (98%) middle 1.8303329221060987, p5 -0.010947032429769847, p95 22.828159700238476
- `15:28:57`   fcf_growth                          5091 of 5260 common (97%) middle 0.027435246917222256, p5 -3.6578847257862592, p95 3.3682405536162654
- `15:28:57`   fcf_per_share                       5158 of 5260 common (98%) middle 0.27584673575290747, p5 -10.155265980297322, p95 16.186336466883674
- `15:28:57`   fcf_yield                           5151 of 5260 common (98%) middle 0.01815316465824802, p5 -3.460692916544249, p95 0.2693009249842793
- `15:28:57`   finra_short_interest                5181 of 5260 common (98%) middle 2421641.0, p5 12760.0, p95 34706719.0
- `15:28:57`   float_shares                        5241 of 5260 common (100%) middle 39992884.0, p5 941453.0, p95 717925383.0
- `15:28:57`   gross_margin                        5158 of 5260 common (98%) middle 0.3909746867392133, p5 -0.0584892322982062, p95 0.9102752129090094
- `15:28:57`   interest_coverage                   5158 of 5260 common (98%) middle 0.47247325710070087, p5 -185.513698630137, p95 51.892911010558066
- `15:28:57`   is_debt_or_structured               5260 of 5260 common (100%) middle False, p5 False, p95 False
- `15:28:57`   last_dividend                       5260 of 5260 common (100%) middle 0.0, p5 0.0, p95 3.34
- `15:28:57`   mcap                                5260 of 5260 common (100%) middle 745591044.0, p5 2191153.0, p95 62347745007.0
- `15:28:57`   net_debt_ebitda                     5158 of 5260 common (98%) middle 0.7725239616613419, p5 -6.329346092503987, p95 12.980304099570155
- `15:28:57`   net_income_growth                   5098 of 5260 common (97%) middle 0.0427643349982108, p5 -5.578997237750058, p95 2.737681603788971
- `15:28:57`   net_margin                          5158 of 5260 common (98%) middle 0.018877702102457802, p5 -10.439782589669258, p95 0.3615702479338843
- `15:28:57`   operating_income_growth             5098 of 5260 common (97%) middle 0.07659286037053774, p5 -2.888853497701486, p95 2.1778776899908334
- `15:28:57`   operating_margin                    5158 of 5260 common (98%) middle 0.04799263117774107, p5 -8.860587803278198, p95 0.4434101559992296
- `15:28:57`   outstanding_shares                  5260 of 5260 common (100%) middle 54030921.0, p5 1787950.0, p95 827126342.0
- `15:28:57`   p_fcf                               5151 of 5260 common (98%) middle 3.655674733672997, p5 -41.66742965206635, p95 53.31568227099967
- `15:28:57`   payout_ratio                        5158 of 5260 common (98%) middle 0.0, p5 -0.049617228761853524, p95 1.0517677486262087
- `15:28:57`   pb_score                            5150 of 5260 common (98%) middle 2.0, p5 1.0, p95 5.0
- `15:28:57`   pct_altman_z                        5048 of 5260 common (96%) middle 50.01, p5 4.99, p95 95.03
- `15:28:57`   pct_consensus_score                 4325 of 5260 common (82%) middle 50.0, p5 5.0, p95 95.03
- `15:28:57`   pct_dcf_upside_pct                  2497 of 5260 common (47%) middle 50.0, p5 4.97, p95 95.07
- `15:28:57`   pct_dividend_yield                  5158 of 5260 common (98%) middle 50.01, p5 4.98, p95 95.04
- `15:28:57`   pct_ev_ebitda                       3501 of 5260 common (67%) middle 50.0, p5 5.0, p95 95.03
- `15:28:57`   pct_fcf_yield                       5151 of 5260 common (98%) middle 50.0, p5 4.99, p95 95.03
- `15:28:57`   pct_float_shares                    5241 of 5260 common (100%) middle 50.0, p5 5.0, p95 95.02
- `15:28:57`   pct_gross_margin                    5158 of 5260 common (98%) middle 50.01, p5 4.98, p95 95.04
- `15:28:57`   pct_mcap                            5260 of 5260 common (100%) middle 50.01, p5 5.0, p95 95.02
- `15:28:57`   pct_net_debt_ebitda                 5158 of 5260 common (98%) middle 50.01, p5 4.98, p95 95.04
- `15:28:57`   pct_operating_margin                5158 of 5260 common (98%) middle 50.01, p5 4.98, p95 95.04
- `15:28:57`   pct_p_fcf                           2823 of 5260 common (54%) middle 50.0, p5 5.0, p95 95.04
- `15:28:57`   pct_piotroski                       5061 of 5260 common (96%) middle 50.0, p5 5.0, p95 95.02
- `15:28:57`   pct_ret_126d                        4967 of 5260 common (94%) middle 50.0, p5 4.99, p95 95.03
- `15:28:57`   pct_ret_21d                         5159 of 5260 common (98%) middle 50.0, p5 4.98, p95 95.04
- `15:28:57`   pct_ret_252d                        4760 of 5260 common (90%) middle 50.01, p5 5.0, p95 95.02
- `15:28:57`   pct_ret_63d                         5097 of 5260 common (97%) middle 50.0, p5 4.98, p95 95.04
- `15:28:57`   pct_revenue_growth                  5098 of 5260 common (97%) middle 50.01, p5 4.98, p95 95.04
- `15:28:57`   pct_roic                            5158 of 5260 common (98%) middle 50.01, p5 4.98, p95 95.04
- `15:28:57`   pct_short_pct_float                 5217 of 5260 common (99%) middle 50.0, p5 4.98, p95 95.03
- `15:28:57`   pct_volatility_20d                  5114 of 5260 common (97%) middle 50.01, p5 4.99, p95 95.03
- `15:28:57`   pe_score                            5150 of 5260 common (98%) middle 1.0, p5 1.0, p95 4.0
- `15:28:57`   piotroski                           5061 of 5260 common (96%) middle 5.0, p5 1.0, p95 8.0
- `15:28:57`   quick_ratio                         5158 of 5260 common (98%) middle 1.3629191107587686, p5 0.17214813904830797, p95 16.777464126087978
- `15:28:57`   rating_score                           0 of 5260 common (0%) middle None, p5 None, p95 None
- `15:28:57`   rd_to_revenue                       5158 of 5260 common (98%) middle 0.0, p5 0.0, p95 2.47672828865244
- `15:28:57`   ret_126d                            4967 of 5260 common (94%) middle -12.1508, p5 -80.926, p95 63.6119
- `15:28:57`   ret_1d                              5196 of 5260 common (99%) middle 0.3037, p5 -9.1438, p95 9.2493
- `15:28:57`   ret_21d                             5159 of 5260 common (98%) middle -6.5934, p5 -47.3469, p95 27.883
- `15:28:57`   ret_252d                            4760 of 5260 common (90%) middle -4.0634, p5 -89.0055, p95 86.8608
- `15:28:57`   ret_5d                              5188 of 5260 common (99%) middle -2.0873, p5 -22.4171, p95 12.7611
- `15:28:58`   ret_63d                             5097 of 5260 common (97%) middle -14.386, p5 -69.3494, p95 35.6398
- `15:28:58`   revenue_growth                      5098 of 5260 common (97%) middle 0.041321518997705496, p5 -0.4427802162993801, p95 1.1045480862013508
- `15:28:58`   revenue_per_share                   5158 of 5260 common (98%) middle 11.335322939246542, p5 0.0, p95 216.99864860693327
- `15:28:58`   roa_score                           5150 of 5260 common (98%) middle 2.0, p5 1.0, p95 5.0
- `15:28:58`   roe_score                           5150 of 5260 common (98%) middle 2.0, p5 1.0, p95 5.0
- `15:28:58`   roic                                5158 of 5260 common (98%) middle 0.023440358751727793, p5 -1.3426811461138592, p95 0.24073961912527
- `15:28:58`   sbc_to_revenue                      5158 of 5260 common (98%) middle 0.007882409043731942, p5 0.0, p95 0.8479293313069909
- `15:28:58`   shares_growth                       5098 of 5260 common (97%) middle 0.008050949291035808, p5 -0.09469708878017484, p95 2.522431135404239
- `15:28:58`   short_interest                      5230 of 5260 common (99%) middle 2401430.0, p5 12218.0, p95 34633011.0
- `15:28:58`   short_pct_float                     5217 of 5260 common (99%) middle 5.1037, p5 0.2147, p95 28.9352
- `15:28:58`   short_volume_ratio                  5246 of 5260 common (100%) middle 0.4996, p5 0.2286, p95 0.7025
- `15:28:58`   spct_altman_z                       5048 of 5260 common (96%) middle 50.0, p5 4.9, p95 95.14
- `15:28:58`   spct_consensus_score                4325 of 5260 common (82%) middle 50.0, p5 4.86, p95 95.14
- `15:28:58`   spct_dcf_upside_pct                 2497 of 5260 common (47%) middle 50.0, p5 4.81, p95 95.26
- `15:28:58`   spct_dividend_yield                 5158 of 5260 common (98%) middle 50.0, p5 4.89, p95 95.12
- `15:28:58`   spct_ev_ebitda                      3501 of 5260 common (67%) middle 50.0, p5 4.84, p95 95.17
- `15:28:58`   spct_fcf_yield                      5151 of 5260 common (98%) middle 50.0, p5 4.9, p95 95.11
- `15:28:58`   spct_float_shares                   5241 of 5260 common (100%) middle 50.0, p5 4.92, p95 95.13
- `15:28:58`   spct_gross_margin                   5158 of 5260 common (98%) middle 50.0, p5 4.89, p95 95.12
- `15:28:58`   spct_mcap                           5260 of 5260 common (100%) middle 50.0, p5 4.93, p95 95.09
- `15:28:58`   spct_net_debt_ebitda                5158 of 5260 common (98%) middle 50.0, p5 4.89, p95 95.12
- `15:28:58`   spct_operating_margin               5158 of 5260 common (98%) middle 50.0, p5 4.89, p95 95.12
- `15:28:58`   spct_p_fcf                          2823 of 5260 common (54%) middle 50.0, p5 4.83, p95 95.17
- `15:28:58`   spct_piotroski                      5061 of 5260 common (96%) middle 50.0, p5 4.9, p95 95.1
- `15:28:58`   spct_ret_126d                       4967 of 5260 common (94%) middle 50.0, p5 4.91, p95 95.11
- `15:28:58`   spct_ret_21d                        5159 of 5260 common (98%) middle 50.0, p5 4.9, p95 95.11
- `15:28:58`   spct_ret_252d                       4760 of 5260 common (90%) middle 50.0, p5 4.9, p95 95.13
- `15:28:58`   spct_ret_63d                        5097 of 5260 common (97%) middle 50.0, p5 4.88, p95 95.13
- `15:28:58`   spct_revenue_growth                 5098 of 5260 common (97%) middle 50.0, p5 4.89, p95 95.11
- `15:28:58`   spct_roic                           5158 of 5260 common (98%) middle 50.0, p5 4.89, p95 95.12
- `15:28:58`   spct_short_pct_float                5217 of 5260 common (99%) middle 50.0, p5 4.87, p95 95.15
- `15:28:58`   spct_volatility_20d                 5114 of 5260 common (97%) middle 50.0, p5 4.91, p95 95.13
- `15:28:58`   volatility_20d                      5114 of 5260 common (97%) middle 67.6406, p5 21.1098, p95 205.662
- `15:28:58`   ORCL: {"altman_z": 1.4300704833351383, "beta": 1.778, "buyback_growth": 0.8626666666666667, "capex_to_revenue": 1.0541127953633527, "cash_conversion_cycle": -98.14642721521116, "cash_per_share": 12.500674308833446, "consensus_score": 0.6322, "current_ratio": 1.1708128130656228, "dcf": -22.39717175467773, "dcf_score": 1.0, "dcf_upside_pct": null, "de_score": 1.0, "debt_to_assets": 0.5141842451501851, "debt_to_equity": 2.335275265081172, "dividend_yield": 0.01841451, "ebitda_growth": 0.39875376380060223, "ebitda_margin": 0.5255099197503343, "ev": 450721994650.0, "ev_ebitda": 11.949468295819084, "ev_sales": 6.279564125195051, "fcf_growth": -59.11675126903553, "fcf_per_share": -9.683074848280512, "fcf_yield": -0.08672545133464538, "finra_short_interest": 46405138.0, "float_shares": 1860068780.0, "gross_margin": 0.6395731163620152, "interest_coverage": 4.517437304075235, "is_debt_or_structured": false, "last_dividend": 2.0, "mcap": 331129767400.0, "net_debt_ebitda": 3.1698083194146185, "net_income_growth": 0.3732218918267299, "net_margin": 0.2635978600089166, "operating_income_growth": 0.16562959610815703, "operating_margin": 0.3212355104770397, "outstanding_shares": 3023740000.0, "p_fcf": -11.434832917827297, "payout_ratio": 0.31390063424947146, "pb_score": 1.0, "pct_altman_z": 49.3, "pct_consensus_score": 55.11, "pct_dcf_upside_pct": null, "pct_dividend_yield": 75.06, "pct_ev_ebitda": 4
- `15:28:58`   CRWV: {"altman_z": 0.04557478410090916, "beta": 7.674, "buyback_growth": 1.0, "capex_to_revenue": 2.793674710982425, "cash_conversion_cycle": -167.10102793693903, "cash_per_share": 9.998211091234348, "consensus_score": 0.6364, "current_ratio": 0.4430685964984214, "dcf": null, "dcf_score": 1.0, "dcf_upside_pct": null, "de_score": 1.0, "debt_to_assets": 0.7238552267684, "debt_to_equity": 16.45192546583851, "dividend_yield": 0.0, "ebitda_growth": 4.141666666666667, "ebitda_margin": 0.34972320184375655, "ev": 84516423573.0, "ev_ebitda": 24.445576087038262, "ev_sales": 8.54918514007419, "fcf_growth": -0.21805973885041097, "fcf_per_share": -36.53130590339892, "fcf_yield": -0.8571456067941526, "finra_short_interest": 74211562.0, "float_shares": 354621453.0, "gross_margin": 0.667494748077667, "interest_coverage": -0.03136936410523712, "is_debt_or_structured": false, "last_dividend": 0.0, "mcap": 23693733957.0, "net_debt_ebitda": 17.554586921121214, "net_income_growth": -0.3522595596755504, "net_margin": -0.2555887655797588, "operating_income_growth": -1.1419753086419753, "operating_margin": -0.007646139392007044, "outstanding_shares": 556125685.0, "p_fcf": -1.149642277459478, "payout_ratio": -0.00012150110459147529, "pb_score": 1.0, "pct_altman_z": 29.23, "pct_consensus_score": 55.3, "pct_dcf_upside_pct": null, "pct_dividend_yield": 14.56, "pct_ev_ebitda": 14.57, "pct_fcf_yield": 11.55, "pct
- `15:28:58`   INTC: {"altman_z": 2.5479893747212063, "beta": 2.424, "buyback_growth": 0.0, "capex_to_revenue": 0.22881186135043077, "cash_conversion_cycle": 67.90603489509164, "cash_per_share": 10.298436308161708, "consensus_score": 0.2989, "current_ratio": 2.2080971226869184, "dcf": 2.6083376680074526, "dcf_score": 1.0, "dcf_upside_pct": -95.2403, "de_score": 2.0, "debt_to_assets": 0.21933378249494692, "debt_to_equity": 0.43627408322745703, "dividend_yield": 0.0, "ebitda_growth": 10.931837073981713, "ebitda_margin": 0.050307219662058374, "ev": 320054925000.0, "ev_ebitda": 106.22466810487886, "ev_sales": 5.343867711881387, "fcf_growth": 0.6838911599386817, "fcf_per_share": 0.7185354691075515, "fcf_yield": 0.012914417903948502, "finra_short_interest": 152855264.0, "float_shares": 4571707880.0, "gross_margin": 0.3989681426567822, "interest_coverage": 1.4781879194630871, "is_debt_or_structured": false, "last_dividend": 0.0, "mcap": 291262000000.0, "net_debt_ebitda": 9.388649186856954, "net_income_growth": 0.9857645553422905, "net_margin": -0.20089494423295265, "operating_income_growth": 0.810412741907861, "operating_margin": 0.0294196219862419, "outstanding_shares": 5315000000.0, "p_fcf": 77.15777708466031, "payout_ratio": -1.1917386968085106, "pb_score": 2.0, "pct_altman_z": 64.65, "pct_consensus_score": 20.47, "pct_dcf_upside_pct": 4.89, "pct_dividend_yield": 28.52, "pct_ev_ebitda": 3.17, "pct_fcf_
- `15:28:58`   AAPL: {"altman_z": 12.949218141500884, "beta": 1.089, "buyback_growth": 0.04463448798828845, "capex_to_revenue": 0.01888128244861476, "cash_conversion_cycle": -36.96836896642827, "cash_per_share": 3.8930158450178602, "consensus_score": 0.5625, "current_ratio": 1.0231998352117235, "dcf": 159.37016072691864, "dcf_score": 3.0, "dcf_upside_pct": -54.9891, "de_score": 2.0, "debt_to_assets": 0.19802594500528986, "debt_to_equity": 0.7146833432952995, "dividend_yield": 0.00304099, "ebitda_growth": 0.1954828390813352, "ebitda_margin": 0.36058425258022614, "ev": 5181665310000.0, "ev_ebitda": 30.010803370786515, "ev_sales": 10.821423102787186, "fcf_growth": -0.09227347505215656, "fcf_per_share": 9.337966993578329, "fcf_yield": 0.026481067656949915, "finra_short_interest": 144791472.0, "float_shares": 14514819489.0, "gross_margin": 0.48785591666423017, "interest_coverage": 0.0, "is_debt_or_structured": false, "last_dividend": 1.07, "mcap": 5145416054000.0, "net_debt_ebitda": 0.2537124985520676, "net_income_growth": 0.18002856887777877, "net_margin": 0.27603511864236874, "operating_income_growth": 0.18799699361142427, "operating_margin": 0.3300997840587761, "outstanding_shares": 14532200000.0, "p_fcf": 37.58223280296349, "payout_ratio": 0.11894079818422545, "pb_score": 1.0, "pct_altman_z": 94.55, "pct_consensus_score": 47.43, "pct_dcf_upside_pct": 28.57, "pct_dividend_yield": 63.43, "pct_ev_ebitd
- `15:28:58`   PLUG: {"altman_z": -6.214068576979781, "beta": 2.147, "buyback_growth": 0.0, "capex_to_revenue": 0.049870120395917844, "cash_conversion_cycle": 231.09154692023088, "cash_per_share": 0.09625614449043428, "consensus_score": 0.3158, "current_ratio": 2.2139912116306335, "dcf": null, "dcf_score": 5.0, "dcf_upside_pct": null, "de_score": 1.0, "debt_to_assets": 0.42228159045173824, "debt_to_equity": 1.2586599159082925, "dividend_yield": 0.0, "ebitda_growth": 0.198737814414295, "ebitda_margin": -1.4821177800879064, "ev": 2057259518.0, "ev_ebitda": -1.80822371506622, "ev_sales": 2.680000518476253, "fcf_growth": 0.3776643136502238, "fcf_per_share": -0.36416026512641275, "fcf_yield": -0.38947507247402346, "finra_short_interest": 280964711.0, "float_shares": 1335335065.0, "gross_margin": -0.030208406610441954, "interest_coverage": -4.686113228744533, "is_debt_or_structured": false, "last_dividend": 0.0, "mcap": 1302907518.0, "net_debt_ebitda": -0.6630360263121812, "net_income_growth": 0.22478584844118002, "net_margin": -1.536819630188345, "operating_income_growth": 0.6635532249025172, "operating_margin": -0.4564362704101173, "outstanding_shares": 1398870000.0, "p_fcf": -2.637577723913686, "payout_ratio": -0.013964389704292986, "pb_score": 2.0, "pct_altman_z": 9.73, "pct_consensus_score": 21.53, "pct_dcf_upside_pct": null, "pct_dividend_yield": 43.79, "pct_ev_ebitda": null, "pct_fcf_yield": 17.9,
- `15:28:58` common stocks by market value: over $10B 895, $2-10B 1063, $300M-2B 1256, under $300M 2046
## 4. The Treasury curve's depth and the price bank's depth

- `15:28:58` Treasury par curve: 9231 days ['1990-01-02'] .. ['2026-11-24'] (0.18 MB), tenors ['10Y', '1M', '1Y', '20Y', '2M', '2Y', '30Y', '3M', '3Y', '4M', '5Y', '6M', '7Y']
- `15:28:58` price bank ORCL: 38 KB, keys ['adjusted', 'fields', 'first', 'last', 'n', 'rows', 'sessions_folded_last', 'source', 'ticker', 'updated_at'], 1318 bars, first [1630008000000, 88.81, 89.18, 88.46, 88.72, 9093352.0, 88.7939, 65019], last [1795554000000, 111.38, 112.2499, 108.24, 109.5, 21071737.684635, 109.7007, 234649]
- `15:28:58` price bank SPY: 40 KB, keys ['adjusted', 'fields', 'first', 'last', 'n', 'rows', 'sessions_folded_last', 'source', 'ticker', 'updated_at'], 1318 bars, first [1630008000000, 448.61, 448.86, 446.16, 446.26, 57710772.0, 447.238, 441246], last [1795554000000, 715.148, 720.93, 712.14, 718.78, 64740484.868913, 717.0507, 953269]
- `15:28:58` price bank CRWV: 13 KB, keys ['adjusted', 'fields', 'first', 'last', 'n', 'rows', 'sessions_folded_last', 'source', 'ticker', 'updated_at'], 418 bars, first [1743192000000, 39, 41.94, 37.46, 40, 41091102.0, 39.5625, 265161], last [1795554000000, 46.65, 47.04, 41.22, 42.605, 61569838.106166, 43.0938, 508743]
- `15:28:58` price bank HYG: 33 KB, keys ['adjusted', 'fields', 'first', 'last', 'n', 'rows', 'sessions_folded_last', 'source', 'ticker', 'updated_at'], 1318 bars, first [1630008000000, 87.76, 87.77, 87.575, 87.72, 17676076.0, 87.6853, 43141], last [1795554000000, 76.3, 76.51, 76.3, 76.43, 56376712.047595, 76.4202, 56322]
## 5. Government curves for the books the board does not read

- `15:28:58` data/warm/treasury-par/      [('data/warm/treasury-par/curve.json.gz', 181382, '2026-11-24')]
- `15:28:58` data/warm/ecb-yc/            []
- `15:28:58` data/warm/ecb/               [('data/warm/ecb/catalog-summary.json', 929, '2026-08-18'), ('data/warm/ecb/catalog.json.gz', 4236, '2026-08-18'), ('data/warm/ecb/coverage.json', 10807, '2026-11-25'), ('data/warm/ecb/data/AGR.dat.gz', 31616, '2026-11-22'), ('data/warm/ecb/data/AME.dat.gz', 56263, '2026-11-22'), ('data/warm/ecb/data/BKN.dat.gz', 1887551, '2026-11-22'), ('data/warm/ecb/data/BLS.dat.gz', 7816244, '2026-08-18'), ('data/warm/ecb/data/BLS.manifest.json', 1318, '2026-11-11')] ...
- `15:28:58` data/warm/boe/               [('data/warm/boe/sonia-history.csv.gz', 39406, '2026-11-25')]
- `15:28:58` data/warm/boc/               []
- `15:28:58` data/warm/rba/               []
- `15:28:58` data/warm/bundesbank/        []
- `15:28:58` data/warm/sovereign/         []
- `15:28:58` data/warm/yield              []
- `15:28:58` data/warm/gov                []
- `15:28:58` banks under data/warm/ (90): ['_audit', 'archived-fred', 'asia-trade', 'backlog', 'banxico', 'bis', 'blackswan', 'bls-full', 'boe-full', 'boe', 'boj-full', 'bond-warroom', 'cds', 'census-econ', 'census-us', 'cftc', 'coinbase', 'dol-full', 'dtcc-fails', 'dtcc', 'eba-risk-dashboard', 'ecb-mmsr', 'ecb-sup', 'ecb', 'edgar-filings', 'eia-bank', 'eiopa', 'epu', 'esma-ratings', 'etf-history', 'eurostat', 'factor-book', 'fao-food-price-index', 'fdic-bankfind', 'fhfa-hpi', 'finra-full', 'fiscaldata-full', 'fmp-bank', 'fmp', 'fragile-states-index', 'frbddp-full', 'fred-canary', 'fred-catalog', 'fred-scoped', 'gdelt-full', 'gdelt', 'gleif', 'history-book', 'icma-sftr', 'imf-full', 'inst-public', 'katlin', 'nasa-power', 'nyfed-cmdi', 'nyfed-hhdc', 'nyfed-markets', 'nyfed-research', 'nyfed', 'oecd', 'official-yields', 'ofr-bsrm', 'ofr-fsi', 'ofr-hfm', 'ofr-site', 'ofr', 'playbooks', 'polygon-bank', 'polygon-full', 'portwatch', 'provider-bank', 'real-economy', 'repo-master', 'risk', 'sec-bulk', 'sec-dera', 'sec-ftd', 'sec-midas', 'statcan', 'te-mirror', 'te-yields', 'tic-full', 'treasury-auctions', 'treasury-par', 'treasury', 'tv-bars', 'us-equities-daily', 'usgov', 'world-uncertainty-index', 'worldbank-full', 'worldbank-wgi']
- `15:28:58` data/bond-warroom.json: 5785 KB, keys ['auction', 'donor_inputs', 'elapsed_s', 'equity_risk', 'eurodollar_shortage', 'flags', 'fleet', 'freshness', 'generated_at', 'heartbeat', 'jgb_curve', 'methodology', 'n_series', 'notes', 'panels', 'sources_doctrine', 'version']
- `15:28:58` data/sovereign-yields.json: NoSuchKey
- `15:28:58` data/global-yields.json: NoSuchKey
## 6. The board's ledger, and the rest of the bond desk document

- `15:28:58` ledger days (15): [('2025-12-31', 151532), ('2026-10-30', 181000), ('2026-11-05', 183595), ('2026-11-06', 183572), ('2026-11-09', 183608), ('2026-11-10', 183483), ('2026-11-11', 183449), ('2026-11-12', 183362), ('2026-11-13', 183275), ('2026-11-16', 183430), ('2026-11-17', 183754), ('2026-11-18', 183837), ('2026-11-19', 183908), ('2026-11-20', 184193), ('2026-11-23', 183901)]
- `15:28:59` bond desk document: 8.9 MB, keys {'engine': ('str', 19), 'version': ('str', 5), 'generated_at': ('str', 20), 'as_of': ('str', 10), 'read': ('str', 329), 'decision': ('dict', 3), 'coverage': ('dict', 5), 'sleeves': ('list', 0), 'trace': ('dict', 5), 'euro': ('dict', 9), 'cds_matched_issuers': ('int', 20), 'sources': ('dict', 5), 'issues': ('dict', 22)}
- `15:28:59` issues block keys: {'n_bonds': ('int', 23039), 'n_issuers': ('int', 5046), 'n_sectors': ('int', 32), 'n_with_rate': ('int', 21931), 'n_with_dur': ('int', 23011), 'n_with_price': ('int', 19197), 'n_with_change': ('int', 21453), 'n_with_move': ('int', 18866), 'n_with_flow': ('int', 23144), 'n_with_ytd': ('int', 15727), 'n_left': ('int', 150), 'dur': ('float', 4.766), 'demand': ('float', 138991300130.0), 'move_as_of': ('str', 10), 'ytd_as_of': ('str', 10), 'funds': ('list', 16), 'sectors': ('list', 33), 'issuers': ('list', 5046), 'bonds': ('list', 23189), 'as_of': ('str', 10), 'status': ('str', 4), 'note': ('str', 967)}
- `15:28:59` one bond: {"k": "013d1ac57bc931ba", "issuer": "1261229 BC LTD             144A", "sector": "Healthcare", "country": "United States", "ccy": "USD", "coupon": 10.0, "maturity": "2032-04-15", "issued": "2025-04-08", "rate": 8.43, "chg": -0.18, "chg_y": -0.42, "price": 104.52, "dpx": 0.43, "dpx_y": 0.52, "pct": 0.41, "demand": 234792641.0, "ddm": -4002455.0, "dur": 4.1, "nf": 4, "book": "US HY", "vs": 2.338}
- `15:28:59` ✅ AUDIT COMPLETE: nothing was invoked or written
