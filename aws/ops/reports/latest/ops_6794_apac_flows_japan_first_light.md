# ops 6794 - apac-flows 1.1.0 first light (Japan weekly + Korea daily) 2026-11-12T16:05+00:00

**Status:** success  
**Duration:** 36.5s  
**Finished:** 2026-11-12T16:06:20+00:00  

## Data

| fails | japan | jp_weeks | korea_today |
|---|---|---|---|
| [] | LIVE 2026-11-06 foreign -81.762 | 5 | 57.6 |

## Log
- `16:05:43` function: code last modified 2026-11-12T16:04:28.000+0000, sha SOaKzykGHI3K, timeout 300s
- `16:06:19` invoke: status 200, function error None, 36s, payload {"ok": true, "status": "LIVE", "sources": {"twse_t86": true, "twse_bfi": true, "korea_naver": true, "korea_marketwide": true, "japan_jpx": true, "hk_southbound": true}, "tw_stocks": 1087, "tw_foreign_col": "\u5916\u9678\u8cc7\u8cb7\u8ce3\u8d85\u80a1\u6578(\u4e0d\u542b\u5916\u8cc7\u81ea\u71df\u5546)", "tw_top_buy": "\u53f0\u73bb", "kr": "LIVE", "bridges": 4}
- `16:06:20` apac-flows.json: version 1.1.0, generated 2026-11-12T16:05:44+00:00 | japan status LIVE week_ending 2026-11-06 foreign_net -81.762 individual_net -443.52 investment_trusts 102.431 trust_banks 225.566 | files ['stock_1_w_20261005_20261009.xlsx', 'stock_1_w_20261013_20261016.xlsx', 'stock_1_w_20261019_20261023.xlsx', 'stock_1_w_20261026_20261030.xlsx', 'stock_1_w_20261102_20261106.xlsx'] | err None
- `16:06:20` korea market_wide: {"KOSPI": {"foreign_value": 4123.0, "institution_value": -6624.0, "individual_value": -2689.0, "as_of": "20261112"}, "KOSDAQ": {"foreign_value": -3547.0, "institution_value": 420.0, "individual_value": 3112.0, "as_of": "20261112"}, "total_foreign_value": 576, "unit": "KRW net (Naver index units)"}
- `16:06:20` history: 125 days (2026-07-03 -> 2026-11-12) | jp_weeks 5: ['2026-10-09', '2026-10-16', '2026-10-23', '2026-10-30', '2026-11-06'] | jp_unit JPY billions, net purchases by investor type (Value basis), JPX weekly
- `16:06:20` today's day row: {"kr_status": "LIVE", "kr_kospi_foreign_net_krw_bn": 412.3, "kr_kospi_institution_net_krw_bn": -662.4, "kr_kospi_individual_net_krw_bn": -268.9, "kr_kospi_as_of": "20261112", "kr_kosdaq_foreign_net_krw_bn": -354.7, "kr_kosdaq_institution_net_krw_bn": 42.0, "kr_kosdaq_individual_net_krw_bn": 311.2, "kr_kosdaq_as_of": "20261112", "kr_foreign_net_krw_bn": 57.6}
- `16:06:20` latest week 2026-11-06 TSE Prime nets (JPY bn): {"proprietary_cash": 217.97, "proprietary_margin": -141.074, "individuals_cash": -268.755, "individuals_margin": -174.765, "foreigners_institutions": -80.716, "foreigners_individuals": -1.046, "securities_cos": -35.512, "investment_trusts": 102.431, "business_cos": 300.757, "other_cos": 0.283, "life_nonlife_insurance": -52.422, "city_regional_banks": -65.143, "trust_banks": 225.566, "other_financials": -13.247, "foreigners": -81.762, "individuals": -443.52, "proprietary": 76.896, "financial_inst
- `16:06:20` ✅ Japan weekly flows LIVE (5 weeks stored), Korea daily nets stored
