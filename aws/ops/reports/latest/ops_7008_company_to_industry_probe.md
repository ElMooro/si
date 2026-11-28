# 7008 - his company list, and what it places (read-only), 2026-11-28T14:57+00:00

**Status:** success  
**Duration:** 17.3s  
**Finished:** 2026-11-28T14:57:46+00:00  

## Data

| companies_listed | eva_not_placed | eva_placed | industries | unusable_betas |
|---|---|---|---|---|
| 41499 | 82 | 1418 | 94 | 110 |

## Log
## 1. His pages and the file

- `14:57:29` home.htm: 17007 bytes; links naming a company list: []
- `14:57:30` datacurrent.html: 59212 bytes; links naming a company list: ['databreakdown.html#company']
- `14:57:30` data.html: 8747 bytes; links naming a company list: ['dataarchived.html#company', 'databreakdown.html#company']
- `14:57:30` indname.html: HTTPError
- `14:57:41` pc/datasets/indname.xls: HTTP 200, 22255104 bytes, Last-Modified Thu, 08 Jan 2026 19:29:37 GMT, ETag "1539600-647e56df1be29"
- `14:57:43` sheets: By industry (48157 rows); By company name (48157 rows); By geography (48157 rows)
- `14:57:43`   By industry | Company Name | Exchange:Ticker | Industry Group | Primary Sector | SIC Code | Country | Broad Group | Sub Group
- `14:57:43`   By industry | 2performant Network S.A. ( | BVB:2P | Advertising | Communication Services | 7379 | Romania | Emerging Markets | Eastern Europe & Russia
- `14:57:43`   By industry | 4imprint Group plc (LSE:FO | LSE:FOUR | Advertising | Communication Services | 7330 | United Kingdom | Developed Europe | UK
- `14:57:43`   By industry | Able View Global Inc. (Nas | NasdaqCM:ABLV | Advertising | Communication Services | 8742 | China | Emerging Markets | China
- `14:57:43`   By company name | Company Name | Exchange:Ticker | Industry Group | Primary Sector | SIC Code | Country | Broad Group | Sub Group
- `14:57:43`   By company name | &Do Holdings Co.,Ltd. (TSE | TSE:3457 | Homebuilding | Consumer Discretionary | 1500 | Japan | Japan | Japan
- `14:57:43`   By geography | Company Name | Exchange:Ticker | Industry Group | Primary Sector | SIC Code | Country | Broad Group | Sub Group
- `14:57:43`   By geography | Ambitions Enterprise Manag | NasdaqCM:AHMA | Advertising | Communication Services | 7900 | United Arab Emirates | Emerging Markets | Africa and Middle East
- `14:57:44` the US sheet 'By industry': headings ['Company Name', 'Exchange:Ticker', 'Industry Group', 'Primary Sector', 'SIC Code', 'Country', 'Broad Group', 'Sub Group'] | 41499 companies with a ticker and an industry, 94 industries
- `14:57:44` exchanges in its tickers: [('TSE', 3846), ('BSE', 3289), ('SZSE', 2875), ('SEHK', 2417), ('SHSE', 2225), ('OTCPK', 2152), ('NSEI', 1877), ('KOSDAQ', 1747)]
## 2. Its industries against the banked industry table

- `14:57:44` wacc-us headings: ['Industry Name', 'Number of Firms', 'Beta', 'Cost of Equity', 'E/(D+E)', 'Std Dev in Stock', 'Cost of Debt', 'Tax Rate', 'After-tax Cost of Debt', 'D/(D+E)', 'Cost of Capital', 'Cost of Capital (Local Currency)']
- `14:57:44` industries in both: 94 | only in the company list: [] | only in the table: ['Total Market', 'Total Market (without financials)']
## 3. What it places

- `14:57:46` a factor book row carries: ['altman_z', 'beta', 'composite_quality', 'effective_tax_rate', 'ev', 'exchange', 'industry', 'invested_capital', 'is_adr', 'is_common', 'is_debt_or_structured', 'is_etf', 'is_fund', 'is_otc', 'is_preferred', 'is_shell', 'is_warrant_unit', 'mcap', 'name', 'piotroski', 'price', 'pt_upside_pct', 'revenue_per_share', 'roic', 'sector', 'security_type', 'symbol']
- `14:57:46` EVA's 1500 companies: 1418 placed by ticker (94.5%), 82 not: BRK-B (Financial Services), GOOG (Communication Services), SPCX (Industrials), CELG-RI (Healthcare), MRSH (Financial Services), EPD (Energy), MPLX (Energy), HONA (Industrials), HEI-A (Industrials), VYLR (Basic Materials), TEVA (Healthcare), P (Technology), SUNB (Industrials), CQP (Energy), CCJ (Energy), FWONK (Communication Services), TECK (Basic Materials), FWONA (Communication Services), VMRK (Real Estate), ARXS (Industrials), CBRS (Technology), LEN-B (Consumer Cyclical), ASND (Healthcare), FDXF (Industrials), JHX (Basic Materials), PAA (Energy), NWS (Communication Services), LOGI (Technology), ECHO (Communication Services), BF-A (Consumer Defensive), MKC-V (Consumer Defensive), PS (Financial Services), WSO-B (Industrials), WSE (Technology), STLA (Consumer Cyclical), MOG-B (Industrials), BIO-B (Healthcare), INIO (Industrials), UHAL-B (Industrials), EGO (Basic Materials)
- `14:57:46` betas outside 0.2 to 3, or absent: 110 of 1500 (negative 34, 0 to 0.2 45, above 3 31, absent 0); of them placed by his list: 93
- `14:57:46`   SPCX: beta 0.0, $1760bn, sector Industrials, his industry None
- `14:57:46`   ANT: beta 0.0, $1240bn, sector Technology, his industry Metals & Mining
- `14:57:46`   XOM: beta 0.175, $653bn, sector Energy, his industry Oil/Gas (Integrated)
- `14:57:46`   TTE: beta 0.05, $192bn, sector Energy, his industry Oil/Gas (Integrated)
- `14:57:46`   COP: beta 0.134, $162bn, sector Energy, his industry Heathcare Information and Technology
- `14:57:46`   LMT: beta 0.095, $140bn, sector Industrials, his industry Aerospace/Defense
- `14:57:46`   CELG-RI: beta -0.466, $126bn, sector Healthcare, his industry None
- `14:57:46`   SNDK: beta 5.647, $97bn, sector Technology, his industry Computers/Peripherals
- `14:57:46`   NOC: beta -0.112, $80bn, sector Industrials, his industry Aerospace/Defense
- `14:57:46`   CVNA: beta 3.65, $77bn, sector Consumer Cyclical, his industry Retail (Automotive)
- `14:57:46`   ALL: beta 0.137, $67bn, sector Financial Services, his industry Hotel/Gaming
- `14:57:46`   LNG: beta -0.008, $62bn, sector Energy, his industry Oil/Gas Distribution
- `14:57:46`   OXY: beta 0.169, $54bn, sector Energy, his industry Oil/Gas (Integrated)
- `14:57:46`   HONA: beta 0.08, $53bn, sector Industrials, his industry None
- `14:57:46` median usable beta by sector: {'Basic Materials': 1.21, 'Communication Services': 0.96, 'Consumer Cyclical': 1.14, 'Consumer Defensive': 0.51, 'Energy': 0.69, 'Financial Services': 0.85, 'Healthcare': 0.84, 'Industrials': 1.11, 'Real Estate': 0.94, 'Technology': 1.21, 'Utilities': 0.53}
- `14:57:46` a usable own beta against its industry's beta in his table: median distance 0.30 over 1325 companies
- `14:57:46` GF Value document: keys ['all_tickers', 'deepest_value', 'edge_basis', 'generated_at', 'methodology', 'most_overvalued', 'n_analyzed', 'n_deep_value', 'n_fair', 'n_modestly_overvalued', 'n_significantly_overvalued', 'n_undervalued', 'n_valid', 'sources', 'universe', 'universe_median_mos_pct', 'universe_state', 'version']
- `14:57:46`   deepest_value: 25 rows; a row: {"ticker": "CHTR", "price": 153.87, "gf_value": 967.7, "margin_of_safety_pct": 84.1, "rating": "DEEP_VALUE", "n_lenses": 2, "lenses_used": ["evebit", "graham"], "dcf_fair_value": null, "evebit_fair_value": 1436.73, "graham_number": 311.07, "price_5yr_low": 102.18, "price_5yr_high": 679.3, "dcf_meta": null, "evebit_meta": {"ebit_ttm": 13343000000, "fair_multiple": 22.4, "median_evebit_history": 22.4, "n_history": 10},
- `14:57:46`   most_overvalued: 25 rows; a row: {"ticker": "P", "price": 101.46, "gf_value": 36.43, "margin_of_safety_pct": -95.0, "rating": "SIGNIFICANTLY_OVERVALUED", "n_lenses": 3, "lenses_used": ["dcf", "evebit", "graham"], "dcf_fair_value": 74.29, "evebit_fair_value": 13.92, "graham_number": 7.35, "price_5yr_low": 22.22, "price_5yr_high": 156.97, "dcf_meta": {"fcf_ttm": 615741000.0, "growth_used": 15.0, "wacc": 0.09, "shares": 342992000, "net_debt": -13311760
- `14:57:46`   all_tickers: 494 rows; a row: {"ticker": "CHTR", "price": 153.87, "gf_value": 967.7, "margin_of_safety_pct": 84.1, "rating": "DEEP_VALUE", "n_lenses": 2, "lenses_used": ["evebit", "graham"], "dcf_fair_value": null, "evebit_fair_value": 1436.73, "graham_number": 311.07, "price_5yr_low": 102.18, "price_5yr_high": 679.3, "dcf_meta": null, "evebit_meta": {"ebit_ttm": 13343000000, "fair_multiple": 22.4, "median_evebit_history": 22.4, "n_history": 10},
- `14:57:46` GF Value's companies: 494 named in the document, 484 placed by his list; not placed: ['BNY', 'BRK-B', 'GOOG', 'HONA', 'MRSH', 'NWS', 'P', 'SKYD', 'VMRK', 'VYLR']
- `14:57:46` GF Value: version 1.0.1, generated 2026-11-27T22:02:14.892594+00:00, summary {"version": "1.0.1", "generated_at": "2026-11-27T22:02:14.892594+00:00", "universe": "S&P 500", "universe_state": "MARKET_SIGNIFICANTLY_OVERVALUED", "universe_median_mos_pct": -26.6, "n_analyzed": 503, "n_valid": 494, "n_deep_value": 21, "n_undervalued": 86, "n_fair": 88, "n_modestly_overvalued": 68, "n_significantly_overvalued": 231, "edge_basis": "Graham 1949 (margin of safety), Damodaran 2002 (multi-method DCF), Greenblatt 2010 (EV/EBIT > P/E for cyclicals), GuruFocus internal methodology. Co
- `14:57:46` GF Value methodology: {"wacc": 0.09, "terminal_multiple": 15.0, "projection_years": 10, "growth_cap_min": 0.03, "growth_cap_max": 0.15, "composite_weights": {"dcf": 0.4, "evebit": 0.35, "graham": 0.25}, "lenses": ["DCF (10yr FCF + 15x terminal at 9% WACC)", "EV/EBIT (10yr median multiple, outlier-pruned)", "Graham Number (sqrt(22.5 * EPS * BVPS))"], "rating_bands": {"DEEP_VALUE": ">= +50%", "MODESTLY_UNDERVALUED": "+20% to +50%", "FAIR": "-10% to +20%", "MODESTLY_OVERVALUED": "-30% to -10%", "SIGNIFICANTLY_OVERVALUED
- `14:57:46` ✅ read; nothing invoked, nothing written
