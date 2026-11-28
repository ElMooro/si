# 7003 - the Damodaran bank, dry run on the runner; EVA before and after (nothing written), 2026-11-28T04:08+00:00

**Status:** failure  
**Duration:** 33.6s  
**Finished:** 2026-11-28T04:08:58+00:00  

## Error

```
SystemExit: 1
```

## Data

| countries | eva_companies | eva_median_wacc_after | eva_median_wacc_before | eva_turned_creator | failed_checks | listed | megabytes | premium_pct | tables | would_bank | writes |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 1500 | 8.22 | 9.05 | 64 |  |  |  |  |  |  |  |
| 180 |  |  |  |  | 1 | 251 | 3.8 | 4.41 | 18 | 27 | 0 |

## Log
## 1. The bank's own run, dry

- `04:08:41` [damodaran] 251 files listed, 0 in the bank, 27 asked about (the bank has never been swept)
- `04:08:41` [damodaran] {"ok": true, "version": "1.0.0", "dry_run": true, "at": "2026-11-28T04:08:25+00:00", "seconds": 16.4, "listed": 251, "asked": 27, "sweep": "the bank has never been swept", "swept": false, "stopped": null, "changed": ["ctryprem.xlsx", "ctrypremApr26.xlsx", "ctrypremJuly25.xlsx", "ctrypremJuly26.xlsx", "ERPbymonth.xlsx", "histimpl.xls", "histretSP.xls", "ratings.xls", "betaGlobal.xls", "betas.xls", "ctrypremJune13.xls", "EVA.xls", "EVAGlobal.xls", "margin.xls", "marginGlobal.xls", "pbvdata.xls", "pbvGlobal.xls", "pedata.xls", "peGlobal.xls", "psdata.xls", "psGlobal.xls", "roe.xls", "
- `04:08:41` summary: {"ok": true, "seconds": 16.4, "listed": 251, "asked": 27, "sweep": "the bank has never been swept", "stopped": null, "unchanged": 0, "banked_bytes": 3785072, "in_bank": 27, "groups": {"premium": "derived", "country": "derived", "industry": "derived"}, "wrote": 21, "peak_memory_mb": 75, "premium": {"as_of": "2026-11-01", "pct": 4.41}, "full_update": "January 9, 2026"}
- `04:08:41` changed (would be banked): 27 files, 3.8 MB
- `04:08:41` ✅ the run finished and wrote nothing (0 writes refused)
- `04:08:41` ✅ his two pages listed 251 spreadsheets; the 27 read here gave no trouble
- `04:08:41` ✅ all three groups of documents were derived: {'premium': 'derived', 'country': 'derived', 'industry': 'derived'}
## 1a. The premium

- `04:08:41` his page: 2026-11-01 -> [4.41, 3.79, 5.81, 3.43, 3.39] ['Trailing 12 month, with adjusted payout', 'Trailing 12 month cash yield', 'Average CF yield last 10 years', 'Net cash yield', 'Normalized Earnings & Payout']
- `04:08:41` the file's newest row: 2026-11-01 -> [('sustainable_payout', 4.41, 'ERP (T12 m with sustainable payout)'), ('cash_yield', 3.79, 'ERP (T12m)'), ('average_cf', 5.81, 'ERP (Smoothed)'), ('net_cash_yield', 3.43, 'ERP (Net Cash Yield)'), ('normalized', 3.39, 'ERP (Normalized)')]
- `04:08:41` headline: 4.41% over the ten-year US Treasury bond rate (T.Bond 4.94%), S&P 500 7580, expected return 9.35% | a month before {'date': '2026-10-01', 'value': 0.042, 'pct': 4.2} | a year before {'date': '2025-11-01', 'value': 0.0373, 'pct': 3.73}
- `04:08:41` monthly: 218 rows, 2008-09-01 to 2026-11-01 | headings: Start of month | S&P 500 | T.Bond Rate | $ Riskfree Rate | Ten-year average CF | CF (Trailing 12 month) | Normalized CF | Expected growth rate | ERP (T12 m with sustainable payout) | ERP (T12m) | ERP (T12m) with adj riskfree rate | ERP (Smoothed) | ERP (Normalized) | ERP (Net Cash Yield) | ERP (Covid Adjusted) | Expected Return | Notes
- `04:08:41` measure columns: {'sustainable_payout': 'erp_t12_m_with_sustainable_payout', 'cash_yield': 'erp_t12m', 'average_cf': 'erp_smoothed', 'net_cash_yield': 'erp_net_cash_yield', 'normalized': 'erp_normalized'}
- `04:08:41`   ['2026-09-01', 7686.14, 0.0475, 0.0453, 301.16, 201.83, 175.27, 0.1399, 0.0414, 0.0409, 0.0431, 0.0605, 0.0356, 0.038, None, 0.0884, None]
- `04:08:41`   ['2026-10-01', 7652, 0.0529, 0.0507, 306.5, 198.17, 176.54, 0.1459, 0.042, 0.037, 0.0392, 0.0567, 0.033, 0.034, None, 0.0899, None]
- `04:08:41`   ['2026-11-01', 7580, 0.0494, 0.0472, 306.5, 198.17, 176.54, 0.1552, 0.0441, 0.0379, 0.0401, 0.0581, 0.0339, 0.0343, None, 0.0873, None]
- `04:08:41` the headline measure starts 2016-01-01
- `04:08:41` months with no row in his file: ['2024-09'] | days of the month his rows are dated: ['01'] | two rows in one month: none
- `04:08:41` ✅ the five premiums his page states are each found in the monthly file
- `04:08:41` ✅ the file's newest row is the page's five figures, in the page's order: [4.41, 3.79, 5.81, 3.43, 3.39] against [4.41, 3.79, 5.81, 3.43, 3.39]
- `04:08:41` ✅ monthly since 2008-09-01, 218 rows, to the month of his page (2026-11-01)
- `04:08:41` annual: 66 rows, 1960 to 2025 | headings: Year | Earnings Yield | Dividend Yield | S&P 500 | Earnings* | Dividends* | Dividends + Buybacks | Change in Earnings | Change in Dividends | T.Bill Rate | T.Bond Rate | Bond-Bill | Smoothed Growth | Implied Premium (DDM) | Analyst Growth Estimate | Implied ERP (FCFE) | Implied ERP with risk adjusted riskfree rate | Implied Premium (FCFE with sustainable Payout) | ERP/Riskfree Rate
- `04:08:41`   [2024, 0.041369, 0.01248, 5881.63, 243.32, 73.4, 182.8, 0.099205, 0.047524, 0.0507, 0.0458, -0.0049, 0.046088, 0.0176, 0.0957, 0.0433, None, 0.04, 0.945415]
- `04:08:41`   [2025, 0.039664, 0.011469, 6845.5, 271.52, 78.51, 198.98, 0.115897, 0.069619, 0.0365, 0.0418, 0.0053, 0.046146, 0.0169, 0.105, 0.0423, 0.0446, 0.0418, 1.011962]
- `04:08:41` returns: 98 rows, 1928 to 2025, updated 2026-01-01 | headings: Year | Annual Returns on Investments in: S&P 500 (includes dividends) | Annual Returns on Investments in: US Small cap (bottom decile) | Annual Returns on Investments in: 3-month T.Bill | Annual Returns on Investments in: US T. Bond (10-year) | Annual Returns on Investments in: Baa Corporate Bond | Annual Returns on Investments in: Real Estate | Annual Returns on Investments in: Gold* | Value of $100 invested at start of 1928 in: S&P 500 (includes dividends)3 | Value of $100 invested at start of 1928 in: US Small cap (bottom decile) Cum Value | Value of $100 invested at start of 1928 in: 3-month T.Bill4 | Value of $100 invested at start of 1928 in: US T. Bond5 | Value of $100 invested at start of 1928 in: Baa Corporate Bond2 | Value of $100 invested at start of 1928 in: Real Estate2 | Value of $100 invested at start of 1928 in: Gold2 | Annual Risk Premium: Stocks - Bills | Annual Risk Premium: Stocks - Bonds | Annual Risk Premium: Small Cap Premium (S&P 500 minus Bottom Decile) | Annual Risk Premium: Stocks - Baa Corp Bond | Annual Risk Premium: Historical ERP | Annual Real Returns: Inflation Rate | Annual Real Returns: S&P 500 (includes dividends)2 | Annual Real Returns: US Small cap (bottom decile)22 | Annual Real Returns: 3-month T. Bill (Real) | Annual Real Returns: !0-year T.Bonds | Annual Real Returns: Baa Corp Bonds | Annual Real Returns: Real Estate3 | Annual Real Returns: Gold | Value of $100 invested at start of 1928 in real terms: S&P 500 (includes dividends)3 | Value of $100 invested at start of 1928 in real terms: US Small cap (bottom decile)22 | Value of $100 invested at start of 1928 in real terms: 3-month T.Bill4 | Value of $100 invested at start of 1928 in real terms: US T. Bond5 | Value of $100 invested at start of 1928 in real terms: Baa Corporate Bond2 | Value of $100 invested at start of 1928 in real terms: Real Estate2 | Value of $100 invested at start of 1928 in real terms: Gold2 | Real Risk Premium: Stocks minus T. Bills | Real Risk Premium: Stocks minus T.Bonds
- `04:08:41`   [2024, 0.248786, 0.087, 0.0518, -0.016372, 0.017364, 0.039634, 0.25957, 982817.816681]
- `04:08:41`   [2025, 0.177237, 0.1653, 0.0421, 0.077955, 0.069627, 0.015757, 0.662158, 1157009.087608]
- `04:08:41`   premiums worked here 1928-2025 (98 years): stocks over bills 8.44% arithmetic, 6.65% compounded; over bonds 7.03%, 5.48%; stocks 11.85%, 10.02%
- `04:08:41`   premiums worked here 1976-2025 (50 years): stocks over bills 8.80% arithmetic, 7.61% compounded; over bonds 6.56%, 5.79%; stocks 13.17%, 11.92%
- `04:08:41`   premiums worked here 2016-2025 (10 years): stocks over bills 13.50% arithmetic, 12.44% compounded; over bonds 14.52%, 13.79%; stocks 15.75%, 14.68%
- `04:08:41` ✅ annual premiums since 1960 (66 rows) and annual returns since 1928 (98 rows), with the premiums worked from them
## 1b. Countries

- `04:08:41` file ctrypremJuly26.xlsx, as of 2026-07-01 | mature market premium 4.20%, US 4.42%, equity-to-bond volatility 1.554496 | 180 countries | other files ['ctryprem.xlsx', 'ctrypremApr26.xlsx', 'ctrypremJuly25.xlsx']
- `04:08:41`   input: Enter the current risk premium for a mature equity market = 0.042 (Implied ERP for S&P 500 with adjusted dollar riskfree rate (for US Aa1)
- `04:08:41`   input: Enter the current risk premium for the US = = 0.0442 (US equity risk premium computed using implied ERP approach)
- `04:08:41`   input: If yes, enter the multiplier to use on the default spread (See workshe = 1.554496 (See "Relative Equity Volatilty" worksheet)
- `04:08:41`   ['Abu Dhabi', 'Middle East', 'Aa2', 0.00393, 0.04811, 0.00611, 0.0052, 0.050083, 0.008083]
- `04:08:41`   ['Albania', 'Eastern Europe & Russia', 'Ba3', 0.028634, 0.086512, 0.044512, None, None, None]
- `04:08:41`   ['Andorra (Principality of)', 'Western Europe', 'Baa1', 0.012726, 0.061783, 0.019783, None, None, None]
- `04:08:41`   ['Brazil', 'Central and South America', 'Ba1', 0.019932, 0.072984, 0.030984, 0.0208, 0.074334, 0.032334]
- `04:08:41`   ['China', 'Asia', 'A1', 0.005615, 0.050728, 0.008728, 0.0039, 0.048063, 0.006063]
- `04:08:41`   ['Germany', 'Western Europe', 'Aaa', 0.0, 0.042, 0.0, 0.0006, 0.042933, 0.000933]
- `04:08:41`   ['India', 'Asia', 'Baa3', 0.017499, 0.069202, 0.027202, 0.0072, 0.053192, 0.011192]
- `04:08:41`   ['Japan', 'Asia', 'A1', 0.005615, 0.050728, 0.008728, 0.0036, 0.047596, 0.005596]
- `04:08:41`   ['Turkey', 'Western Europe', 'Ba3', 0.028634, 0.086512, 0.044512, 0.027, 0.083971, 0.041971]
- `04:08:41`   ['United States', 'North America', 'Aa1', 0.002186, 0.0442, 0.002186, 0.0043, 0.048684, 0.006684]
- `04:08:41` ✅ 180 countries, a mature-market premium of 4.20%, and the United States among them (0.0442)
- `04:08:41`   outside 1% to 60%: ['Brunei', None, None, 0.007419, 0.004772, None, None, None, None]
- `04:08:41` countries with no premium: ['Frontier Markets (no sovereign ratings)', 'Country']
- `04:08:41` ✗ every country's total premium is a premium (1% to 60%): 1 outside, 2 without one
## 1c. Industries

- `04:08:41` as of 2026-01-05 | 18 tables | regions: US 9 tables, 96 industries, missing []; Global 9 tables, 96 industries, missing []
- `04:08:41` files that hold no industry table (0): 
- `04:08:41` topics (9): beta, eva, margin, pbv, pe, ps, roe, vebitda, wacc
- `04:08:41`   wacc-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of Firms | Beta | Cost of Equity | E/(D+E) | Std Dev in Stock | Cost of Debt | Tax Rate | After-tax Cost of Debt | D/(D+E) | Cost of Capital | Cost of Capital (Local Currency)
- `04:08:41`   margin-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | Gross Margin | Net Margin | Pre-tax, Pre-stock compensation Operating Margin | Pre-tax Unadjusted Operating Margin | After-tax Unadjusted Operating Margin | Pre-tax Lease adjusted Margin | After-tax Lease Adjusted Margin | Pre-tax Lease & R&D adj Margin | After-tax Lease & R&D adj Margin | EBITDA/Sales | EBITDASG&A/Sales | EBITDAR&D/Sales | COGS/Sales | R&D/Sales | SG&A/ Sales | Stock-Based Compensation/Sales | Lease Expense/Sales
- `04:08:41`   roe-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | ROE (unadjusted) | ROE (adjusted for R&D)
- `04:08:41`   eva-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of Firms | Beta | ROE | Cost of Equity | (ROE - COE) | BV of Equity | Equity EVA (US $ millions) | ROC | Cost of Capital | (ROC - WACC) | BV of Capital | EVA (US $ millions) | E/(D+E) | Std Dev in Stock | Cost of Debt | Tax Rate | After-tax Cost of Debt | D/(D+E)
- `04:08:41`   pe-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | % of Money Losing firms (Trailing) | Current PE | Trailing PE | Forward PE | Aggregate Mkt Cap/ Net Income (all firms) | Aggregate Mkt Cap/ Trailing Net Income (only money making firms) | Expected growth - next 5 years | PEG Ratio
- `04:08:41`   pbv-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | PBV | ROE | EV/ Invested Capital | ROIC
- `04:08:41`   ps-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | Price/Sales | Net Margin | EV/Sales | Pre-tax Operating Margin
- `04:08:41`   vebitda-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | EV/EBITDAR&D | EV/EBITDA | EV/EBIT | EV/EBIT (1-t) | EV/EBITDAR&D | EV/EBITDA | EV/EBIT | EV/EBIT (1-t)
- `04:08:41`   beta-us: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of firms | Beta | D/E Ratio | Effective Tax rate | Unlevered beta | Cash/Firm value | Unlevered beta corrected for cash | HiLo Risk | Standard deviation of equity | Standard deviation in operating income (last 10 years) | 2022 | 2023 | 2024 | 2025 | Average (2022-2026)
- `04:08:41`   wacc-global: 96 rows, updated 2026-01-05, header row of sheet 'Industry Averages' | Industry Name | Number of Firms | Beta | Cost of Equity | E/(D+E) | Std Dev in Stock | Cost of Debt | Tax Rate | After-tax Cost of Debt | D/(D+E) | Cost of Capital
- `04:08:41` US benchmarks: Industry | Companies | Beta | Cost of equity | Equity share of capital | Cost of debt, before tax | Tax rate | Cost of capital | Gross margin | Operating margin, before tax | Net margin | Return on equity | Return on capital | PE, trailing | PE, forward | Price to book | Price to sales | EV to sales | EV to EBITDA | EV to EBIT
- `04:08:41`   ['Advertising', 52.0, 1.210507, 0.093489, 0.713266, 0.052913, 0.050167, 0.078061, 0.362429, 0.100658, -0.003018, -0.013423, 0.277227, 44.433353, 52.872058, 4.551215, 1.642597, 2.124894, 11.998645, 20.908716]
- `04:08:41`   ['Aerospace/Defense', 79.0, 0.945491, 0.081669, 0.865332, 0.052913, 0.115753, 0.076015, 0.174846, 0.086546, 0.049891, 0.15269, 0.160124, 92.800315, 45.86731, 7.880461, 3.172719, 3.570707, 21.583913, 34.787926]
- `04:08:41`   ['Semiconductor', 66.0, 1.518199, 0.107212, 0.974743, 0.052913, 0.051064, 0.105506, 0.589721, 0.353278, 0.30446, 0.313647, 0.27227, 100.184403, 37.292657, 13.312176, 15.462268, 15.700649, 34.753099, 44.298631]
- `04:08:41`   ['Semiconductor Equip', 31.0, 1.397223, 0.101816, 0.953641, 0.052913, 0.099621, 0.098936, 0.463214, 0.261712, 0.213155, 0.35778, 0.284045, 46.825234, 41.583718, 9.702594, 7.489075, 7.607009, 24.743246, 28.810672]
- `04:08:41`   ['Software (System & Application)', 309.0, 1.276648, 0.096439, 0.947175, 0.052913, 0.055098, 0.09344, 0.717207, 0.329825, 0.25493, 0.296249, 0.293184, 79.174209, 34.133893, 9.13709, 11.007221, 11.408764, 24.483536, 32.412339]
- `04:08:41`   ['Total Market', 5994, 0.912111, 0.08018, 0.739802, 0.052913, 0.083017, 0.069643, 0.377574, 0.128175, 0.097381, 0.172124, 0.089559, 57.863855, 27.65973, 4.605158, 3.072484, 3.966336, 19.731928, 29.124354]
- `04:08:41`   ['Total Market (without financials)', 4822, 0.991296, 0.083712, 0.852602, 0.052913, 0.071033, 0.077222, 0.343888, 0.131356, 0.085584, 0.17602, 0.15071, 61.558599, 30.889185, 5.097032, 3.029109, 3.460309, 16.954952, 25.696739]
- `04:08:41` ✅ US: 9 tables and 96 industries; Global: 9 tables (of the files read here)
- `04:08:41` ✅ every benchmark column is found for the US and for Global (missing: [] / [])
- `04:08:41` ✅ the US total market's cost of capital reads 6.96%
- `04:08:41` document sizes: premium 78825, countries 16020, industries 44125 bytes; 18 tables, 222009 bytes in all
## 2. EVA: the engine's own whole-market pass, on 5.50% and on the bank's premium

- `04:08:41` the live document: generated 2026-11-28T00:45:27.295429+00:00, version 1.1.0 | Rf 4.85, credit spread 1.24, premium 5.5 (Damodaran 2026 implied ERP (hardcoded))
- `04:08:42` the engine here: version 1.2.0, factor book available, statement bank present
- `04:08:58` live document's market block: 1500 valid, 623 creators (41.5%), medians {'eva_spread_pct': -1.36, 'roic_pct': 7.55, 'wacc_pct': 9.02}, regime MIXED, book {'fmp-bank': '2026-11-26T23:25:18.164400+00:00', 'fmp-walker': '2026-11-26T23:38:06.663737+00:00', 'polygon-bank': '2026-11-26T22:51:43.393667+00:00', 'float': '2026-11-26', 'polygon-short-interest': '2026-11-13', 'finra-short-interest': '2026-11-13', 'polygon-tape': '2026-11-25'}
- `04:08:58` on 5.50%% (today's rule): 1500 valid, 621 creators (41.4%), 879 destroyers, medians {'eva_spread_pct': -1.38, 'roic_pct': 7.54, 'wacc_pct': 9.05}, regime MIXED, super compounders 48, book {'fmp-bank': '2026-11-27T23:26:30.950043+00:00', 'fmp-walker': '2026-11-27T23:38:17.026213+00:00', 'polygon-bank': '2026-11-27T22:51:55.474961+00:00', 'float': '2026-11-27', 'polygon-short-interest': '2026-11-13', 'finra-short-interest': '2026-11-13', 'polygon-tape': '2026-11-27'}
- `04:08:58` on 4.41% (the bank's): 1500 valid, 685 creators (45.7%), 815 destroyers, medians {'eva_spread_pct': -0.67, 'roic_pct': 7.54, 'wacc_pct': 8.22}, regime MIXED, super compounders 48, book {'fmp-bank': '2026-11-27T23:26:30.950043+00:00', 'fmp-walker': '2026-11-27T23:38:17.026213+00:00', 'polygon-bank': '2026-11-27T22:51:55.474961+00:00', 'float': '2026-11-27', 'polygon-short-interest': '2026-11-13', 'finra-short-interest': '2026-11-13', 'polygon-tape': '2026-11-27'}
- `04:08:58` ✅ both passes ran on the same 1500 companies
- `04:08:58` ✅ on the fixed premium the pass runs on a newer factor book than the live document's, so the two are not compared
- `04:08:58` cost of capital, per company: falls by a median 0.83 points (from -6.18 to 7.15 at the least and most); median 9.05% before, 8.22% after
- `04:08:58` companies that turn from below their cost of capital to above it: 64 of 1500: ADI (-0.59 to 0.72), ETN (-0.87 to 0.20), PH (-0.10 to 0.98), CSX (-0.19 to 0.83), ELV (-0.56 to 0.07), UPS (-0.48 to 0.33), JCI (-0.56 to 0.69), HON (-0.36 to 0.47), URI (-1.40 to 0.19), EW (-0.34 to 0.57), LITE (-0.56 to 1.23), VST (-0.92 to 0.12), GEHC (-0.63 to 0.07), IR (-1.07 to 0.08), DOV (-0.62 to 0.51), STE (-0.51 to 0.38), PHM (-0.24 to 1.00), TROW (-0.30 to 1.29), JBHT (-0.30 to 1.00), SNX (-1.22 to 0.01), NVT (-1.37 to 0.04), JLL (-1.12 to 0.15), IEX (-0.37 to 0.60), LAMR (-0.52 to 0.52), SMCI (-1.78 to 0.36)
- `04:08:58` the other way: 0
- `04:08:58` top ten by spread before: MDT, ESS, KIM, MANH, SNDK, BKNG, FICO, APP, MEDP, NP
- `04:08:58` top ten by spread after:  MDT, ESS, KIM, MANH, SNDK, BKNG, FICO, APP, MEDP, STX
- `04:08:58` ✅ every company's cost of capital moves by its equity weight times its beta times the 1.09 points the premium fell: 0 of 1500 off
- `04:08:58` found, not changed: 34 companies carry a negative beta in the factor book (their cost of capital rises when the premium falls) and 31 a beta above 3; a beta is used as it is: FDXF beta -7.34, cost of capital -25.73% -> -19.69%; OCTV beta -5.93, cost of capital -26.26% -> -20.08%; MMED beta -3.71, cost of capital -15.53% -> -11.49%; PS beta 9.16, cost of capital 4.84% -> 4.84%; CRWV beta 7.67, cost of capital 16.98% -> 14.57%; SEZL beta 6.60, cost of capital 40.94% -> 33.79%
- `04:08:58` the engine's fifty (48 in the book): creators 36 before, 36 after; median cost of capital 9.07% to 8.24%; median spread 4.69 to 5.48
- `04:08:58`   AAPL: beta 1.089, cost of capital 10.79% -> 9.61%, spread 42.43 -> 43.60
- `04:08:58`   MSFT: beta 1.045, cost of capital 10.43% -> 9.32%, spread 9.97 -> 11.08
- `04:08:58`   NVDA: beta 2.247, cost of capital 17.18% -> 14.73%, spread 45.67 -> 48.11
- `04:08:58`   GOOGL: beta 1.226, cost of capital 11.43% -> 10.12%, spread 3.38 -> 4.68
- `04:08:58`   GOOG: beta 1.215, cost of capital 11.36% -> 10.07%, spread 3.44 -> 4.73
- `04:08:58`   AMZN: beta 1.503, cost of capital 12.51% -> 10.99%, spread -4.29 -> -2.77
- `04:08:58` ✗ 1 check(s) failed
