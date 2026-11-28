# The Damodaran bank, and the EVA engine's premium (2026-11-28)

Khalid, 2026-11-27 22:40 ET, with a list of fifty sites: "any of these would be beneficial to use in my system?" and
then "add whatever my system needs".

## What the fifty came to

| Group | Sites | Verdict |
|---|---|---|
| Already feeding engines | SEC EDGAR, FRED, Federal Reserve, ECB, BIS, Trading Economics, Finviz, TradingView | nothing to add |
| Readers of filings taken at the source | Dataroma, WhaleWisdom, 13f.info, OpenInsider, Capitol Trades, ETF Database | nothing to add |
| Front ends over data already banked | Macrotrends, Stock Analysis, CompaniesMarketCap, Koyfin, AnnualReports, Morningstar, Simply Wall St | nothing to add |
| Tools for a person, no feed | Portfolio Visualizer, Curvo | nothing to add |
| Charts and essays | GMO, JPMorgan guide, ARK research | nothing to add (ARK's holdings have their own engine) |
| Reading | Buffett, Marks, Dalio, the blogs, the education sites | nothing an engine can use |
| **Data no engine read** | **Damodaran Online** | **added: this document** |
| **Data no engine read** | **AQR's data library** | **not added: its terms forbid it (below)** |

## AQR: read, and left out

ops 7001 and 7002 fetched two of AQR's workbooks on the runner and read the notices inside them. The notice in the
workbook (a text box, not a cell) says it may not be copied, reproduced, republished, posted, transmitted, disclosed,
distributed or disseminated, in whole or in part, without AQR's prior written consent, and that it is for educational
and informational purposes only. AQR's site terms say the same of anything taken from the site. JustHodl is a public
site with paid tiers. Nothing of AQR's was banked or published. It can be revisited with AQR's written consent.

## Damodaran: his terms, and what was built

His "About data" page says he shares the data with no strings attached, that an acknowledgement is welcome and not
required, and it lists what the data is poor at: real-time valuation of one company, historical research, court
cases, public policy. Every document the bank writes names him and his file and carries his caution.

**The gap that made it necessary.** `justhodl-eva-spread` priced every company's cost of equity with a fixed
`ERP_PCT = 5.50`, labelled "Damodaran 2026 implied ERP (hardcoded)". His page said 4.23% at the start of 2026 and
4.41% on 2026-11-01. No engine read his files.

### justhodl-damodaran 1.0.0

Once a day, 23:17 UTC (Scheduler `justhodl-damodaran-daily`), 512 MB, 600 s, no retry of a failed run, starts no
engine, publishes no event, calls his site and nothing else.

A run reads his home page and his current-data page (251 spreadsheets listed), asks "has it changed?" (ETag /
Last-Modified) of the files that move during the year (the monthly premium, the country premiums, the two histories,
the ratings table) and of all the others only once a week, when his page announces a new full update, or when a file
is not banked yet. A changed file is kept verbatim under `damodaran/raw/` (outside what the site serves) and the copy
it replaces under `damodaran/raw/_superseded/<file>.<date>`: he overwrites last year's files each January, and the
bank keeps them. Documents are rewritten only when their content changed.

| Key | What |
|---|---|
| `data/equity-risk-premium.json` | the implied premium today (five measures, the first is the headline), monthly since 2008-09, annual since 1960, annual returns since 1928 and the premiums worked from them; his home page's own figures beside the file's |
| `data/country-risk-premiums.json` | rating, default spread, total and country risk premium for each rated country, and the frontier markets under their own headings |
| `data/industry-benchmarks.json` | for each of his eight regions, one row an industry: cost of capital, margins, returns, multiples; and the list of every table |
| `data/warm/damodaran/tables/<topic>-<region>.json` | every industry table he publishes, all columns, under his headings (233) |
| `data/warm/damodaran/manifest.json` | each file's address, size, ETag, Last-Modified, hash, what it replaced; the last sixty runs |

The headline premium is his "ERP (T12 m with sustainable payout)": the figure his page states first, measured over
the ten-year Treasury bond rate, which is the risk-free rate the EVA engine uses.

### justhodl-eva-spread 1.2.0

Reads `us.premium_pct` from `data/equity-risk-premium.json` at the start of each run. A figure older than 75 days,
dated after today, or outside 1% to 12% is not used: the fixed 5.50% is, and `wacc_inputs.erp_fixed` and
`source_erp` say so. `wacc_inputs` gains `erp_as_of`, `erp_fixed`, `erp_age_days`, `erp_fixed_until_v1_2_pct`.

### Pages

`cost-of-capital.html` (`jh-cost-of-capital.js`): the premium today and its five measures beside his page's figures; the
premium through time (monthly since 2008, year ends since 1960; one axis, a key that switches lines, the month under
the pointer); every month and every year in a table; country risk premiums and frontier markets; industries by region;
every table he publishes, opened whole; stocks, bills and bonds since 1928; source, terms, cautions, files. Every list
sorts both ways on every column and saves as a file. `eva.html` shows where its premium was read from.

## Dry run (ops 7003, 2026-11-28 04:04 UTC, nothing written)

- 251 files, 21.7 MB, 99 s, peak memory 98 MB; 233 industry tables in eight regions; every benchmark column found.
- The monthly file's newest row equals his page's five figures for 2026-11-01: 4.41, 3.79, 5.81, 3.43, 3.39.
- Monthly 218 rows from 2008-09 (his file has no row for 2024-09); the headline measure starts 2016-01.
- Countries: `ctrypremJuly26.xlsx`, as of 2026-07-01; mature market 4.20%, United States 4.42%; 157 rated, 21 frontier.
- Worked from his annual returns, 1928-2025: stocks over bills 8.44% a year on average, 6.65% compounded; over bonds
  7.03% and 5.48%.

**EVA, the engine's own whole-market pass on the same 1,500 companies, same rates, premium 5.50% then 4.41%:**

| | on 5.50% | on 4.41% |
|---|---|---|
| Median cost of capital | 9.05% | 8.22% |
| Companies earning above it | 621 (41.4%) | 685 (45.7%) |
| Median EVA spread | -1.38 | -0.67 |
| Regime | MIXED | MIXED |
| Super compounders | 48 | 48 |
| The engine's fifty: earning above it | 36 | 36 |
| The engine's fifty: median spread | 4.69 | 5.48 |

Every company's cost of capital moves by its equity weight times its beta times the 1.09 points the premium fell
(0 of 1,500 off). 64 companies turn from below their cost of capital to above it (ADI, ETN, PH, CSX, UPS, HON among
the largest); none turns the other way.

## Found, not changed

- **Betas are used as the factor book holds them.** 34 of the 1,500 carry a negative beta and 31 a beta above 3
  (FDXF -7.34 gives a cost of capital of -25.7%). Such names rank at the top by EVA spread for the wrong reason.
- `justhodl-gf-value` discounts every company at a flat 9% (`WACC_DEFAULT`). It names no source and was left alone.
- The EVA engine asks FRED for the Treasury rate and the credit spread itself; when FRED does not answer it uses 4.41
  and 1.50 without saying so in the document.

## Proof

ops 7004: first light, read-back, EVA's run and the pages in a real browser on the live site. See its report.
