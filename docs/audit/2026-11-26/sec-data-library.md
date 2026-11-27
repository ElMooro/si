# The SEC's data library: audited, then seven data sets banked (2026-11-26 / 27)

Khalid, 2026-11-26 19:02 ET: "go and add as many files as my system needs especially the ones that would improve my
system and its data exponentially".

## What was there before

45 engines call sec.gov, one company or one filing at a time: the filings archive (23 engines), the company ticker
list (13), company filing feeds (13), full-text search (11), the filing index (8), company facts (4). Four bulk
sources were already kept: fails-to-deliver (`justhodl-equity-ftd`), the financial statement data sets and the EDGAR full
index (`justhodl-hist-banker`), company facts and submissions (`justhodl-sec-bulk`), and the market-structure metrics
by security and exchange (`justhodl-sec-midas`). No engine read fund holdings, money fund portfolios, or the BDC
schedules of investments.

## The audit (ops 6986, read-only)

The library has 23 data sets with files. Opened whole on the runner: the newest quarter of Form N-PORT (441 MB, 32
tables, 5.6 million holdings), the newest month of Form N-MFP (9.6 MB) and the largest recent BDC file (75 MB).

| Kept (7) | Files | All files | What it holds |
|---|---|---|---|
| Form N-PORT | 28 quarters since 2019 Q4 | 11.1 GB | every registered fund's holdings: bonds, loans, swaps, stock on loan |
| Form N-MFP | 100 (monthly now, quarter-wide at the start in 2010) | 1.1 GB | every money market fund's portfolio, repo counterparties, daily flows |
| BDC data sets | 23 | 0.5 GB | business development companies' schedules of investments |
| Form 13F | 54 quarters since 2013 | 2.8 GB | every institutional manager's holdings (11,852 filers a quarter) |
| Forms 3 / 4 / 5 | 83 quarters since 2006 | 0.9 GB | every insider transaction |
| Form D | 75 quarters since 2008 | 0.2 GB | private offerings (15,952 in 2026 Q3) |
| Form N-CEN | 33 quarters since 2018 | 0.3 GB | the fund census: authorized participants, custodians, lending agents |

Not kept: already held (fails-to-deliver, financial statements, market-structure metrics by security and exchange);
no desk would read them (Regulation A, crowdfunding, transfer agents, municipal advisors, adviser lists, variable
insurance, fund fee summaries, the four other market-structure summaries, statements with notes at 87 MB a month).

## The engine: `justhodl-sec-datasets` 1.0.0

Once a day at 22:47 UTC (Scheduler `justhodl-sec-datasets-daily`), 3008 MB, 900 s, 8 GB of local disk, no
self-chaining, no event, no invoke, and a failed run is not retried by Lambda (it waits for the next day).

1. Reads each data set's page on sec.gov and takes only links whose file name fits the set's own pattern.
2. Banks what the warehouse lacks, verbatim, with sha256 / pulled_at / the SEC's Last-Modified in the object's
   metadata: the newest file of every set first, then history newest first, 2.5 GB a run. The two newest files of
   each set are measured again every run: a different size, or a Last-Modified later than our pull, banks it again.
3. Reads banked files into tables: every month of N-MFP, one quarter of N-PORT a run.
4. Publishes `data/sec-money-funds.json`, `data/sec-fund-holdings.json` and the manifest
   `data/warm/sec-datasets/manifest.json`.

**Identifiers.** The raw files carry CUSIP and ISIN. They are kept verbatim under `sec-datasets/raw/<set>/`,
outside the site's `/data/` tree (a stranger gets 404 from the site and 403 from the bucket; ops 6988). Everything
under `data/` is identifier-free: a bond is issuer, title, coupon and maturity; names and titles are scrubbed of
anything shaped like an ISIN or a CUSIP; listed stocks keep their ticker. ops 6988 collected all 389,039 CUSIPs and
401,564 ISINs of the quarter and found none in the 20 published objects.

Tables per quarter of N-PORT, under `data/warm/sec-datasets/derived/nport/<file>/`:

| Table | Rows (2026 Q3 file, holdings of May to July 2026) | Columns |
|---|---|---|
| `bonds.json.gz` (13 MB) | 287,615 bonds, loans and asset-backed securities | issuer, title, cls, itype, ctry, cur, cpn, ck, mat, d, funds, rows, face, value, px, px_lo, px_hi, n_px, n_default, n_arrears, n_pik, n_restricted, n_l3 |
| `cds.json.gz` | 755 reference names, 89 indices, 10,583 positions, $177bn | reference, positions, funds, sold, bought, unclear, net_sold, not_in_dollars, usual_end_year |
| `stock-loan.json.gz` | 17,206 stocks funds hold $25m or more of | ticker, issuer, ctry, held, shares, on_loan, on_loan_pct, funds, funds_lending |
| `summary.json` | 13,880 funds, $46.1tn | asset classes with level-3 share, shareholder flows by month, what was set aside |

A month of N-MFP, under `derived/nmfp/<YYYY-MM>.json.gz` (October 2026: 329 funds, $9.06tn including 32 feeder
funds, $8.46tn without): fund types with maturity, yield and liquidity; holdings by category; repo $2.79tn by
counterparty (502 names; $1.20tn through the Fixed Income Clearing Corp, written as "FICC - sponsor"), by
collateral; unsecured lending $626bn by issuer (752); daily subscriptions and redemptions (22 days, +$68bn).

## What the dry runs taught (ops 6987, four runs)

1. **A month-end has two dates.** 255 funds dated October "31", 74 dated it "30" (the last business day). Taking
   the commonest date dropped a quarter of the industry: repo read $2.07tn instead of $2.79tn. A month is its
   year and month.
2. **One fund, one filing.** A file carries amendments; the latest filing of a fund's latest report date counts.
3. **Feeder funds double count.** 32 money funds hold $604bn of their own master funds.
4. **Filings carry units slips.** 57 holdings were "worth" more than 1.5 times their whole fund ($32.9bn): set
   aside, counted in the summary.
5. **Placeholder identifiers.** 723 "CUSIPs" carried three or more maturities; one tied 9,421 unrelated loans of
   46 funds into a $38bn "security". An identifier names one bond, so one maturity; a stock is held once or twice
   a fund. Rows under an identifier that breaks either rule are tied by name, coupon and maturity instead.
6. **No identifier is not no security.** Bonds issued outside the United States and most loans have no CUSIP: they
   are tied across funds only when written the same way, so one such bond can appear as several rows. 331,611
   single-fund holdings under $1m with no identifier (consumer loans, mostly) are set aside.
7. **A dollar value can equal a CUSIP.** The first identifier check failed on one nine-digit number. Identifiers
   are looked for in text, never in numbers.
8. **Flags are per position.** 1,997 securities carry a default flag from at least one fund; `rows` beside
   `n_default` gives the share of positions that flag it.
9. **Direction of a credit swap** is read from which leg is fixed: the fund receives the fixed payment = it sold
   protection. 526 positions name no reference; notionals not in dollars are counted, never added.
10. **Older file names differ** (`2013q4_d_0.zip`, `2024q1_ncen_0.zip`); the early quarter-wide N-MFP files hold
    three months and are banked but refused by the monthly reader, with the reason on record.

## First light (ops 6988, 2026-11-27 01:06 UTC, 0 failed checks)

One disclosed one-off run outside the schedule: 341 s, 52 files banked (2.44 GB), 13 months of money funds and one
quarter of holdings read, both documents published. Every table was read a second time on the runner from the banked
raw file and agreed (counts exactly, dollar sums within a rounding). The newest N-MFP file's sha256 was recomputed and
matched its record, and its size matched what sec.gov serves.

The history fills by itself: about six more daily runs bank the remaining 14.5 GB; one quarter of holdings is read a
run (27 to go). `ops_6989_sec_datasets_own_runs.py` reads the engine's own runs from the warehouse (read-only).

## Not built yet

No page reads these documents. Candidate desks, in the order proposed to Khalid: Credit Before Equity (who holds each
bond, the spread of fund marks, default flags, fund CDS positioning per company), Plumbing (who money funds lend to),
a stock-on-loan board. Also open: BDC tables (the schedule-of-investments file mixes totals with loans and needs its
own cleaning), readers for 13F / insiders / Form D / N-CEN, grouping unidentified bonds by ISIN on the runner side
without publishing it, a dealer-family roll-up of repo counterparties, the provider catalogue entry on data.html.
