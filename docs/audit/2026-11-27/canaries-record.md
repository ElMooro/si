# Canaries: tested against history, and read on that record (2026-11-27)

Khalid, 11:06 ET: "next improve the engine : canaries https://justhodl.ai/canaries.html and add an icon for it in
chart.html also test it against historical data to calibrate it".

## What was there (ops 6995, read-only)

- `justhodl-canary-warroom` (daily 20:50 UTC) reads twelve feeds into one board of 263 canaries. Its barometer is the
  mean of their 0-100 stress. Over its 135 days of history it had moved between 35 and 46: most canaries are
  percentiles or z-scores of their own past, so their mean sits near 45-50 whatever happens, and the fixed bands
  (40 / 60 / 80) were never reached. "Firing" at 50 or 55 on a percentile is true about half the time by construction.
- Nine of the twelve mechanisms vote; the dollar, global stress and eurodollar feeds were rewritten in September to
  describe only and send no canaries. The headline still said "across 12 mechanisms"; the page said "6".
- `justhodl-warroom-weights` (monthly) had not run since 1 October: the fleet freeze of the October cost event was
  lifted on 2 November 15:44 UTC, after its only run of the month. Its study weighs twelve mechanisms on stand-in
  series (VIX for volatility, three series for the macro grid) against eight hand-picked windows; it never tested a
  canary. A failed request there could log the first characters of a key (the URL was cut at 80 characters).
- The chart already had the button: **Cn Canaries**, on the bar since 2026-11-26, between Liquidity and Breadth.

## What was built

| Piece | What it does |
|---|---|
| `justhodl-warroom-weights` 2.0 (`canary_record.py`, `record_io.py`) | Rebuilds every canary whose series the warehouse holds, month by month, with its engine's own rule and only what was public that day; counts what the S&P 500 did next; writes `data/canary-record.json`. Reads the warehouse only (the site's resolver, one request at a time). The proxy study still runs beside it. |
| `scripts/build_canary_defs.py` | Writes the canaries' definitions from the engines' own sources (grid `SIGNALS`, plumbing `INDICATORS`, the war room's dedupe list); a deploy test fails if they drift. |
| `justhodl-canary-warroom` 2.0 | Adds `calibrated` (today's level and three-month turn on the record's scale, each with what followed readings like it), a `record` on every canary row, ids, a headline that counts voting mechanisms, `{"dry_run": true}`. Every earlier key is published to the figure. |
| `justhodl-canary-grid` | When FRED answers with fewer observations than asked, the older ones come from the warehouse. High-yield spread window 2,600 -> 1,300 observations. |
| `canaries.html` + `jh-canary-record.js` | Section "What the record says" above the board; the board is untouched. |

## How the record counts

- **The fall**: a close 10% or more under a month-end's close inside 126 sessions (also 20% inside 252), on the
  S&P 500's daily closes since 1927.
- **Early months**: month-ends with the index within 5% of its one-year high. A canary that only rises after the
  market has broken is a thermometer, so each is scored on these months.
- **Score**: area under the curve of stress against the fall (0.5 = chance); error by leaving one calendar year out.
- **No look-ahead**: an observation counts from its date plus the age of the series' newest observation today.
- Daily series are kept as the last observation of each ISO week, windows in daily observations divided by five.
  Rebuilt readings are checked against the engines' own today: the middle gap is 0.4 points over 114 canaries.

## What the record says (cut of 2026-11-25; 164 of 192 defined canaries rebuilt)

- Since the barometer starts: 646 month-ends, 138 followed by such a fall (21%); near a high 58 of 397 (15%).
- **The level does not warn.** Early score 0.51 +- 0.06. It confirms: 17% of months in its lowest band were followed
  by a fall, 57% in its highest - but near a high the bands say nothing.
- **Weighting canaries by their past record does not help** (0.49 walk-forward), and a barometer of only the canaries
  that had led by each January does not hold up either (0.41-0.47).
- **The three-month turn tilts the odds.** Each mechanism against itself three month-ends earlier, mechanisms weighted
  by how far their own turn's early score stood above chance in months already closed: early score 0.65 +- 0.06 over
  508 months since 1984; every month 0.67. Months followed by a fall: 11% after an easing turn, 19% steady, 36%
  rising, 31% rising fast (near a high: 6%, 15%, 25%, 20%). By period: 0.53 (1984-98), 0.69 (1998-2012), 0.73
  (2012-26). All four spans tried are published: 1 month 0.58, 3 months 0.65, 6 months 0.54, 12 months 0.45.
- By mechanism (turn, early): macro grid 0.63, plumbing 0.57, CISS 0.49 (no weight).
- **Verdicts**: 16 lead, 129 show no edge, 7 run the wrong way, 7 are stuck, 5 have a short record, 28 could not be
  rebuilt. The leaders that share a cause are credit: EM high-yield yield 0.77, EM corporate spread 0.72, EM
  high-yield spread 0.69, BBB-AAA 0.67, US high-yield spread 0.66. Stuck: levels on a trend scored as percentiles
  (bank deposits, bank securities, retail money funds, margin loans, the Fed's balance sheet). Wrong way: swap-line
  usage, Swiss unemployment, pulp and paper prices - they peak after the damage.
- With 164 tested, about eight would look like leaders by luck; series are as they stand today, not as first
  published; the three-month span is the best of four. A tilt in the odds, not a signal.

## Today (17:14 UTC)

Level 49.2, the 38th percentile of its history (lowest band: 55 of 326 months followed by a fall, 17%). Turn +6.2
points against 31 August, the 93rd percentile (rising fast: 16 of 52, 31%; near a high 4 of 20). The S&P 500 closed
8.1% under its one-year high on 25 November, after an 11.6% fall from 6 October to 20 November.

## Found on the way

1. **FRED hands out three years of the ICE BofA indices** (793 observations whatever is asked; ops 6997). Seven credit
   canaries were z-scored against three years where their rule says five: the high-yield spread read 94 against 55.
   Fixed in the grid (above); after its run the seven sit within 3 points of the record's rebuild.
2. **The banked S&P 500 file (`tv-bars/universe/I__SPX`) carries quarterly bars among its daily ones**: a row dated the
   first day of March, June, September or December holding that quarter's last close. 170 rows are taken out by the
   record before any fall is counted. The file itself was not changed (the chart reads it too).
3. **The warehouse copy of `IPG3344S` mixes two vintages** from July 2026 (181.9 then 131.1 after a rebasing). The
   record leaves that canary's newest 18 months out. Other revised series may carry the same seam after a rebasing.
4. The macro grid still asks FRED, the ECB and DBnomics itself for its 64 signals (over 100 requests a run).
5. Thirteen "global risk" canaries (HYG/LQD, fallen angels, EM volatility...) keep history only since 2025 in their
   feed, so they have no record; the ETF bars to rebuild them are in the bank.

## Proof

- ops 6998 dry run on the runner: 32 checks, 0 failed (the record from the live warehouse equals the offline build).
- ops 6999 go-live, 17:12-17:14 UTC: packages read back identical to the repository; three disclosed one-off runs
  (grid 43 s, record engine 99 s, war room 7 s); 19 checks, 0 failed.
- ops 7000 real browser on justhodl.ai, 17:18 UTC: 48 checks, 0 failed (figures equal the documents; 192 rows; sorting
  both ways; filter; saved file; phone; the chart's Canaries button opens the page).
- Tests: engines 19 + 8 + 3, page 9, deployment 1,272, node 5,349.

## Not done; his to decide

- Rebuild the other mechanisms through history (funding's 40 canaries, market leadership, volatility, factor, CFTC,
  the 13 global-risk canaries) so they join both readings; the framework takes a definition, not new code.
- Clean the banked index file itself; make the warehouse vintage-safe for rebased series.
- Move the macro grid's requests to the warehouse; take stuck canaries off the board or score them on change.
- A direction for the three silent mechanisms (dollar, global stress, eurodollar).
