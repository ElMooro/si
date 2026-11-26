# Credit Before Equity 3.0: a distressed switch, 1,238 more companies, what the stock did next, every figure on the page

Khalid, 2026-11-25 10:20 ET: "i like it you should add an icon that would sort distrissed. also you should increase the
coverage to include a lot more companies and think of different ways and addition to improve this engine so that it can
predict what would happen to the stock of these companies. also make sure all the data from the engines is viewable in the
iux"

Four things were asked: (1) one control that shows the distressed companies, (2) a lot more companies, (3) ways for the
engine to say what happens to a company's stock, (4) everything the engine publishes viewable on the page.

Yesterday's record (2.0 and 2.1) is `docs/audit/2026-11-24/credit-before-equity.md`. Nothing it describes was renamed, moved
or removed; 3.0 is added beside it.

## The short answer to "can it predict the stock"

It was measured before anything was built, on the board's own history, and the answer has two halves.

- **Which way the stock goes next: no reading of this board tells it.** Not the leads, not the nine combinations of what
  credit and the stock just did, not the stress rank, not the month's or the quarter's spread change, not the traded CDS
  week by week. Of 22 tests none is past chance. The page now says so, beside the leads.
- **How big the stock's next fall can be: the spread tells it, and tells more than the stock's own volatility does.** The
  share of stocks that were at some point 20% or more under where they started, inside three months, by how wide their bonds
  traded: under 100 bp 9.3%, 100 to 200 bp 14.2%, 200 to 400 bp 22.7%, 400 to 700 bp 33.7%, 700 to 1,000 bp 41.2%, 1,000 bp
  or more 52.1% (the whole board: 16.5%). Checked walk-forward on months the table had not seen: said 5.3 / 12.7 / 27.0 /
  39.8%, happened 4.7 / 12.4 / 26.7 / 37.3%.

So the engine does not publish a direction it cannot support. It publishes, for each listed company, **the chance of a fall
of 20% or more inside one month and inside three**, the range its stock has moved in, and the record those rest on; and it
keeps each day's chances as they were said, to be scored against what happens.

## Audit first (read-only unless said)

- **ops 6949** (15:28 UTC, read-only). The bond desk's fund files still serve older month-ends when asked for by date. The
  board's own ledger held 15 days, two of them worked back. The warehouse already banks government yields for Germany, the
  UK, Canada and Australia (`data/warm/official-yields/`, kept by the bond war room), and 5,339 of the bonds the bond desk
  holds are not in dollars: the board had been leaving them out.
- **ops 6950** (15:33 to 15:55 UTC; **this one wrote**, and removed nothing). The month-end holdings of the four corporate
  bond funds the bond desk already scans, July 2021 to October 2026, fetched once on the runner and kept as 64 new files
  under `credit/cbe-holdings/<day>.json.gz` (36.5 MB in all, no identifiers), with each of those days' spreads added to the
  board's own ledger, `credit/cbe-spreads/`. The ledger's two worked-back days (2025-12-31, 2026-10-30) were replaced by
  the real ones.
- **ops 6951** (16:05 UTC, read-only). The board rebuilt at each of those month-ends with the engine's own arithmetic and
  each listed company's reading set beside its stock over the next 21 and 63 sessions: 62,444 company-months of 1,149
  companies over 63 month-ends (2022-06-30 was served without yields - 2,534 of 11,757 bonds with a spread - and is left
  out). The traded CDS the same way, weekly: 236 names, 111 weeks, 12,031 name-weeks.

### What the history says

Returns are measured against the board's average stock over the same days, entered at the close one session after the
holdings day (a fund publishes a day's holdings the next morning). "How sure" is the month-by-month figure over its standard
error; with 22 tests at once the bar is 3.

| Reading | Next month | How sure | Company-months |
|---|---|---|---|
| Lead down (credit worse, stock has not fallen) | -0.06% | 0.5 | 1,836 |
| Lead up (credit better, stock has not risen) | +0.42% | 0.45 | 1,874 |
| Credit worse, stock already fell | +0.39% | 0.6 | 3,153 |
| Stress rank 90 to 100 | +0.68% | 0.67 | 1,110 |
| Spread 1,000 bp or more | +3.54% | 1.25 | 482 |
| CDS wider over the week (next week) | +0.03% | 0.36 | 1,520 name-weeks |

None of the 22 is past 3. Rank correlations between any reading and the next return are all under 0.035.

The size of the move is another matter. Inside the most volatile third of stocks, those with a spread of 300 bp or more fell
20% inside three months 16.3 points more often than those under 150 bp (how sure: 10.5). Once the spread band and the
stock's own volatility third are fixed, nothing else the board reads adds to it: not the month's or the quarter's change,
not the curve, not the bond price, not what comes due.

## What changed

### Engine `justhodl-credit-before-equity` 3.0.0 (contract still `credit-before-equity.v2`)

The warehouse only; no provider is asked for anything. Same schedule, once a day at 22:35 UTC. Memory 2,048 MB to 3,008 MB,
timeout 900 s (the record, when it is redone, took 146 s inside Lambda). Every key of 1.0, 2.0 and 2.1 is published and
their 37 columns keep their places, with 32 new ones after them.

- **More companies: 4,184 (were 2,946).** Bonds in euros, sterling, Canadian and Australian dollars are read against their
  own government's banked yields and stand in two books of their own, *Non-dollar investment grade* (942 companies) and
  *Non-dollar high yield* (297), so a euro spread is never ranked against a dollar one. By currency: USD 2,945, EUR 911, GBP
  157, CAD 130, AUD 41. A company with a dollar bond the board can read stays read in dollars exactly as before.
- **What a company's bonds say besides the spread**: the share that comes due inside two years, the average maturity and
  coupon, the average and lowest price, near bonds against far ones (the company's own curve), and the chance of default
  the spread prices over one year and five at 40 cents recovered.
- **The nine cells**: credit worse / unmoved / better against stock fell / flat / rose, for every listed company, with the
  count in each.
- **The record** (`cbe_study.py`): the study above, inside the engine. Redone when a month has finished - inside the daily
  run, never on a schedule of its own - and published as `data/credit-before-equity-evidence.json` with a month-by-month
  history of every company's spread, `data/credit-before-equity-history.json`. Each day's holdings are kept so the history
  grows by itself.
- **The chance of a fall**, for the 1,083 listed companies in the dollar books: how often the record saw a stock fall 20%
  for its band of spread and its own volatility third, inside one month and inside three, with the range one stock in ten
  did worse than and one in ten did better than.
- **A track record that starts today**: each day's chances are kept as they were said (`credit/cbe-calls/<day>`) and scored
  when their 21 and 63 sessions are up (`credit/cbe-track.json`). The first score is due in 21 sessions.
- **Every bond the board read**, under its company, with no identifier: `data/credit-before-equity-bonds.json` (21,106
  rows).

### The page - `credit-before-equity.html`, `jh-credit-board.js`, new `jh-credit-sheet.js`

- **Distressed**: a switch with a warning icon and the count on the table of every company and on the stress view, and a
  tile in the summary. One click shows the companies at 1,000 bp or more, widest first; another click puts the table back.
- **A sheet for any company**: click a name anywhere on the page (the table, the stress view, a lead, the CDS table, the
  map's detail). Every figure the engine has on it, each with what it means; its spread at every month-end since 2021
  against the middle company of its book, on a chart; every one of its bonds, sortable; its CDS and its stock; what stocks
  that read this way did next.
- **What the stock did next** (section 12): the record, in words and figures: the verdict on direction, the share that
  fell by spread band, the table by spread and volatility, the walk-forward check, the nine cells, the traded CDS, what the
  record cannot see.
- **Fall risk 3M**: a column in the table and the stress view, a tile in the summary, a choice in *Show*.
- **Every column** (69) beside *The board's columns*; **Download as a file** (CSV) for the table and for a company's bonds.
- **Non-dollar IG / Non-dollar HY** beside the three books, on the map and in the tables.
- **Everything the engine published** (last section): the whole document, key by key, with where each is drawn.

## Decisions taken for you

- **No direction is published.** The history does not support one, so the board says what it measured instead. The leads
  stay exactly as they were - they still say where the two markets disagree today - with their record now beside them.
- **A fall is 20% under where the stock started, at any point inside the window**; the windows are 21 and 63 sessions.
- **The chance is the plain count in the record's table** (spread band by volatility third; a cell with fewer than 150
  company-months uses its row). No fitted model: a table can be checked by eye and cannot be tuned to the past.
- **Non-dollar companies get no chance.** The history is dollar-only, so there is nothing to rest one on. They get every
  credit figure.
- **Non-dollar curves are coarse** (Germany 2, 10, 30 years; the UK 5, 10, 20; Canada 2, 5, 10, 30; Australia 2, 3, 5,
  10) and the page says so. Euro bonds are read against Bunds whatever the issuer's country.
- **Distressed stays at 1,000 bp**, the market's usual line and the board's since 2.0.
- **Month and year changes now rest on real month-end holdings**, not on days worked back. Those changes moved by about
  1 bp, and 14 lead signals differ from yesterday's board (56 leads today, 47 yesterday).

## Found, and not changed

- **The CDS desk prints two lines named AT&T today**, one at 7.1 bp (71 trades in 30 days, last print 2026-11-19, 88 bp
  tighter in a week) and one at 99.5 bp (5 trades, printed 2026-11-24). AT&T's bonds trade at 145 bp; 7.1 bp is not a level
  a company like it trades at. The board carries both lines as the desk prints them and reads AT&T's own CDS from the line
  that trades more, so its sheet says 7.1 bp. Expedia's level (17.6 bp, 68 bp tighter in a month, bonds at 116 bp) looks
  like the same kind of reading. Neither changes a lead today: both companies' bonds are flat on the month. These readings
  are the CDS desk's (`aws/shared/cds_desk.py`), not this board's; that desk was not touched.
- **Survivorship.** A company is tied to a stock through today's listings, so a company since delisted - the bankruptcies
  and the take-overs - is not in the history. The averages after a very wide spread are flattered by that (the +3.5% after
  1,000 bp is the survivors'). The page says so.
- **One credit cycle.** 63 month-ends, one bear market.
- **The top of the distressed list is small holdings of unlisted borrowers** (a few million dollars or less held).
  *Listed stocks only* is beside the switch; with it on, the list is the 18 listed ones.
- Yesterday's open items are unchanged: SoftBank in the AI stack, `ZENITH ARC LLC`, `SPCX`.

## Proof

- **Dry runs on the real warehouse**, ops 6942 (the last run at 16:52 UTC; read-only): the 3.0 build on the runner
  with its writes left out. Spreads, bond facts, non-dollar companies, stress ranks, the record and every chance worked
  again by arithmetic that is not the engine's: every check agrees. Against yesterday's published board on the same
  holdings day, every company is still there in its book, industry and tie with the same spread; one company (UPM-Kymmene)
  gains a spread it did not have, read in euros.
- **Going live**, ops 6952 (17:15 UTC): the deployed package is this checkout's, file for file; one dry run inside Lambda
  (20.4 s, 294 MB of 3,008 MB, nothing written); then one publication at 17:18:27 UTC (167.7 s, 939 MB, the record measured
  inside it in 146.2 s). 4,184 companies, 1,107 listed, 130 distressed (18 listed), 56 leads, 1,083 chances; 23 read-back
  checks pass, among them: 2,946 companies were on the board, 0 are no longer, 0 changed book, industry or tie, 0 have
  another spread, 1,238 are new; every chance is the record's own table; no bond carries an identifier; no ledger day,
  dated holdings or day of calls was removed; the site serves the three documents. **Both invokes were by hand**, outside
  the 22:35 UTC schedule, which is untouched and runs tonight as usual.
- **Live proof**, ops 6944 (17:24 UTC, read-only, can be run again): **0 failed checks**, 26 pictures, in a real browser on
  justhodl.ai. Everything the 2.0 and 2.1 proofs checked, again; and for 3.0: the Distressed switch shows 130 companies,
  widest first (4,429 bp); *Every column* shows 69 and sorts on a new one; Oracle's sheet lists 69 figures, 56 bonds and 63
  month-ends, and the chart reads out a month under the pointer; the record on the page agrees with the document (the six
  bands, the four said-and-happened rows, the nine cells), and a cell narrows the table; 1,083 companies carry a chance, 63
  at one in three or more, ranked from 52.6%; the map of non-dollar investment grade draws 20 industries; all 35 keys of
  the document have a place on the page; the record and a sheet draw on a phone with no sideways scroll; no page error.
  Report: `aws/ops/reports/latest/ops_6944_credit_before_equity_live_proof.md`.
- **The first live proof (17:19 UTC) failed one check, and the check was wrong**: it looked a CDS level up by name, and
  the desk carries two lines named AT&T today (above). Lines are now matched one for one. Everything else in that run had
  passed.
- **Tests**: the engine's 45 offline tests on a new cut of the real warehouse and a sample of the real history (the record
  reproduced figure for figure; a made-up history with one true thing in it, which the study finds; a walk-forward that
  cannot look ahead); 46 page tests; 9 deployment tests, one failing if anything the engine publishes has no place on the
  page; the repository's suites (5,261 node tests, 1,248 deployment tests).
- **The first scheduled run, on its own** (22:35:41 UTC, nothing invoked by hand): 3.0.0 published, nothing degraded,
  bonds, stocks and CDS of 2026-11-24; 4,182 companies (1,107 listed), 52 leads, 135 distressed, and 402 company CDS
  levels - the first read of the CDS desk's 2.0 document (244 tied to a listed stock; 200 more trade with no level).
  The live proof at 22:51 UTC failed one check, "the heat map, hy: the page prints ['Infinity']". Ops 6959 (read-only)
  read the page in a real browser, every book and every colour: the text is **Infinity Natural Resources, Inc. (INR)**,
  a company in the high-yield table, and its name inside two quoted lists in *Every figure*. No figure on the page is
  broken; the check was wrong. It now matches the word only where a number would be (ops 6944, and the CDS desk's
  ops 6957, which carried the same pattern). Run again at 22:57 UTC on the live site: **0 failed checks**.

## On the chart (2026-11-25, evening)

Khalid, after the first scheduled run: "now make sure this have an icon on chart.html".

- **Audit first.** The chart's bar had 31 desk buttons, among them Bonds and CDS; the credit board was the only credit desk
  with none. CDS already had its button (`btn-cds`, added 2026-10-15), so nothing was needed there.
- **What changed.** One button, **Cr Credit** (`btn-cbe`), between Bonds and CDS; a click opens this board in the chart's
  workspace, with "Open page" to the page itself. Nothing on the bar was renamed, moved or removed: the other 31 buttons are
  in the order they had. Files: `jh-chart-engine.js` (the button and its click), `jh-chart-tvrail.js` (the workspace entry),
  `chart.html` (`&cbe=20261125` on the two script tags).
- **Found while doing it.** Inside the workspace, a company sheet's *Chart* link would have loaded a second chart inside the
  workspace. It now hands the company to the chart behind and closes the workspace, as the leaders and volatility desks do
  (`chartHandoff` in `jh-credit-board.js`; the link carries `data-t`). On the page by itself the link is what it was.
- **Not touched.** The engine, its schedule, the document, the CDS desk. Nothing was invoked; the change costs nothing to run.
- **Tests.** Three page tests (the button's place and wording, every other button in its order, the workspace entry, the two
  cache tags; the byte ledger returns the three chart files to their bytes before; the hand-off under the chart, by itself,
  under another page, with no symbol, under a page it may not read). The three older gates that chain the ledger by hand
  (`risk-map-page`, `chart-indicator-controls`, `chart-crypto-routes`) take this change off first.
- **Proof.** Ops 6960 (read-only, can be run again), a real browser on justhodl.ai, 2026-11-26 00:00 UTC (19:00 New York),
  **0 failed checks**, 6 pictures. The button is on the bar once, between Bonds and CDS, and the 32 desk buttons are in
  order; a real click at the bar opened the board in the workspace (20 tiles for 20 industries, 4,182 companies, the
  22:35 UTC document) with the chart behind still on SPY; on Oracle's sheet, *Chart* closed the workspace and put the chart
  on ORCL (10,250 bars); Bonds, CDS, Repo, Sovereign and Risk Map each still open their own desk; on the page by itself the
  same link went to the chart on ORCL; on a 390 px phone the button sits between Bonds and CDS, a tap opens the board, and
  nothing scrolls sideways; no page error. The deploy and the page gate were green before it was judged (the proof waited
  for the site to serve the new files). Report: `aws/ops/reports/latest/ops_6960_credit_button_on_chart_live_proof.md`.
- **To take it back.** `docs/snapshots/chart-search-2026-11-10/README.md`, "2026-11-25: the chart's Credit button".

## Cost

One run a day, about 21 s at 3,008 MB (a tenth of a cent); about 170 s on the day after a month ends. The two one-off
invokes today cost about one cent together.

## To take it back

Revert commit `9a144db3` (2026-11-25 17:06 UTC): the engine and the page return to 2.1.0 and the next 22:35 UTC run
publishes the 2.1 document; the page draws a 2.1 document as it did. The files 3.0 added to the warehouse
(`credit/cbe-holdings/`, `credit/cbe-calls/`, `credit/cbe-track.json`, the three new documents under `data/`) are small,
nothing else reads them, and they can stay.
