# Sovereign stress: a chart button, a desk, and the CISS engine behind it — 2026-11-22

Khalid, 2026-11-22 09:36 ET: "now add an icon for sovereign stress: this engine should help: https://justhodl.ai/ciss.html
you may need to enhance that engine even further and also you may need to fix some things that dont work in that
engine", quoting the page's raw headline number and its three auxiliary cards that said "original-source replay for
this auxiliary is pending". "most of this engine data is pulled from the ecb data source".

## What was found first (ops 6911, 6912 — read-only)

| Where | What |
|---|---|
| ciss.html | The headline printed the ECB's raw 18-digit decimal (0.009462216765937262); 32 such numbers on the page. |
| ciss.html | The three auxiliaries (EUR/USD, two foreign-currency claims) came from older packets whose ECB responses were never kept — "replay pending" could never have completed. |
| ciss.html | 26 of 88 rows showed no value: 28 series the ECB stopped in April/May 2025 (the pre-2025 methodology) labelled "stale 571 days", and 2 (Germany's and Spain's CISS) whose newest ECB row, 2026-11-20, is blank (status M). |
| ciss.html | The series panel and "verify the original row" work (ops 6911's first reading that they did not was the audit's own wrong click). The one console 404 is the browser asking for /favicon.ico; no page request fails. |
| engine justhodl-ciss-stress | Healthy: 14 runs in 14 days, 0 errors, 88 of 88 series compiled, the six contributions reconcile to the composite. It held CISS, SovCISS and CLIFS only — no yields, no spreads, no TARGET balances, no debt. |
| sovereign.html (US fiscal page) | Stopped at "Loading…" on `Cannot read properties of null (reading 'toFixed')`. |
| sovereign-stress.html | Covers 8 of the 11 SovCISS countries (no Belgium, Austria, Ireland, Netherlands) and names Greece "most stressed" by 5-year percentile while France and Belgium read higher in level and in whole-history percentile. Not changed here. |
| systemic-stress.html | Says "ELEVATED 60/100 · ACUTE FRAGMENTATION 74/100" on a France–Germany SovCISS gap of 0.144, on the same day the euro-area SovCISS is lower than 76% of its history and sovereign-stress.html says "CALM 17/100". Not changed here. |

## What was built

**Engine** — `aws/lambdas/justhodl-ciss-stress/source/sovereign_desk.py` (new), called by the handler after the
warehouse run. It reads the warehouse's retained original CSVs again (61 current series), asks the ECB Data Portal
for seven more responses, keeps each through the evidence store before computing anything, and publishes
`data/ciss-sovereign.json` (176 measures, 30 countries) and `data/ciss-sovereign-history.json`:

| Added ECB response | Path | What the desk gets |
|---|---|---|
| Government yield curve, AAA and all issuers, daily from 2004 | `YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM.SR_2Y+SR_10Y` | all issuers minus AAA, 10-year and 2-year, in bp |
| 10-year yields, 34 areas, monthly | `IRS/M..L.L40.CI.0000..N.Z` | each country's yield and its spread to Germany |
| TARGET balances, end of month | `TGB/M...A094T.U2.EUR.E` | each central bank's balance; the sum owed by those in deficit |
| Government debt, % of GDP, quarterly | `GFS/Q.N..W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T` | debt ratio per country |
| Euro reference rate; two Eurosystem foreign-currency claims | `EXR/D.USD.EUR.SP00.A`, `ILM/W.U2.C.A030000.U2.Z06`, `ILM/W.U2.C.A020000.U4.Z06` | the CISS page's auxiliaries, now with their originals |

Every measure carries its value and date, changes over a week / month / quarter / year, its place in its own history
(whole and five years, counted as the warehouse counts them), its record, its highest and lowest reading inside 16
named windows (Lehman … US tariff shock 2025), and its eight highest readings by the data itself. Daily series are
read from every daily observation. A series with no observation for 400 days is listed as ended and not shown.
The document is compiled twice from two separate reads of the stored originals and published only if both agree;
the run receipt (`data/ciss-research/sovereign/runs/<digest>.json`) names every original and the compiler's bytes.
A refused ECB response falls back to the last retained one and costs only its own block; a failure in this module
never fails the warehouse run. `{"sovereign_only": true}` recompiles the desk document alone.

The warehouse (`data/ciss-stress.json`), its contract, its compiler and its 20 consuming engines are untouched:
nothing under `aws/shared` changed. Function timeout 600 → 900 s.

**Desk** — `sovereign-desk.html` + `jh-sovereign-desk.js`: opening lines in words; 11 euro-area tiles; the 11
governments with a SovCISS ranked, each against Germany, its own history, its 2011–12 peak, its 10-year yield and
spread, TARGET balance, debt and CLIFS; the other 16 EU governments; CISS by country; what the euro-area CISS is made
of; **Crisis peaks** (16 windows × three sets of series, plus now and record); **World** (yields, CDS, ratings from
the existing global-sovereign document, reported figures only); **Sources** (each ECB response, the run receipt).
Any number opens its history with the windows shaded.

**Chart** — `btn-sov` ("Sov Sovereign") right after Repo → `jhOpenWorkspace("sov")` → `/sovereign-desk.html`;
both script tags of chart.html carry `&sov=20261122`.

**CISS page** — the headline is "ECB euro-area CISS: 0.0095 on 2026-11-19 — lower than 86% of its readings since
1980." with the exact source value on the line below; the reconciliation is said in words; values in the table are
four decimals (the exact decimal stays in each series' panel); current series come first; a blank newest row shows
the last value with its own date; ended series say "ended at the ECB"; the three auxiliaries are read from the new
document, each with a link to the ECB response it came from and a button that checks it in the browser (same bytes,
same row, same value).

**Fiscal page** — sovereign.html's summary strip writes "not available" for a figure its source did not publish
instead of stopping.

## Proof

| Step | Op | Result |
|---|---|---|
| Dry run against live originals, nothing written | 6913 (second run) | 176 measures, 30 countries, no errors; all 58 measures with a fresh warehouse row equal it; replay agrees; 119 KB + 372 KB gzip |
| First compile by the deployed engine (one desk-only invoke, disclosed) | 6914, 15:34 UTC | published; receipt names the documents; warehouse untouched; 61 s |
| Real browser on the live site (re-runnable) | 6915, 16:02 UTC | 0 failed checks: button after Repo, mouse click opens the desk; 11 tiles and 11 board rows equal the document; chart, ranges, pointer; crisis cells in all three sets; world 44 rows; a retained ECB response re-hashed; 0 px overflow at 390 px; CISS page headline, 58 current / 2 blank / 28 ended, an auxiliary verified in the browser; sovereign.html renders |

Tests: engine 37 (24 new); `tests/sovereign-desk.test.js` 21 on the real documents
(`tests/fixtures/sovereign-desk.json.gz`); node suite 5,145; deployment gate 1,233.

Numbers on 2026-11-22 (ECB data to 2026-11-19): euro-area SovCISS 0.0112, lower than 76% of its readings since 2000
(record 0.7514, 29 Nov 2011). France 0.1739, Belgium 0.1613, Greece 0.1007 … Portugal 0.0038. All issuers minus AAA
at 10 years 59.8 bp (record 194.8 bp, 23 Jul 2012). 10-year spreads to Germany, October: France 147 bp, Italy 126,
Greece 99, Belgium 96. TARGET owed by central banks in deficit €1,307 bn (record €1,592 bn, Sep 2022).

The first pass of ops 6915 (15:50 UTC) failed four checks: two were the proof's own rounding (Python rounds the binary
value, a browser the shortest decimal — 0.17015 is 0.1702 on the page), one counted the hidden chart pane's chips with
the view's, and one was real: the Sources view was 13 px wider than a 390 px screen (long ECB keys now wrap).

Runs outside the schedule: `justhodl-ciss-stress` once, 15:33 UTC, desk-only (61 s). Scheduled checks: 2026-11-23
08:05 UTC (first scheduled run with the desk compile), 2026-11-24 15:30 UTC (cost).

## Left as it is, and why

* sovereign-stress.html and systemic-stress.html keep their own scores (see the findings above); they are other
  engines' heuristics and changing them is a decision, not a repair.
* /favicon.ico answers 404 on the whole site.
* The ECB publishes no daily yield per country on its Data Portal; country spreads on the desk are monthly.
* The older auxiliary packets (`data/ecb-hist/*.json`) are still written by their own engine; ciss.html no longer
  reads them unless the new document is missing.

## Second round, same day (Khalid, 11:12 ET)

Asked: the United States and China CISS; a history chart of every series in each Crisis-peaks set; the chart to open
from a country's name as well as its number; more SovCISS countries if there are any; colour tags that move a country
to the top; sorting both ways; anything else worth adding.

### What the ECB has, checked before building (ops 6916)

* A **daily SovCISS** exists for eleven governments (AT BE DE ES FI FR GR IE IT NL PT) and two euro-area aggregates.
  There is no twelfth. Six more governments (CZ DK GB HU PL SE) had one in the ECB's **monthly** series, which it
  stopped in April 2025. They are carried as ended history (`SOVM_<cc>`, group `sovciss_ended`): peaks, record and
  chart, never a current reading, never coloured as stress.
* The **CISS** (systemic stress, not sovereign) is daily for the euro area, ten member countries, the United States,
  China and the United Kingdom. US, UK and euro area from January 1980; China from October 2006.
* Quarterly budget balance, balance before interest and interest paid (GFS), four-quarter sums in percent of GDP.

### What changed

* **Engine** `justhodl-ciss-stress`, sovereign document **1.1.0** (`sovereign_desk.py`): 266 measures (was 176), ten
  ECB requests beside the warehouse's (was seven), 22 named windows from the October 1987 crash (was 16 from 2001),
  `world_ciss`, `SOVM_`, `DEF_` / `PRIM_` / `INT_`. The warehouse run and `aws/shared/ciss_*.py` are untouched.
* **Page** `sovereign-desk.html` + `jh-sovereign-desk.js`:
  * Desk: three tiles for the United States, China and the United Kingdom; a CISS-by-country table; a table of the six
    ended SovCISS series; CDS and rating (from the site's world document, labelled as not ECB), budget balance and
    interest paid beside each government; a "past week" line and an "outside the euro area" line in the opening read.
  * A country's name opens the same chart as its number.
  * Colour tags (six colours; the dot beside a country). A tagged country moves to the top of every table, leads the
    columns of the crisis table and is drawn thick in its colour on the charts. Filters: all / tagged / one colour.
    Kept in the browser under `jh.sovereign-desk.v1`; nothing is sent anywhere.
  * Every column heading sorts, high to low first, low to high on the second click; a country with no reading in the
    sorted column goes last either way.
  * Crisis peaks: four sets (SovCISS by country · 10-year spreads to Germany · CISS by country · Euro area and
    TARGET). Each same-unit set draws all its series over their whole history on one chart with the windows shaded, a
    legend that switches lines on and off, and ranges 1Y / 5Y / 10Y / All; the mixed-unit set draws one small chart
    per series. A cell opens that series' chart on the window with a year either side.

### Proof

* Dry run, ops 6913 (16:23 UTC): 266 measures, 30 countries, 58 of 58 measures equal to their warehouse rows.
* One desk-only run of the deployed engine, ops 6914 (16:39 UTC, 90 s, disclosed as the second one-off run of the
  day): document 1.1.0 published, replays, warehouse not touched.
* Real browser on the live site, ops 6915 (16:57 UTC), **0 failed checks**: 14 tiles and all four tables equal the
  document; Italy tagged red and Greece amber moved to the top of the governments table and Italy to the top of the
  CISS table; sorting by CISS ran France-first then Netherlands-first under the two tagged rows; the tags were found
  in the browser's storage and survived loading the page again on a phone; clicking "France" opened France SovCISS and
  "United States" opened the US CISS since 1980; the four sets drew 18, 15 and 14 lines and 10 small charts; the cell
  Italy / "Italy and Spain drawn in" read 0.9903 as the document does and opened Italy's chart on that window; US CISS
  in the October 1987 window 0.7194; nothing wider than a 390 px screen; no page errors.
* Tests: page 29, engine 41, node suite 5,153, deployment 1,233.

### Taking the second round back

Page: `git checkout 704fe810 -- sovereign-desk.html jh-sovereign-desk.js tests/sovereign-desk.test.js
tests/fixtures/sovereign-desk.json.gz`. Engine, back to document 1.0.0: `git checkout 50ba4e92 --
aws/lambdas/justhodl-ciss-stress`, then one desk-only run (ops 6914 with its version check set back to 1.0.0). Take
the page back first: the second-round page needs document 1.1.0.

## Third round, same day (Khalid, 12:57 ET): a chart on the World tab

Asked: "add a chart to world cds and yields and have it show all and also have it the chart show only the ones i
tagged same as you have on crisis peak".

### What history the site held, checked first (ops 6917, 6918 — read-only)

The World tab's table is one day's figures for 44 governments (`data/global-sovereign.json`, as reported by World
Government Bonds). It carried no history per country. What the site already keeps:

* **10-year yields, monthly**: the ECB's statistic (already in the desk document) and the OECD's long-term rates in
  the warehouse (FRED `IRLTLT01<cc>M156N`, through the series resolver) — 30 of the 44 countries, the United States
  from 1953. Not held for Turkey, Brazil, Colombia, Vietnam, Peru, Indonesia, India, Thailand, the Philippines, Taiwan,
  Hong Kong, Malaysia, China, Singapore (the OECD publishes no such series for them).
* **5-year CDS**: what the CDS desk measures from DTCC public prints since September 2024 — 25 countries with two or
  more prints, 12 of them trading almost every day; most developed sovereigns rarely print.
* **The table's own figures**: the world engine keeps every publication it replaces, and each publication names the
  receipt that names the one before it. Walking that chain gave 71 publications on 53 days from 26 September 2026:
  a daily yield for all 44 countries and a daily CDS for 38.

### What changed

* **Engine** `justhodl-ciss-stress`: a new module `world_history.py` beside `sovereign_desk.py` publishes
  `data/ciss-sovereign-world.json` (contract `sovereign-world.v1`, 137 series, about 122 KB compressed) after the
  desk document. Four sets, each from one kind of source: `yield` (monthly, ECB or OECD — whichever reaches further
  back and is still current; the source is named on each line), `cds` (DTCC, measured), `yield_reported` and
  `cds_reported` (the table's own figures, day by day). It makes no ECB request. Its state
  (`data/ciss-research/sovereign/world-state.json`) holds the day-by-day record and the monthly series, which are
  asked for again only when a new month can be there. A failure leaves the last document and touches nothing else.
  Event `{"world_only": true}` compiles it alone. The desk document and the warehouse are unchanged.
* **Page**: the World tab opens with a chart of every country in the chosen set over its whole stored history, with
  the controls of Crisis peaks (1Y / 5Y / 10Y / All; Show all / Only tagged / Hide all; a list of names that hide or
  show a line). Crisis peaks and the World tab now share one description of that chart. "Only tagged" follows the
  tags as they are made and is kept between visits; clearing the tags shows all again. A country's name, its yield
  and its CDS in the table open that country's own history. Days a sovereign did not trade are not joined by a line
  across more than 45 days. Countries with no stored history in a set are named; nothing is estimated for them.

### Proof

* Dry run, ops 6919 (18:24 UTC, final code): 137 series, 0 failed checks; a second run on what the first kept folds
  one publication and asks the resolver for nothing.
* One world-only run of the deployed engine, ops 6920 (18:33 UTC, 25 s, no ECB request; the third disclosed one-off
  run of this engine today): document published, 82 reported series end on the table's figures of the day, every
  series agrees with its measure, the warehouse and the desk document untouched.
* Real browser on the live site, ops 6915 (18:46 UTC), **0 failed checks**: the four sets drew 30, 25, 44 and 38
  countries; with Turkey and Japan tagged "Only tagged" drew Japan alone on the long yields (Turkey has none) and,
  when the United States was tagged while it was on, Japan and the United States; carried to the other three sets it
  drew the three tagged countries each time; Turkey's CDS figure opened "Turkey 5-year CDS, measured", Japan's yield
  "Japan 10-year government yield, monthly"; nothing wider than a 390 px screen; everything of the earlier rounds
  still holds.
* Tests: page 35, engine 61, node suite 5,159, deployment 1,233.

### Taking the third round back

Page: `git checkout 3c0eadef -- sovereign-desk.html jh-sovereign-desk.js tests/sovereign-desk.test.js
tests/fixtures/sovereign-desk.json.gz`. Engine: `git checkout 6b36db41 --
aws/lambdas/justhodl-ciss-stress/source/lambda_function.py aws/lambdas/justhodl-ciss-stress/tests/test_sovereign_desk.py`
and `git rm aws/lambdas/justhodl-ciss-stress/source/world_history.py
aws/lambdas/justhodl-ciss-stress/tests/test_sovereign_world.py`. The two objects it wrote simply stop being refreshed.

## Fourth round, same day (Khalid, 13:52 ET): a Dollar shortage tab, and the same three on the Dollar desk

Asked: "add these 3 to it under their own tab called dollar shortage and also add it to the dxy tab" — the euro's
reference rate against the dollar, the Eurosystem's claims on euro-area residents in foreign currency, and its claims
on non-euro-area residents in foreign currency (the three auxiliaries of the CISS page).

### What changed

* **Engine** `justhodl-ciss-stress`: a third module, `dollar_shortage.py`, publishes `data/ciss-dollar-shortage.json`
  (contract `ciss-dollar-shortage.v1`, 219 KB, 53 KB compressed) right after the desk document. It makes **no request
  to the ECB or anyone**: it reads the desk document that is published and the three ECB responses that document
  names, and writes the three measures with every observation — 7,141 daily fixes of the reference rate since
  4 January 1999 (the desk's history keeps one a week), 1,455 weekly readings of each claim. Every figure must equal
  the desk document's (date, value, count, source row) or nothing is published. Event `{"dollar_only": true}`
  compiles it alone. The warehouse, the desk document and the world histories are untouched.
* **Sovereign desk**: a tab **Dollar shortage** between World and Sources (`?view=dollar` opens it): the reading in
  words, three tiles, each one's whole history with the named windows shaded and 1Y / 5Y / 10Y / All, a table of the
  lowest and highest reading inside each window (the euro's low and lending's high are marked; a cell opens the chart
  around that window), and the ECB series and kept response behind each number. Until the document has arrived the
  tab draws from the desk's own two documents and says so.
* **Dollar desk** (`dollar-indexes.html`, the "$ Dollar" button): a ninth family, "Dollar shortage · the euro side,
  from the ECB", with the three as cards like every other series there — history strip since 1996 with the four bear
  markets, day / week / month / quarter / year change — read from the same document through the desk engine's feed
  adapter (`FLOW_FEEDS["ecb-dollar"]`). A card's "open on the chart" opens the Sovereign desk's tab on that series.
  The desk is now 98 series in 9 families.
* Charts: the margin no longer takes the axis of a never-negative series below zero.

What the ECB's own series says: its foreign-currency lending to euro-area banks stood above €100 bn at four peaks —
€248.9 bn on 5 Dec 2008, €153.8 bn on 8 May 2020, €142.1 bn on 10 Apr 2009, €100.6 bn on 10 Feb 2012 — and is
€20.2 bn in the week to 13 Nov 2026.

### Proof

* Dry run, ops 6921 (18:59 UTC): 0 failed checks; the desk's weekly history is inside the complete series value for
  value; the data worker hands `/data` documents to the site's pages.
* One dollar-only run of the deployed engine, ops 6922 (19:12 UTC, 2.0 s, no request to anyone; the fourth disclosed
  one-off run of this engine today): published, equal to the desk document figure for figure, nothing else touched.
* Real browser on the live site, ops 6915 (19:52 UTC), **0 failed checks**: the tab's three tiles and charts equal
  the document (7,141 / 1,455 / 1,455 points drawn), the Lehman row reads 1.2460 / 1.4731 and €85.5 / €248.9 as the
  measures do, a cell opened lending's chart on the COVID window, the reference rate's own chart drew every day; on
  the Dollar desk the three cards read 1.099, 20.19 and 521.89 with week / month / quarter / year changes equal to
  the proof's own arithmetic on the same series and four bear-market bands each, and the card's chart button opened
  `/sovereign-desk.html?view=dollar&m=AUX_FX_EA`; nothing wider than a 390 px screen.
  (Two earlier attempts of the proof itself failed on its own slips — a script path, a variable read before it was
  set — and judged nothing; both are fixed and one is now in its self-test.)
* Tests: page 40, desks 9, engine 70, node suite 5,164, deployment 1,233.

### Taking the fourth round back

Pages: `git checkout 10f4ca4e -- sovereign-desk.html jh-sovereign-desk.js chart-pro.html assets/jh-workspaces.js
tests/sovereign-desk.test.js tests/fred-desks.test.js tests/fixtures/sovereign-desk.json.gz` (then
`python scripts/build_workspaces.py`). Engine: `git checkout b2f22e12 --
aws/lambdas/justhodl-ciss-stress/source/lambda_function.py aws/lambdas/justhodl-ciss-stress/tests/test_sovereign_desk.py`
and `git rm aws/lambdas/justhodl-ciss-stress/source/dollar_shortage.py
aws/lambdas/justhodl-ciss-stress/tests/test_sovereign_dollar.py`. The object it wrote simply stops being refreshed.

## Taking it back

Button: see `docs/snapshots/chart-search-2026-11-10/README.md` (section 2026-11-22). Page: `git rm sovereign-desk.html
jh-sovereign-desk.js tests/sovereign-desk.test.js tests/fixtures/sovereign-desk.json.gz`. CISS and fiscal pages:
`git checkout 298fcf3d -- ciss.html jh-ciss-research.js sovereign.html`. Engine: `git checkout d7f3ff77 --
aws/lambdas/justhodl-ciss-stress` and `git rm aws/lambdas/justhodl-ciss-stress/source/sovereign_desk.py
aws/lambdas/justhodl-ciss-stress/tests/test_sovereign_desk.py` — the warehouse run is the same code either way; the
two desk documents simply stop being refreshed (ciss.html then falls back to the older auxiliary packets).
