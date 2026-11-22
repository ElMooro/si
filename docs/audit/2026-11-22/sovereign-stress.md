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

## Taking it back

Button: see `docs/snapshots/chart-search-2026-11-10/README.md` (section 2026-11-22). Page: `git rm sovereign-desk.html
jh-sovereign-desk.js tests/sovereign-desk.test.js tests/fixtures/sovereign-desk.json.gz`. CISS and fiscal pages:
`git checkout 298fcf3d -- ciss.html jh-ciss-research.js sovereign.html`. Engine: `git checkout d7f3ff77 --
aws/lambdas/justhodl-ciss-stress` and `git rm aws/lambdas/justhodl-ciss-stress/source/sovereign_desk.py
aws/lambdas/justhodl-ciss-stress/tests/test_sovereign_desk.py` — the warehouse run is the same code either way; the
two desk documents simply stop being refreshed (ciss.html then falls back to the older auxiliary packets).
