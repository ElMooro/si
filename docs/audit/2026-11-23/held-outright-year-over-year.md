# Fed securities held outright: every chart as percent change from a year ago

Khalid, 2026-11-23 10:08 ET: "on all the charts on: Fed securities held outright you should also add the option to view
the chart on year to year percentage change. add it to every single chart".

State before: `792aaee2`. Shipped in `f0605463` and `d9fe221d` (pages only; no engine, no schedule, no cost).

## What the audit found (ops 6928, read-only)

The desk's 222 series were read one at a time through `/series`, the request each card makes.

| What the line is | Lines | As a percentage |
|---|---|---|
| A level above zero | 81 | drawn |
| A level with zero prints (mortgage-backed securities before 2009, bills 2012 to 2019, the short maturity buckets) | 61 | drawn where the year-ago print is not zero |
| Below zero throughout (unamortized discounts) | 15 | drawn, FRED's formula as it stands |
| Itself a change (the H.4.1's weekly and year-ago changes, tagged `C` in the catalog) | 60 | not drawn |
| Ended before 1996 (no strip at all) | 5 | not drawn |

- Every line is weekly, dated Wednesday.
- **The year is the Federal Reserve's own.** The H.4.1 prints "change from a year ago" beside twenty of the levels.
  The level less its print 364 days earlier equals that line on 520 of the last 520 weeks for all twenty pairs; with
  371 days it does not. So "a year ago" on this release is 52 weeks, which is also FRED's rule for weekly data
  (`((x(t) / x(t - n_obs_per_yr)) - 1) * 100`, 52 observations a year).
- A line that started from almost nothing reads absurd figures for a year: Treasury bills +16,302,100% in July 2020,
  mortgage-backed securities +17,092% in January 2010. 44 of 157 lines have extremes more than one span of their
  middle 96% beyond it.

## What was built

All in the MacroData class of `chart-pro.html` (the engine of the Macro, flows, Dollar, held outright and ICE BofA
desks), switched on for this desk alone by `yoy: true` on `DESKS.soma`.

- **Two buttons on every chart**, Level and YoY %, in the head of the strip. **One switch for the whole desk** at the
  top (Level / % change from a year ago). **The same two buttons in the full-screen view.** A chart's own button moves
  that chart alone; the switch sets every chart again. The choice is kept in the browser (`chartView` on the desk's
  saved state, `view` on a series that differs from it).
- **Level is what the desk opens as**, and its strip is byte for byte the strip it was (compared on 16 real histories
  before and after, small and full screen; on the live site a strip is compared with itself before and after a visit
  to YoY %).
- **The figure** is FRED's percent change from a year ago: the print 364 days earlier (the same weekday, 52 weeks) for
  a weekly or daily line, the same month a year earlier for a monthly or slower one. No figure where the year-ago
  print is missing or zero; the line breaks there instead of bridging the gap. The 400 days before 1996 are kept so
  the strip's first year has its figures.
- **Under a chart drawn year over year**: the figure, its two prints and their dates
  ("+3.3% from a year ago · 2026-11-18: 6,472,944 against 6,263,192 on 2025-11-19").
- **Not made a percentage**: the 60 change lines and the 5 that ended before 1996 keep their chart and say why
  ("Drawn as it is: this line is itself a change, so a percentage of it would mislead."); their YoY % button is dimmed
  and, pressed, says the same.
- **Below zero**: the 15 discount lines follow FRED's formula as it stands, and say that a plus means the line grew in
  size.
- **The scale** takes in every figure unless one stands more than a whole far-out fence beyond it (the fence: three
  interquartile ranges beyond the quartiles). Then the scale stops at the fence, the line runs along a dashed edge,
  and the card prints the true high and its month. Zero and the latest figure are always on the scale.

## The Y box and the year-over-year figure

The Y box on a card counts from the same calendar date a year back, or the last print before it. For a weekly line
that is 53 weeks. The year-over-year view is 52 weeks, as FRED and the H.4.1 itself count. On 18 November the total
reads Y +3.31% (against 12 Nov 2025) and +3.3% year over year (against 19 Nov 2025). The switch says so in a sentence
and the Y box names its print in its tooltip. Nothing about the Y box was changed: the same question is open on the
Sovereign desk (see `docs/audit/2026-11-22/sovereign-stress.md`) and waits for Khalid's word.

## Proof

Real browser on the live site, ops 6929 (re-runnable). The op reads the 222 series itself, one at a time, and works
every figure out in Python.

First build (`f0605463`): 16:11 UTC, 0 failed checks. Second build (`d9fe221d`: the range label "1996 → today" stays
in either view, so the head of a strip keeps to one line; engine bundle `?v=f810fd1c`): 16:40 UTC, **0 failed checks**.

- Opens as Level: 222 cards read, the switch on Level, nothing saved in the browser, 60 change notes as before.
- The switch, clicked: 157 charts drawn year over year. Against the op's own arithmetic: the latest figure equal on
  157, both prints and both dates on 157, the number of figures on 157, the scale and its edge marks on 157, the
  unbroken runs on 157. The scale stops short on 86 and each prints its true high; the 15 below-zero lines say what a
  plus means; 65 stay as they are and say why (60 changes, 5 ended before 1996).
- Figures on 18 November: total +3.3% (6,472,944 against 6,263,192 on 2025-11-19, last crossed zero 22 Apr 2026),
  Treasuries +9.5%, mortgage-backed securities −9.0%, bills +197.0%, unamortized discounts +13.3% (−26,371 against
  −23,282). A line with prints before 1996 (RAGSOTBO) has its first figure on 1996-01-03.
- Kept across a reload. Level on the Treasuries chart alone moved that chart alone ("1 chart is set on its own").
  YoY % pressed on the Fed's own change-from-a-year-ago line: stays Level and says why.
- Full screen: the same two buttons and sentence, "now +3.3%" on the chart, the off-the-scale label, four events;
  Level there redraws the big chart and the card behind it.
- Back to Level: 222 of 222, nothing saved, 8 sampled strips the very picture they were before.
- Phone (390 px): no sideways scroll, buttons 24 px tall on a card and 29 px on the switch; full screen as wide as
  the screen.
- The chart's Fed Outright button opens the desk as Level with the switch working inside the overlay. The Dollar,
  ICE BofA and Macro desks have no switch and no buttons, and read as before.

Tests: `tests/fred-desks.test.js` 18 (nine new, on ten real histories in `tests/fixtures/held-outright-yoy.json.gz` and
ops 6928's own figures); node suite 5,179; deployment 1,233.

## Taking it back

Off for the desk without removing code: delete `, yoy: true` from `DESKS.soma` in `chart-pro.html` and run
`python scripts/build_workspaces.py`. Whole change: `git checkout 792aaee2 -- chart-pro.html assets/jh-workspaces.js
assets/jh-workspaces.css tests/fred-desks.test.js config/page-data-contracts.json engine-manifest.json` and
`git rm tests/fixtures/held-outright-yoy.json.gz`. A choice saved in a browser is ignored by the older code.
