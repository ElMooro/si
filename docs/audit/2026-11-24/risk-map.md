# The Risk Map: every asset class, one heat map, and booming means booming

Khalid, 2026-11-24 10:05 ET: "i want you to improve this engine so that it covers all asset classes: Risk Map booming vs
getting destroyed ... https://justhodl.ai/regime-map.html then add a tab for it on chart.html make sure you double check
your work to make sure whats booming is actually booming and whats destroyed is actually destroyed and make it in a bug
heatmap visual like tradingview heatmap"

Four things were asked: (1) the engine covers all asset classes, (2) it opens from chart.html, (3) what it calls booming
is booming and what it calls destroyed is destroyed, (4) a big heat map like TradingView's ("bug" read as "big").

## What the first engine (1.0.0) got wrong — audit, ops 6935, 15:18 UTC

Read from its own live feed and the warehouse; nothing was changed by the audit.

| Finding | Evidence on the day |
|---|---|
| "Booming" was its six highest scores, whatever they were | XLV +41, UUP, XLP, IWD +9, XLY, XLF: none was in the state BOOMING; IWD, XLY and XLF were down over three months |
| "Destroyed" was its six lowest, likewise | six names always, in any market |
| It counted bars, not days | a coin trades every day, so its "1 / 3 / 6 months" were 21 / 63 / 126 days: Bitcoin "six months −5.4%" against −20.0% by the calendar, Ethereum −8.0% against −19.4%, Solana −7.5% against −16.0% |
| A share split looked like a crash | the bank holds bars from before a split as they traded: QUAL and MTUM (4-for-1 on 2026-11-05) read −76% and −78%; ITA 3-for-1, VIXM 1-for-2 likewise |
| 38 instruments, seven sleeves | no currencies, no futures, no volatility, three coins, three "international" funds |
| It fetched from Polygon itself | 38 provider calls a run, against the rule that an engine reads the warehouse |
| Its turn names could contradict the move | the rule ended in a bare `else`, so a change it had no other name for was WEAKENING whichever way it went: XLF, **up** 17 points from NEUTRAL to RISK-ON, was "weakening" |

## What changed

### Engine `justhodl-regime-map` 2.0.0 (contract `risk-map.v2`)

- **293 instruments in nine asset classes and 25 groups**: US stocks (size, style, sectors, industries, themes), world
  stocks (regions, developed, emerging), bonds (Treasuries, investment grade, high yield, world), currencies (the dollar,
  nine majors, fourteen emerging), commodities (precious, energy, industrial, agriculture, baskets), crypto (the forty
  largest banked coins by market value, re-picked each run), real estate, volatility (VIX, VVIX, MOVE and two funds),
  alternatives. 216 funds, 40 coins, 23 currencies, 10 front-month futures, 4 indexes. The list is
  `aws/lambdas/justhodl-regime-map/source/risk_universe.py`.
- **Warehouse only.** Funds from the Polygon daily bank and its forming bar, coins from the banked coin bars, currencies,
  futures and indexes from the FMP price bank, coin market values from the banked CoinMarketCap listing. No provider is
  called and no key is read.
- **Changes by the calendar**: from the last close on or before the same day one week, one, three, six or twelve months
  earlier (or the last close of the previous year); the day's change from the close before. Every tile carries each
  anchor's date and close, so a change can be looked up.
- **Splits**: a split on Polygon's own record that the bank's bars do not yet reflect is rebased first (same rules as the
  Leaders engine, same file). A one-day move too large for its kind with no split on record is not called.
- **The score is the first engine's**, unchanged: 220 × (0.45 × three months + 0.30 × six months + 0.25 × one month) +
  25 × trend, held to ±100; BOOMING at +45, RISK-ON at +15, NEUTRAL, RISK-OFF, DESTROYED at −45.
- **Booming and destroyed are held to the record, not the score alone**: BOOMING also needs one month and three months
  both up, one of them by 5% or more, and the close above its 50- and 200-day averages; DESTROYED the mirror. A score that
  fails is shown one state in, with the reason on the tile.
- **The lists hold only what is in that state**, however many or few. `strongest` / `weakest` keep the six highest and
  lowest scores for anything that wants a ranking.
- **Everything the first engine published is still published** for the same 38 instruments, same keys, same sleeve names,
  same regime rule (BROAD RISK-ON / NARROW / BIFURCATED / BROAD RISK-OFF / MIXED), so the eight engines and two pages that
  read `data/regime-map.json` read on. Contract test: `tests/deployment/test_risk_map_engine.py`.
- **A cross-asset read** beside the stock regime: FLIGHT TO SAFETY, LIQUIDATION, BROAD SELL-OFF, RISK-ON, EVERYTHING UP or
  MIXED, from risk assets against Treasuries, gold, the yen and franc, and the dollar. The rule is in the document.
- **A turn is named by its direction.** A test walks every state a score can carry against every other; no name
  contradicts its direction.
- **A run is compared with the last run of an earlier day** (`turns_since` says which), so running the engine by hand no
  longer hides the day's turns or a regime change from the engines that read `regime_changed`.
- **A whole kind of bar more than five days old at the run is said so** in `notes`; an instrument a bar behind its kind is
  marked; one with no bar for a week, under 130 bars, or a fund trading under $250,000 a day is left out and named.
- Runs once a day at 21:00 UTC as before, about 11 seconds at 1,024 MB: under a cent a month.

### The page — `regime-map.html`, `jh-risk-map.js`

- **One heat map**: classes, their groups, a tile per instrument (squarified treemap), as wide as the screen. A phone gets
  the same map taller.
- **Colour by Boom ↔ Destroyed** (default; the five states sit in five separate bands, so a rising tile cannot pass for a
  booming one) **or by the change over 1D, 1W, 1M, 3M, 6M, YTD, 1Y**, in percent or against the instrument's own usual
  move (a currency's 1% is more than a coin's 3%). Red and green as on TradingView; orange and blue for red-green colour
  blindness. A sign is on every number and an arrow on every booming and destroyed tile, so nothing rests on colour alone.
- **Tile size**: how heavily it trades against the others in its group (funds: dollars a day; coins: market value), held
  within bounds; currencies, futures and indexes are one size; or all equal.
- **A class at a time**: the chips, or a class's heading.
- **A tile's card**: every change with the day and the close it is measured from, how unusual each is for that
  instrument, the rule's tests in words ("Booming: score +100, one month +36.9%, three months +75.9%, 42.9% over its 50-day
  average ..."), the last bar's date, and what a reader must know (a fear gauge's rise means fear; a bar still forming; a
  rebased split; a bar behind). "Open in the chart" for funds and coins.
- **Under the map**: the booming and destroyed lists (exactly the tiles in that state, with the figures the rule reads),
  what scored that far and was not called it and why, a table by asset class, turns, every instrument as a sortable
  table, the rule and the sources.
- **The page measures nothing and checks everything it draws**: each change is re-derived in the browser from the closes
  the document carries, each booming and destroyed tile is held to the rule, and the result is printed at the foot. A
  document that failed would say so across the top.
- **Live prices for funds** (the Worker's `/quotes`, Polygon, 15 minutes delayed) in the 1D–1Y views: the session price
  over the same anchor closes, shown beside the engine's own figure on the card. A live price never moves a state.
- **The previous view is one click away** ("Previous view", or `?view=classic`): the first Risk Map's page, drawn from the
  same document's first-map fields.

### chart.html

A **Risk Map** button stands between Heat and Auction (nothing that was there moved) and opens the map inside the chart;
a tile's "Open in the chart" loads that instrument and closes the map. The button, its rail entry and the two cache tags
are in the chart's byte ledger (`tests/helpers/chart-macro-line-preservation.cjs`, peeled first).

### Home dashboard

The Risk Map card on `classic-dashboard.html` names what is BOOMING and DESTROYED across the whole map, with the count,
and says "none today" when nothing is (before, an empty list printed an empty line). Ledgered:
`tests/risk_map_card_preservation.py`, import manifest updated.

## Decisions taken for you

1. **Calendar windows**, not bar counts.
2. **The 5% test** for booming and destroyed. A size test in units of each instrument's own usual move was tried on the
   real document and refused: it kept the VIX at +76% off the booming list and MSOS, EWY and UFO off the destroyed one.
3. **The lists hold only that state.** On a day nothing is booming, the list is empty and says so.
4. **Currencies are shown against the dollar**: green means the currency rose. USD/JPY and its like are turned over.
5. **Funds before futures** where both exist (gold, oil, copper, gas, grains); futures only for what has no good fund.
6. **A booming VIX is on the booming list, marked as a fear gauge.** Its price is booming; what that means is on its tile.
7. **Every tile is dated.** Coins, currencies, futures and indexes are a day behind the funds during the US session; the
   page says each kind's last bar.
8. **The map's own colours are exempt from the site's re-skin; its frame is not** (see below).

## Found, and not changed

- **The FMP price bank is never re-adjusted after a split.** 17 of its closes for MTUM, QUAL, ITA and VIXM are still
  pre-split prices. The Risk Map does not read funds from it; anything that does will read a false crash.
- **FMP price files run up to three days late** (the 1.5 GB a run budget): six futures were three days behind on the day.
  They are marked on the map.
- **At 21:00 UTC in winter the US session's last bar is 15 minutes short of the close** (the forming bar, 15 minutes
  delayed, is as of 15:45 New York). The first engine had the same from Polygon directly. Running at 22:05 UTC would give
  the final bar, but the paper book (21:30) and sector emergence (21:20) read this feed; moving all three is yours to say.
- **The first engine's turn rule calls a one-point crossing "bottoming / recovering"** (XLRE, −15 to −14). Left as it is.
- **The Polygon daily bank holds pre-split bars as they traded** until it re-pulls them. This engine and the Leaders engine
  rebase; any other reader of those files should be checked.
- **The site's section badges** (`jh-sections.js`, baked into every page) are placed on top of any big block that has no
  heading. This page now gives them headings; a page that draws its own canvas of blocks may have the same stray badges.
- **The site build's re-skin** (`scripts/reskin_site.py`) turns every cool hue in a served page into the site's amber.
  TradingView's green is a teal: on the first deploy rising tiles were drawn amber. Found by the live proof, fixed the same
  hour (data colours live in the script, which carries the build's opt-out; a deployment test holds it). Other pages that
  draw meaning in teal or blue may be affected the same way; they were not audited here.

## Proof

| Op | When (UTC) | What | Result |
|---|---|---|---|
| 6935 | 15:18 | audit of 1.0.0 and of the warehouse (read-only) | the findings above |
| 6936 | 15:49, 16:27 | 2.0.0 built on the runner, nothing written; its figures against arithmetic written in the op, FMP's own history, the Fed's H.10 and CoinMarketCap | 293 of 293; 0 failed checks; 1,280 anchor closes on 212 funds within 0.051% of a second provider |
| 6937 | 16:11 | first light: deployed package = checkout, a dry run inside Lambda, one publication (a one-off run outside the schedule), read back | 293 instruments, 0 problems |
| 6938 | 17:22, 17:55, 18:20 | the published document against the warehouse's raw bars file by file; a real browser on justhodl.ai | final run: 0 failed checks (below) |

ops 6938 ran three times on the live site, read-only each time:

- **17:22 UTC**: every figure passed (2,049 anchors on 293 instruments looked up in the warehouse's own files, 0 do not
  agree; every last close is the warehouse's; lists, live quotes, previous view, phone, the chart button). It failed four
  checks, all one cause: the colours. The build's re-skin had turned the tiles' green into amber. Fixed (above).
- **17:55 UTC**: 0 failed checks. Its picture of the map inside the chart showed one of the site's section badges sitting
  on the "US stocks" heading; on a phone the same script would have put seven on the map. Fixed: class headings and tiles
  are buttons, which that script passes over, and the blocks around the map carry a heading for the badge.
- **18:20 UTC, final: 0 failed checks.** 2,049 anchors on 293 instruments are the warehouse's own bars, 0 do not agree;
  the page's own check agrees on all 2,049; 293 tiles, none over another, none outside the map; every number on a tile is
  the document's; colour by state (green minus red): booming 117..145, rising 50..77, flat 4, falling −94..−53, destroyed
  −188..−149, so no rising tile is as bright as a booming one; the booming list is VIX, ZR, VVIX, VIXY, BTAL, UAE, XLV, PHO
  and the destroyed list is 100 rows, both the document's; 1D view: 0 tiles differ from the document; 216 of 216 funds
  live, each held to the quote the page was given; 9 section badges on the page, 0 on the map; previous view: 38 cells in
  seven sleeves, one click; phone: 293 tiles, no sideways scroll; chart.html: the Risk Map button between Heat and
  Auction opened the map with a real click (293 tiles inside) and a tile opened QQQ's chart. Report and six pictures:
  `aws/ops/reports/latest/ops_6938_risk_map_live_proof.md`.

Not yet seen: the first scheduled run of 2.0.0 (21:00 UTC), which is the first to carry `turns_since` and the corrected
turn names. ops 6938 is read-only and can be run again after it.

Tests: `aws/lambdas/justhodl-regime-map/tests/run_tests.py` (27, on a cut of the real warehouse taken by the runner),
`tests/risk-map-page.test.js` (21), `tests/deployment/test_risk_map_engine.py`; the node suite 5,228 and the deployment
gate 1,239 pass.

## To take it back

- **The page only** (keep the engine): press "Previous view", or `git revert 634ce82f 973f4a9f 152624ed` and push.
- **The chart button only**: the three peels in `tests/helpers/chart-macro-line-preservation.cjs` are the exact bytes.
- **The engine**: the first engine is the tree before commit `1d838d72`
  (`git checkout 1d838d72~1 -- aws/lambdas/justhodl-regime-map`), then
  `gh workflow run deploy-lambdas.yml --ref main -f function=justhodl-regime-map`. The next 21:00 UTC run publishes a
  1.0.0 document; the page then shows the previous view by itself.
