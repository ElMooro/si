# chart.html universal search — restore points (2026-11-10)

Khalid, 2026-11-10 11:34 ET: "on justhodl.ai/chart.html on macro tab you made me a search bar and its great i love it ...
before you change anything save the current settings somewhere so that we can revert to if you ever mess it up."

Two restore points. Nothing else on the page is touched by the search work; the three files below are the whole
search + watchlist surface (`jh-uni-search.js` = the dialog, `jh-tv-watchlist.js` = the watchlist it adds to,
`chart.html` = the script tags that load them).

| point | what it is | git | files here |
|---|---|---|---|
| `before-148` | the search as Khalid loved it on the morning of 2026-11-10 (Codex lane, 2026-10-05/06) | commit `81d10e20` | `before-148/jh-uni-search.js.txt` |
| `current` | after PR #148 "Search: add all loaded results and enlarge dialog" (Codex lane, 10:47 ET) | commit `86200289` | `current/*.txt` |

Later changes (the "add all" + bigger-dialog enhancement Claude ships after this snapshot) sit on top of `current`.

## Revert (Git Bash on Khalid's Windows machine, inside the si clone)

Back to the search exactly as it was before PR #148:

    git pull
    git checkout 81d10e20 -- jh-uni-search.js
    git commit -m "revert chart search to the 2026-11-10 morning state [skip-ops]"
    git push

Back to the PR #148 state:

    git pull
    git checkout 86200289 -- jh-uni-search.js
    git commit -m "revert chart search to PR #148 [skip-ops]"
    git push

Either push triggers the Pages build (~10 min); the build content-hash-stamps the script tag, so no cache-bust edit
is needed. The `.txt` copies in this folder are the same bytes (sha256 in `SHA256SUMS`) in case the commits are ever rewritten:
`cp docs/snapshots/chart-search-2026-11-10/before-148/jh-uni-search.js.txt jh-uni-search.js`.

## The Macro desk search (added 2026-11-10 14:31 ET — "im trying to add all ice boa but i dont see the add all")

The search bar Khalid meant is the one on the **Macro** desk of chart.html (the `📈 Macro` button in the chart's top bar
opens `/macro-economic-data.html` in an overlay). Its search ("Universal series search", every indexed provider, a
`+ ADD` per row) had no add-all. Source of truth: the `MacroData` class and its CSS inside `chart-pro.html`;
`assets/jh-workspaces.js`, `assets/jh-workspaces.css` and `macro-economic-data.html` are GENERATED from it by
`scripts/build_workspaces.py` (also re-run by every Pages build).

| point | what it is | git | files here |
|---|---|---|---|
| `macro-desk` | the Macro desk search before the add-all (per-row `+ ADD` only) | commit `91cb77d9` | `macro-desk/*.txt` (generated outputs) |

Revert the Macro desk search (Git Bash):

    git pull
    git checkout 91cb77d9 -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css macro-economic-data.html universe-heatmap.html
    git commit -m "revert the Macro desk search to its 2026-11-10 14:30 state [skip-ops]"
    git push

## The Macro desk with the add-all, before the history strips / full-screen view / notes (2026-11-10 20:05 CT)

| point | what it is | git |
|---|---|---|
| `macro-desk-addall` | the Macro desk with the "+ Add all N" search (cde0447a) and nothing else changed | commit `56c0f1b5` |

Revert the desk to that state (Git Bash):

    git pull
    git checkout 56c0f1b5 -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css macro-economic-data.html universe-heatmap.html
    git commit -m "revert the Macro desk to the add-all-only state [skip-ops]"
    git push

## The Inflows & outflows desk (added 2026-11-10 21:00 CT)

New desk beside Macro: `/inflows-outflows.html` (same engine, its own catalog + storage key `jh_flows_workspace_v1`),
the `⇄ Flows` button next to `📈 Macro` on chart.html (jh-chart-engine.js + jh-chart-tvrail.js, peels registered in
tests/helpers/chart-macro-line-preservation.cjs). The state before it: commit `c5ec5497`.

Remove the desk again (Git Bash):

    git pull
    git checkout c5ec5497 -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css macro-economic-data.html universe-heatmap.html jh-chart-engine.js jh-chart-tvrail.js scripts/build_workspaces.py tests/helpers/chart-macro-line-preservation.cjs config/page-data-contracts.json
    git rm -q inflows-outflows.html
    git commit -m "remove the Inflows & outflows desk [skip-ops]"
    git push

## The three FRED desks beside 13F (2026-11-17)

Dollar indexes (`/dollar-indexes.html`, 95 series), Fed securities held outright (`/securities-held-outright.html`, 222) and
ICE BofA indices (`/ice-bofa-indices.html`, 192): the Macro desk's engine with three more catalogs (`DOLLAR_CATALOG`,
`SOMA_CATALOG`, `BOFA_CATALOG` in `chart-pro.html`), their own storage keys, and three buttons between CDS and 13F on
`chart.html` (`btn-usd`, `btn-soma`, `btn-bofa`). The state before them is commit `7f45dbeb`:

```
git checkout 7f45dbeb -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css scripts/build_workspaces.py \
  jh-chart-engine.js jh-chart-tvrail.js tests/helpers/chart-macro-line-preservation.cjs config/page-data-contracts.json
git rm dollar-indexes.html securities-held-outright.html ice-bofa-indices.html tests/fred-desks.test.js tests/fixtures/fred-desks.json
```

then commit with `[skip-ops]` and push. That also takes back the month-end fix in the week / month / quarter / year
arithmetic (`MacroData.monthsBack`), which the Macro and flows desks share.

## The Acc / Bot / Dist icons open their all-stock desks (2026-11-19)

`justhodl-campaign-scan` (data/campaign-scan.json) lists every stock and ETF the chart's Accumulation, Bottom and
Distribution icons would mark. On `chart.html` a click that turns one of those marks on now also opens its desk
(`/wyckoff-desk.html?tab=accum|bottom|dist`); the next click turns the marks off, as before. The wiring is one block in
`jh-chart-tvrail.js` (it wraps `jhCampToggle` / `jhDistToggle`; the marking files themselves are unchanged) plus the rail's
`dist` entry and a cache-bust on `chart.html`. The state before is commit `980fcbac`:

```
git checkout 980fcbac -- chart.html jh-chart-tvrail.js tests/helpers/chart-macro-line-preservation.cjs
```

That returns the three icons to marking only. The desks, the tearsheets and the engine are not touched by it.

## VANTAGE:SP500 and the other S&P 500 cash-index ids chart the index (2026-11-19)

TradingView's own search answers "sp500", "spx" and "us500" with broker and index-vendor ids of the S&P 500 cash index
(`VANTAGE:SP500`, `SP:SPX`, `PEPPERSTONE:US500` ...). The watchlist router knew none of those venues and answered
"No supported chart route; chart unchanged". `chartSymbol` in `jh-chart-tvwatch.js` now sends 24 such ids, each read from
that search or found in the copied lists (`tests/fixtures/chart-spx-cash/tradingview-search-2026-11-19.json`), to `^GSPC`,
the index `SPX` and `TVC:SPX` already chart. Futures, forwards, perpetuals and same-named foreign stocks are not in the
list. `jh-tv-details.js` lost an undefined variable that stopped the details card's History line on any symbol that is not
a stock ticker. The state before is commit `97f51c22`:

```
git checkout 97f51c22 -- jh-chart-tvwatch.js jh-tv-details.js tests/helpers/chart-macro-line-preservation.cjs tests/watchlist-identity.test.js
```

and remove `tests/chart-spx-cash-route.test.js` and `tests/fixtures/chart-spx-cash/`.

## Index ids, ratio legs, the sp500 search order and the status label (2026-11-19, "Fix all of them")

Five things, one change:

1. `justhodl-symdir` search lists its policy pins whether or not the query words matched them: `sp500` now leads with
   `I:SPX`, `SPY`, then `fred:SP500` (one block inside `search()`).
2. `jh-chart-engine.js` sends the directory's Polygon index ids (`I:SPX`, `I:NDX`, `I:DJI`, `I:RUT`, `I:COMP`, `I:VIX`),
   `NASDAQ:NDX`, `DJ:DJI` and bare `DJI` to the live index route; before, `I:SPX` answered "No bars".
3. `jh-chart-expr.js` reads every market leg of a spread from the chart's own daily bars for that symbol (through
   `window.jhWatchlistRoute`, exported by the router); a data-series leg still comes from `/series`.
4. `jh-chart-tvwatch.js` has one reviewed index table at the top of `chartSymbol` (48 ids: S&P 500, Nasdaq-100, Dow, the
   Polygon ids, `TVC:VIX`, `TVC:TNX`); it decides before the symbol map.
5. `jh-chart-engine.js` no longer re-stamps a cached bar array under another name, which is what left a drawn chart with
   "awaiting chart frame" in its status line.

The state before is commit `888de51a`:

```
git checkout 888de51a -- jh-chart-engine.js jh-chart-tvwatch.js jh-chart-expr.js aws/lambdas/justhodl-symdir/source/lambda_function.py \
  tests/helpers/chart-macro-line-preservation.cjs tests/watchlist-identity.test.js tests/trigger_cap_preservation.py tests/chart-spx-cash-route.test.js \
  tests/fixtures/chart-spx-cash
git rm -r tests/chart-index-routes.test.js tests/fixtures/chart-index-routes tests/index_routes_preservation.py tests/deployment/test_symdir_policy_pins.py
```

then commit, push and redeploy the directory (`gh workflow run deploy-lambdas.yml -f function=justhodl-symdir`).

## 2026-11-19, later the same day: a benchmark word leads with its index at once

ops 6849 timed the Symbol Search dialog on the live page. A warm directory process answers a search in about 30 ms, but
a process that has not loaded the index yet spends 22-26 s loading it inside that request, and three of six words typed
landed on such a process. Until the directory answered, the list for `sp500` led with what was already in the browser:
FRED's copy, then an ETF.

`jh-uni-search.js` now knows the first pin of each index word the directory pins (`sp500`, `spx`, `s&p 500`, `s&p` →
`I:SPX`; `nasdaq`, `ndx` → `I:NDX`; `dow`, `dow jones` → `I:DJI`; `russell` → `I:RUT`) and shows it first until the
directory has answered; then the directory's own rows and order stand. `tests/chart-search-bench-pins.test.js` holds the
table to the directory's `SEARCH_POLICY`. Nothing else in the dialog changed.

The state before is commit `0a17bfcc`:

```
git checkout 0a17bfcc -- jh-uni-search.js
git rm tests/chart-search-bench-pins.test.js
```

then commit `[skip-ops]` and push.

## 2026-11-19, evening: the Volatility button on the chart

Khalid asked for an engine that measures every stock's volatility and price spread, and "an icon for it in chart.html".
The chart toolbar has one new button, Volatility (σ), right after Leaders; it opens `/stock-volatility.html` in the
workspace. In the chart's files that is two lines in `jh-chart-engine.js` (the button, its click) and one in
`jh-chart-tvrail.js` (the workspace's name and page); the byte ledger peels them first (`peelVolDeskEngine`,
`peelVolDeskRail`). Nothing else on the chart changed.

To take the button off and keep the desk page:

```
git checkout 7a5a7c11 -- jh-chart-engine.js jh-chart-tvrail.js tests/helpers/chart-macro-line-preservation.cjs
```

then delete the last test of `tests/stock-volatility-desk.test.js`, commit `[skip-ops]` and push. To remove the desk
as well: `git rm stock-volatility.html jh-stock-volatility.js tests/stock-volatility-desk.test.js
tests/fixtures/stock-volatility-desk.json`, drop the `stock-volatility.html` row from `docs/ops/PAGE_ORDER.md`, and
rebuild `config/page-data-contracts.json` (`python3.12 scripts/build_page_data_contracts.py`). The engine
`justhodl-stock-volatility` keeps running once a day until its schedule `justhodl-stock-volatility-daily` is disabled.

## 2026-11-20: the chart's tabs update on their own

Khalid: "On the chart.html there are a lot of tabs that open automatically but they don't update automatically they
should be updating automatically". Each open tab (SPY, QQQ, IWM ...) read its last price and day change once, when the
page opened. `jh-chart-engine.js` now has one block after the live-tick timer (`refreshTabQuotes`): every 30 seconds,
while auto refresh is on and the page is on screen, the tabs' US stocks and ETFs are re-read in one `/quotes` call
(the 15-minute delayed Polygon snapshot), anything else from its own daily bars, and the figures are patched in place.
The byte ledger peels the block first (`peelTabQuotes`, block text in
`tests/fixtures/chart-macro-line/engine-tab-quotes.block.txt`). Nothing else on the chart changed.

To take it back (state before = 2fa411bb):

```
git checkout 2fa411bb -- jh-chart-engine.js tests/helpers/chart-macro-line-preservation.cjs config/page-data-contracts.json
git rm tests/chart-tab-quotes.test.js tests/fixtures/chart-macro-line/engine-tab-quotes.block.txt
```

then commit `[skip-ops]` and push.

## 2026-11-21: the chart's Repo button and the Repo desk

Khalid: "display an icon for it in the chart.html make sure it cover all the different aspect of the repo market and
repo rate including their spikes in crisis". `jh-chart-engine.js` has one more button (`btn-repo`, right after CDS)
and its click, `jh-chart-tvrail.js` one more entry (`repo` -> `/repo.html`), and `chart.html` carries `&repo=20261121`
on both script tags so a browser never pairs a new button with an old rail. `repo.html` opens on a new Desk view and
has a Crisis spikes view (`jh-repo-desk.js`); the master board, favourites, notes and search are as they were. The
byte ledger peels the three chart files first (`peelRepoDeskEngine`, `peelRepoDeskRail`, `peelRepoDeskHtml`).

To take the button back (state before = 720f7134):

```
git checkout 720f7134 -- jh-chart-engine.js jh-chart-tvrail.js chart.html tests/helpers/chart-macro-line-preservation.cjs tests/chart-crypto-routes.test.js
```

then delete the last test of `tests/repo-desk.test.js`, commit `[skip-ops]` and push. To take the page back as well:
`git checkout 720f7134 -- repo.html config/page-data-contracts.json` and `git rm jh-repo-desk.js tests/repo-desk.test.js
tests/fixtures/repo-desk.json.gz`. The engines are independent of the page: `justhodl-ofr-stfm` keeps refreshing the
OFR bank and `justhodl-repo` 3.0 keeps the wider board either way.

## 2026-11-22: the chart's Sovereign button and the Sovereign stress desk

Khalid: "now add an icon for sovereign stress: this engine should help: https://justhodl.ai/ciss.html". `jh-chart-engine.js`
has one more button (`btn-sov`, right after Repo) and its click, `jh-chart-tvrail.js` one more entry (`sov` ->
`/sovereign-desk.html`), and `chart.html` carries `&sov=20261122` on both script tags so a browser never pairs a new button
with an old rail. The desk is a new page (`sovereign-desk.html`, `jh-sovereign-desk.js`) reading `data/ciss-sovereign.json`,
which the CISS engine now compiles. The byte ledger peels the three chart files first (`peelSovDeskEngine`,
`peelSovDeskRail`, `peelSovDeskHtml`), then the Repo desk's. Full record: `docs/audit/2026-11-22/sovereign-stress.md`.

To take the button back (state before = 298fcf3d):

```
git checkout 298fcf3d -- jh-chart-engine.js jh-chart-tvrail.js chart.html tests/helpers/chart-macro-line-preservation.cjs tests/chart-crypto-routes.test.js
```

then delete the test "the chart has a Sovereign button that opens this desk" from `tests/sovereign-desk.test.js`, commit
`[skip-ops]` and push. The page, the CISS page's repairs and the engine are independent of the button; the record above
says how to take each back.

## 2026-11-23: the chart's indicators close, hide and show

Khalid: "some of them wont close and remove out of the chart and visible and invisible doesnt always work".
`jh-chart-engine.js` carries 20 exact edits (Volume's eye, hide checks on VWAP NY and the candle pattern labels, windows
that follow the price chart's zoom and share the room with it, no rebuild under a held button, panes and legend drawn
before a slow benchmark, the retired studio store following the saved layout) and `chart.html` one (`&ind=20261123` on the
engine's tag). The byte ledger peels both first (`peelIndicatorControls`, fixtures in
`tests/fixtures/chart-indicator-controls/`), then the Sovereign desk's. `jh-chart-auction.js`, `jh-chart-sd-ui.js` and
`jh-chart-evr.js` changed with them and are not ledgered. Full record and how to take it back (state before = 64aa2d70):
`docs/audit/2026-11-23/chart-indicator-controls.md`.

## 2026-11-25: the chart's Credit button (Credit Before Equity)

Khalid, after Credit Before Equity 3.0.0 ran on its own: "now make sure this have an icon on chart.html". The bar had Bonds
and CDS but nothing for the credit board. `jh-chart-engine.js` has one more button (`btn-cbe`, "Cr Credit", right after
Bonds and before CDS) and its click, `jh-chart-tvrail.js` one more entry (`credit` -> `/credit-before-equity.html`), and
`chart.html` carries `&cbe=20261125` on both script tags so a browser never pairs a new button with an old rail. The byte
ledger peels the three chart files first (`peelCreditBoardEngine`, `peelCreditBoardRail`, `peelCreditBoardHtml`), then the
Risk Map's. On the board itself (`jh-credit-board.js` `chartHandoff`, `jh-credit-sheet.js` `data-t` on the link): inside the
chart's workspace a company sheet's Chart link loads the company in the chart behind and closes the workspace, as the
leaders and volatility desks do; on the page by itself it is the ordinary link it was. No engine was touched or run.
Live proof: ops 6960 (read-only, re-runnable). Record: `docs/audit/2026-11-25/credit-before-equity.md`, "On the chart".

To take the button back (state before = 4e715c60):

```
git checkout 4e715c60 -- jh-chart-engine.js jh-chart-tvrail.js chart.html tests/helpers/chart-macro-line-preservation.cjs tests/chart-crypto-routes.test.js tests/chart-indicator-controls.test.js tests/risk-map-page.test.js
```

then delete the two tests "the chart has a Credit button between Bonds and CDS ..." and "the chart's byte ledger takes the
button back out ..." from `tests/credit-before-equity.test.js`, commit `[skip-ops]` and push. The hand-off on the board
(`jh-credit-board.js`, `jh-credit-sheet.js`, `credit-before-equity.html`) does nothing without the button and can stay; to
take it back too, add those three files to the checkout and delete the third test ("inside the chart's workspace ...").

## 2026-11-26: the chart's Flow button (Money flow)

Khalid, after the money-flow engine was audited: "add a button on charts.html for it call it flow. Check if that button
doesnt exist first before you add it". It did not exist: the bar has **Flows** (`btn-flows`, the Inflows & outflows desk,
`/inflows-outflows.html`) and the rail an entry `fl` ("Fund flows", `/flow.html`); neither opens the money-flow engine's
page. Nothing was renamed or moved. `jh-chart-engine.js` has one more button (`btn-mflow`, "Flow", right after Flows and
before Heat) and its click, `jh-chart-tvrail.js` one more entry (`mflow` -> `/global-flow-desk.html`), and `chart.html`
carries `&mflow=20261126` on both script tags. The byte ledger peels these three first (`peelMoneyFlowEngine`,
`peelMoneyFlowRail`, `peelMoneyFlowHtml`), then the Credit button's. Inside the chart's workspace a fund's sheet on the
money map sends the fund to the chart behind and closes the workspace (`top.jhOpenSymbol`); on the page by itself it is an
ordinary link. Live proof: ops 6967 (read-only, re-runnable). Record: `docs/audit/2026-11-25/money-flow-engine.md`.

To take the button back (state before = fad9f69d):

```
git checkout fad9f69d -- jh-chart-engine.js jh-chart-tvrail.js chart.html tests/helpers/chart-macro-line-preservation.cjs tests/chart-crypto-routes.test.js tests/chart-indicator-controls.test.js tests/risk-map-page.test.js tests/credit-before-equity.test.js
```

then delete the two tests "the chart has a Flow button ..." and "the chart's byte ledger takes the Flow button back out
..." from `tests/money-map-page.test.js`, commit `[skip-ops]` and push. The page (`global-flow-desk.html`,
`jh-money-map.js`) works without the button and can stay.

## 2026-11-26: twelve desk buttons chosen from the scan

Khalid, from the scan of every engine ("give me list of engines i should add a tab to chart.html"): "Add these first and
audit their engine before you add them". None of the twelve existed on the bar (checked by id and by page): the rail has
`opt` ("Options", `/options.html`) and `eco` ("Economic calendar", `/econ-calendar.html`), which open other pages and are
as they were. Nothing was renamed or moved. `jh-chart-engine.js` has twelve more buttons in one block between **Khalid**
(`btn-snip`) and **Alert** (`btn-alrt`) with their clicks - Research `btn-rsch`, Insiders `btn-ins`, Options `btn-opta`,
Value `btn-val`, Corp bonds `btn-cbond`, Plumbing `btn-plumb`, Liquidity `btn-liq`, Canaries `btn-can`, Breadth `btn-brd`,
Reversal `btn-rev`, Calendar `btn-cal`, Crypto `btn-cryp`; `jh-chart-tvrail.js` has twelve more entries (`research`,
`insiders`, `optx`, `value`, `corpbonds`, `plumbing`, `liquidity`, `canaries`, `breadth`, `reversal`, `calendar`, `crypto`)
and one line that opens Research on the symbol the chart is showing (`window.jhDeskSymbol`, set by the button's click);
`chart.html` carries `&tabs=20261126` on both script tags. The byte ledger peels these three first
(`peelChartTabsEngine`, `peelChartTabsRail`, `peelChartTabsHtml`), then the Flow button's.

To take the twelve buttons back out (state before = commit `cc4b641b`):

```
git checkout cc4b641b -- jh-chart-engine.js jh-chart-tvrail.js chart.html tests/helpers/chart-macro-line-preservation.cjs \
  tests/chart-crypto-routes.test.js tests/chart-indicator-controls.test.js tests/risk-map-page.test.js \
  tests/credit-before-equity.test.js tests/money-map-page.test.js
git rm tests/chart-desk-tabs.test.js
```

The twelve pages keep their new "Engine data" section (`jh-engine-data.js`, one `<div data-jed>` and one script tag at the
end of each page); it does nothing on the chart and can stay. To take it back too, remove those two lines from each page
and delete `jh-engine-data.js`, `tests/engine-data-kit.test.js` and `tests/fixtures/engine-data-cut.json.gz`.
