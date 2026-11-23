# Leaders and the largest companies first, on every desk the chart opens

Khalid, 2026-11-23 11:59 ET: "Also in all the different engines on my chart.html the leaders and largest market caps
should show up first at the top of the enegines they get first priority. everything stay im talking view mode and how
different stocks are arranged in view."

## The reading taken

It is about the order names stand in, not about what is listed:

1. A list of stocks opens with the **industry leaders first** (the Leaders desk's own set: the five largest companies
   of each industry, `data/leaders.json`), largest first, then **every other name by market value**, largest first. A
   name the feed gives no market value for keeps its place after them, in the desk's own order.
2. **Nothing leaves a list and no figure changes.** A card still shows its six rows and "the other N"; opened in
   full, the whole list is there.
3. **The desk's own order is one button away**, on every sheet and table, and the choice is kept in the browser
   (`localStorage['jh-stock-order']`, `priority` or `engine`).
4. **A column the reader clicks wins**, as before, and the table says so.
5. A card whose title is itself a ranking (Strongest, Newest, Heaviest, Most ...) keeps what it is about: its first
   fifteen by the desk's own ranking, and those fifteen are arranged. Without this every card of a desk opened on
   the same six largest names. A list that is a set of its own (under accumulation, up while SPY falls, volatility
   rising) is arranged whole. Opened in full, every list is arranged whole.

## Where each desk stood before (the audit)

| Desk (button on the chart) | Lists of names | Their order before |
|---|---|---|
| Leaders | 11 cards and 6 more on the sheet; the board and the leaders table | each by its own figure (strength, tape score, 13F dollars); table by strength |
| Strength (Strong) | 6 cards and 3 more; the table of every name | by the move against SPY |
| Volatility | 6 cards and 9 more; the 25-name board; the table of every row | by volatility or spread |
| Capitulation | 5 cards and 2 more; the table of prints | newest and highest score |
| Wyckoff: SC, Capit, BC, NR, Spring, Acc, Bot, Dist | 3 to 7 cards a tab | newest, strongest, deepest ... |
| Wyckoff Acc / Bot / Dist all-stock boards | one table a tab, up to 3,000 names | the scan's own order (a micro-cap could head it) |

Market value is on every feed except the capitulation prints and the phase detector's boards; there a leader takes
the Leaders desk's value and the other names keep the desk's order.

## What changed

- `jh-tearsheet.js` 1.8.0: `priority` (order, model, mode, set, leaders). On the five adapters the chart opens
  (leaders, strength, volatility, capitulation, wyckoff) the model's lists are arranged before drawing; a switch
  under the sheet's own controls (LEADERS · LARGEST FIRST | THE DESK'S OWN ORDER); the leaders come from the page's
  own feed on the Leaders desk, else from a copy this browser keeps for twenty hours, else one background read of
  `data/leaders.json`, after which the lists are arranged again. The other eight sheets (ARK, S&P 500, insiders ...)
  are as they were.
- `jh-wyckoff-scan.js`: the Acc / Bot / Dist boards open arranged; two buttons beside STOCKS / ETFS / ALL.
- `jh-leaders.js`, `jh-strong-today.js`, `jh-stock-volatility.js`: the tables of names open arranged until a
  column is clicked; two buttons above each table, and "Ranked by the column you clicked" when one was. A link that
  carries a hand-made ranking keeps it.
- `capitulation-desk.html`: leaders first among the prints, then the desk's order.

## Not changed, and why

- The Wyckoff desk's own graded table under each board: its script is pinned by `tests/wyckoff-katlin-permission`
  (it must not change); the all-stock board above it and the sheet are arranged.
- 13F: its lists are managers' positions ranked by dollars held, which is already largest first.
- Heat (universe heatmap): tiles are sized by market value. ETF, Macro, Flows, Dollar, Fed outright, ICE BofA, Repo,
  Sovereign, CDS, Bonds, Auctions list no stocks.
- Older live proofs that compare a list with the desk's own ranking (ops 6866 to 6868) now need the desk's own order
  selected first; nothing in them is wrong, the default they open on moved.

## Proof

- `tests/stock-order.test.js` (8) on the real feeds in `tests/fixtures`: the rule from the first row to the last of
  every arranged list on 14 sheets, the same records each once, the same row objects on the cards, the desk's own
  order back exactly. Node suite 5,204; deployment 1,233.
- Live: ops 6931 (`aws/ops/STAGED/ops_6931_stock_order_live_proof.py`), re-runnable, read-only; it reads
  `data/leaders.json` itself as the yardstick.

## To take it back

Everyone's default: `PRIO_DESKS = {}` in `jh-tearsheet.js` leaves every sheet in the desk's order, and each desk
file's `largestFirst()` returning `null` does the same for its table. Whole change (state before = `88abed37`):

```
git checkout 88abed37 -- jh-tearsheet.js jh-wyckoff-scan.js jh-leaders.js jh-strong-today.js jh-stock-volatility.js \
  capitulation-desk.html tests/tearsheet-kit.test.js tests/tearsheet-full-lists.test.js tests/wyckoff-tearsheets.test.js \
  config/page-data-contracts.json engine-manifest.json
git rm tests/stock-order.test.js
```
