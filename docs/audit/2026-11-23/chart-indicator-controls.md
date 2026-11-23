# chart.html: every indicator closes, hides and shows

Khalid, 2026-11-23 11:59 ET: "next thing i want you to look at are my indicators in my chart.html the indicators are good
but some of them wont close and remove out of the chart and visible and invisible doesnt always work."

## How it was audited

A local copy of chart.html on real daily bars (NVDA, `tests/fixtures/campaign-scan-bars.json`) in headless Chrome.
Every study of the Indicators list (215: Volume, 95 on the price chart, 119 oscillators) was switched on from the
list, hidden with its eye, shown again and closed with its cross, with a real pointer. The chart was photographed
after each step and the pictures compared pixel for pixel with the chart before the study.

Before the change 88 of 215 came through clean. After it: 215 close; 214 hide and show (Buyback is a text section
under the chart with its own Close button and no eye).

## What was wrong

| # | What a person saw | Cause |
|---|---|---|
| 1 | Volume: the eye removed it; nothing on the legend brought it back; a removed Volume returned at the next page load | the legend treated eye and cross alike; start-up forced Volume on |
| 2 | VWAP hidden: the "VWAP NY" line stayed | the line was drawn whenever VWAP was on |
| 3 | Candle patterns hidden: the labels stayed | same |
| 4 | Session VWAP, Volume profile, PDH / PWH / OR hidden: their lines stayed | `jh-chart-auction.js` drew by "on" alone |
| 5 | Supply & Demand: the Indicators list could not switch it on; with the Demand or Supply button lit, the cross on its legend row did nothing | `jh-chart-sd-ui.js` pushed the buttons' state onto the study on every change to the page |
| 6 | Any oscillator closed (or hidden, or shown): the volume profile on the left stayed squashed at the old height | the profile was redrawn only when the zoom changed |
| 7 | Any oscillator added: the price chart jumped to another zoom; MACD pushed the latest 34 bars off the right edge | the new window pushed its own default zoom onto the price chart |
| 8 | A window's bars did not line up with the candles (MACD: about 25 bars off) | a window held only its own points, so its bar count differed |
| 9 | Four windows left the price chart no height at all; the fifth and sixth were below the screen with no way to scroll, so their crosses could not be reached; a seventh oscillator was on and drawn nowhere | each window took 240 px with nothing to stop them; six is the engine's limit |
| 10 | A window's eye, cross, settings and help were invisible until the pointer was over its title; on a touch screen, never | drawn at opacity 0 until hover |
| 11 | A hidden window stayed as a faint copy taking the same room | hidden meant 12% opacity |
| 12 | An indicator moved onto the price chart had no eye | its legend row carried only "down" and the cross |
| 13 | The Indicators list could only switch things on | a lit row set "on" again |
| 14 | More than a minute after the last repaint, an eye or cross seemed dead for as long as the S&P 500 history took to download (2.5 s measured with a 2 s download) | the panes and the legend were drawn after the benchmark arrived |
| 15 | A press was lost when the chart repainted between button-down and button-up | legend and windows were rebuilt under the pointer |
| 16 | After closing Volume profile from the legend, its chip needed two presses | the chip kept its own switch |
| 17 | A browser that once used the old indicator studio: studies closed since came back at every load, studies added since vanished 1.5 s after load | `jh-chart-studio.js` re-applies its own old snapshot; its panel no longer exists, so the snapshot could never be updated |
| 18 | Effort & Spread had no eye | its own file drew a cross only |

Not a fault, checked: a 12-pixel difference near the STRUCT chip after Ichimoku is text smoothing of a chip behind it.

## What changed

`jh-chart-engine.js` (20 exact edits, ledgered):

- Volume: `volHide` beside `volOn`. The eye hides and shows (row stays, dimmed); the cross removes. Both are saved
  (`volHide`, `volOff`) and honoured at start-up. With Volume Tape on, the eye still wins; the cross then hides the
  bars and keeps the row, and says so. The Indicators list switches Volume on again.
- VWAP NY and the candle pattern labels are drawn only while on and not hidden.
- Windows: a pane never leads the zoom (it cannot be scrolled or zoomed by hand); each carries the price chart's bar
  times and takes its range and price-scale width when built, so the same bar is at the same x everywhere.
- Room: `fitPanes()`. The price chart keeps the larger of 220 px and two fifths of the column. Windows still at
  their default height share the rest (240 px down to 160); a height set by hand is kept; past that the window area
  scrolls. In a column too short to share (a phone held upright) the price chart keeps 220 px and the column scrolls.
- More than six oscillators: a line under the windows names the ones not drawn, each with a cross.
- Eye, cross, settings and help of a window are always drawn. A hidden window folds to its title row.
- An indicator on the price chart gets an eye in its legend row.
- A lit row in the Indicators list switches that study off; a hidden study is shown first (as before).
- While the benchmark downloads, panes and legend are drawn at once and again when it lands.
- Neither the legend nor a window is rebuilt while one of its buttons is held; the rebuild follows the release.
- The profile, the drawings and the tape strip are redrawn when the price chart changes size.
- `mirrorStudio()`: where the old studio store exists, its on / off flags follow the saved layout at start-up and on
  every save. Colours in it are left alone.

`jh-chart-auction.js`: lines are drawn while on and not hidden; the chips follow the study rows.
`jh-chart-sd-ui.js`: the buttons set the study when the page opens and when pressed; a change made elsewhere is what
the buttons then show.
`jh-chart-evr.js`: an eye; hidden folds the window (it used to count as off).
`chart.html`: `&ind=20261123` on the engine's tag so a browser holding the old engine fetches this one.

`jh-chart-indux.js` (legend, list and settings) is unchanged; so is `jh-chart-studio.js`.

## Decisions made for him, each easy to turn

- A hidden window folds to its title row. To keep the room instead: in the engine line that starts
  `if(o.hide){ pane.dataset.hid="1"`, drop the height.
- A removed Volume stays removed across page loads (it used to come back). To have it come back: `volOn=true` in
  `bootLay`.
- A lit row in the Indicators list switches the study off (the Find list already did).
- Two fifths of the column for the price chart: `Math.round(avail*0.4)` in `fitPanes`.

## Found and not changed

- On a phone held upright the toolbar takes about 510 of 844 px and leaves the price chart 110 px (no window open).
  With a window open it had none; it now keeps 220 px. The toolbar itself was not touched.
- The AVWAP / VP / PDH-OR chips sit behind the volume tape strip at the bottom left of the chart.
- Buyback has only its own Close.

## Proof

- Local: the sweep above; `tests/chart-indicator-controls.test.js` (17 tests; each fails on the files as they
  were); node suite 5,196; deployment 1,233.
- Live: ops 6930 (`aws/ops/STAGED/ops_6930_chart_indicator_controls_live_proof.py`), re-runnable, read-only: the same
  sweep on justhodl.ai in a real browser, plus Volume, windows, the list, presses under a repaint, the benchmark wait,
  Supply & Demand, the chips, the studio snapshot and a phone.

## To take it back

State before = `64aa2d70`.

```
git checkout 64aa2d70 -- chart.html jh-chart-engine.js jh-chart-auction.js jh-chart-sd-ui.js jh-chart-evr.js \
  tests/helpers/chart-macro-line-preservation.cjs tests/chart-crypto-routes.test.js config/page-data-contracts.json engine-manifest.json
git rm -r tests/chart-indicator-controls.test.js tests/fixtures/chart-indicator-controls
```
