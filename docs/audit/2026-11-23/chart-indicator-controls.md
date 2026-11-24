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
| 19 | RS / beta vs SPY: the eye dimmed its row and its markers stayed on the chart (found by the live sweep, ops 6930; the local chart had too few bars to show it at first) | the engine asked the studio file for its markers whenever the study was on, hidden or not |
| 20 | A press that landed while a long chart was repainting, or that was held longer than a second and a half, was lost after all (live: NVDA daily has 6,990 bars and one repaint takes a second or two; reproduced on the local copy by holding 1.7 s, and by a 0.15 s press under a repaint that blocks the page 2.5 s) | the hold that keeps a pressed button in place (cause 15) was let go by a 1.5 s timer |
| 21 | Opening Range, Earnings, News, Dividends, Splits, Insiders, Buybacks and AVWAP from earnings: for as long as the study's own download took, the study was on with no row in the legend, so nothing to hide or close it by (live: Opening Range, more than 1.5 s; local copy with the 5-minute bars answered after 3 s: no row, and its cross could not be pressed) | the legend was drawn at the end of the repaint only, after every download |
| 22 | ... and the chart sat shifted (six bars for Opening Range) until that download answered | the zoom was put back at the end of the repaint only |

Not a fault, checked: a 12-pixel difference near the STRUCT chip after Ichimoku is text smoothing of a chip behind it.

## What changed

`jh-chart-engine.js` (27 exact edits, ledgered):

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
- The RS / beta markers are asked for only while that study is not hidden (edit 21, second deploy).
- A hold ends when the button comes up. It also ends at the next pointer move with no button down, when the window
  loses focus, and after 12 s - for a release the page never hears of (third deploy).
- `legendFirst()` inside the repaint: before the first download a study makes for itself (earnings calendar, dividends /
  news / splits, insiders / buybacks, 5-minute bars), the zoom is put back and the legend is drawn, so the row with its
  eye and cross is there at once. Once for a repaint; the usual legend at the end still follows (edits 22-27, third deploy).

`chart.html`: `&ind=20261123b` on the engine's tag so a browser holding the old engine fetches this one
(`&ind=20261123` for the first deploy, `b` for the second, `c` for the third).

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
- In a browser that has never opened the chart, the watchlist announces every signal alert it holds, one message at a
  time at the bottom centre, for the first minutes. A returning browser has seen them and stays quiet.
- On the phone the price chart with no window open measured 59 to 110 px, depending on how the toolbar wraps.

## Proof

- Local: the sweep above; `tests/chart-indicator-controls.test.js` (20 tests; each fails on the files as they
  were); node suite 5,207; deployment 1,233.
- Live: justhodl.ai in a real browser (headless Chrome on the runner, a real pointer), NVDA daily, 6,990 bars, the
  third deploy (`459b4a59`). All three ops are read-only and re-runnable; every study in the Indicators list is
  covered, 215 of 215.
  - **ops 6930**, 2026-11-23 23:44 UTC, success, 0 failed checks. Volume and the 95 studies drawn on the price chart:
    96 of 96 switch on, hide, show and close, the chart back to the same picture each time; the eye on screen
    without hovering on 96 of 96; the row there half a second after switching on for 96 of 96. Volume: the eye hides
    and shows, the cross removes, each still so after a reload. Three windows: zoom [6830, 6995] unchanged, price
    chart 380 of 950 px (40%), the same bar at x = 1272.3 on the chart and in all three windows, buttons at opacity 1.
    A hidden window is 22 px. Seven on: six shown, the area scrolls, the seventh named with a cross. A lit row in
    the list switches the study off. A press held while the chart repaints is kept, on the legend and on a window.
    With the S&P 500 history stale the eye answers in 78 ms. Supply & Demand, the VP chip, a planted studio
    snapshot, a phone (touch only): as designed.
  - **ops 6932**, 2026-11-24 01:08 UTC, success, 0 failed checks. The 119 oscillators: 118 switch on, fold away,
    come back and close by their own eye and cross with the chart back to the same picture; Buyback closes by its own
    Close.
  - **ops 6933**, 2026-11-24 00:29 UTC, success: the eleven studies earlier sweeps had flagged, 11 of 11.
- How the live proof got there (each run's report is in git history under `aws/ops/reports/latest/`):
  1. The first run was cancelled at the 60-minute limit: one study takes about 15 s on 6,990 bars. The sweep was split
     in two ops.
  2. The next runs flagged up to 23 studies. Three things were the proof's own: a message at the bottom centre in the
     pictures (the watchlist's toast, `#jhwl-toast`: in a browser that has never seen them, each signal alert is
     announced once during the first minutes); pictures compared across a page load (the live page reads fresh bars,
     and during the session the last bar had moved); a picture taken while a study's download was still out.
  3. Four were faults of the page that the local copy could not show (causes 19 to 22). Each was then reproduced
     locally - by delaying a download 3 s, by holding a press 1.7 s, by blocking the page 2.5 s - repaired and
     tested before the last runs.
- Counted as the same picture, and shown enlarged in the ops 6930 report: 4 px at the rounded corner of the readout
  panel at the top left (x 369, y 71 to 140), smoothed a hair differently on 7 of the sweep's comparisons (five a study, 96 studies).

## To take it back

State before = `64aa2d70`.

```
git checkout 64aa2d70 -- chart.html jh-chart-engine.js jh-chart-auction.js jh-chart-sd-ui.js jh-chart-evr.js \
  tests/helpers/chart-macro-line-preservation.cjs tests/chart-crypto-routes.test.js config/page-data-contracts.json engine-manifest.json
git rm -r tests/chart-indicator-controls.test.js tests/fixtures/chart-indicator-controls
```
