# ops 6932 - chart.html: every oscillator window closes, hides and shows; real browser on the live site (read-only), 2026-11-24T00:30+00:00

**Status:** success  
**Duration:** 2307.9s  
**Finished:** 2026-11-24T01:08:38+00:00  

## Data

| failed_checks | oscillators | work_at_every_step |
|---|---|---|
| 0 | 119 | 119 |

## Log
## 0. The live site serves the new build

- `00:30:11` engine and Effort & Spread file are the new build: True
- `00:30:11` installing playwright
- `00:30:13` installing pillow
## 1. The sweep: every oscillator in the Indicators list - on, hidden, shown, closed

- `00:30:33` NVDA daily, 6990 bars, auto refresh off; 215 studies in the list (95 on the price chart, 119 oscillators, Volume); the chart shows bars [6830, 6995]; two pictures of the untouched chart are identical: True
- `01:08:31` 119 studies taken through the four steps in 2278 s: 119 work at every step; 119 of them drew something on this chart (the rest had nothing to draw on a daily stock chart and still switched on, hid, showed and closed by their row)
- `01:08:31` the eye was on screen without hovering on 118 of 118; hidden then shown gave back the same picture on 118 of 119 that drew; closed gave back the chart before on 118 of 119
- `01:08:31` half a second after switching on, the study's row or window (with its cross) was there for 118 of 119; a download still out after 6 s when its picture was due: 13 times
- `01:08:31` pictures counted as the same although a few pixels apart (at most 12): 0 comparisons
- `01:08:31`   rsi        drew 236488 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `01:08:31`   macd       drew 241220 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `01:08:31`   effspread  drew 256137 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `01:08:38` Buyback (a section under the chart with its own Close): on {'on': True, 'shown': True} -> its Close -> {'on': False, 'shown': False}
- `01:08:38` ✅ LIVE: 119 of 119 oscillator windows switch on, fold away, come back and close with the chart returning to the same picture
