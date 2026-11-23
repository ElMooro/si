# ops 6932 - chart.html: every oscillator window closes, hides and shows; real browser on the live site (read-only), 2026-11-23T22:34+00:00

**Status:** failure  
**Duration:** 1587.7s  
**Finished:** 2026-11-23T23:00:38+00:00  

## Error

```
SystemExit: 1
```

## Data

| failed_checks | oscillators | work_at_every_step |
|---|---|---|
| 1 | 119 | 109 |

## Log
## 0. The live site serves the new build

- `22:34:11` engine and Effort & Spread file are the new build: True
- `22:34:11` installing playwright
- `22:34:13` installing pillow
## 1. The sweep: every oscillator in the Indicators list - on, hidden, shown, closed

- `22:34:38` NVDA daily, 6990 bars, auto refresh off; 215 studies in the list (95 on the price chart, 119 oscillators, Volume); the chart shows bars [6830, 6995]; two pictures of the untouched chart are identical: True
- `23:00:33` 119 studies taken through the four steps in 1554 s: 109 work at every step; 119 of them drew something on this chart (the rest had nothing to draw on a daily stock chart and still switched on, hid, showed and closed by their row)
- `23:00:33` the eye was on screen without hovering on 118 of 118; hidden then shown gave back the same picture on 111 of 119 that drew; closed gave back the chart before on 115 of 119
- `23:00:33`   stoch        Stochastic 14,3            closed, something stays on the chart (25552 px in [517, 875, 983, 946])
- `23:00:33`   stochrsi     Stoch RSI                  closed, something stays on the chart (32828 px in [456, 875, 1044, 946])
- `23:00:33`   macd         MACD 12,26,9               shown again it is not the picture it was (39886 px)
- `23:00:33`   willr        Williams %R                shown again it is not the picture it was (31366 px)
- `23:00:33`   mfi          MFI 14                     shown again it is not the picture it was (31580 px)
- `23:00:33`   ad           A/D                        shown again it is not the picture it was (31986 px)
- `23:00:33`   cmf          CMF 20                     shown again it is not the picture it was (31224 px)
- `23:00:33`   adx          DMI / ADX 14               shown again it is not the picture it was (31798 px)
- `23:00:33`   eld          Elder Ray                  closed, something stays on the chart (24952 px in [521, 875, 978, 946])
- `23:00:33`   fisher       Fisher 10                  shown again it is not the picture it was (3110 px)
- `23:00:33` pictures counted as the same with a few pixels of label smoothing apart (at most 12): 0 comparisons, largest 0 px
- `23:00:33` FAIL: 10 of 119 studies do not close, hide or show cleanly: stoch, stochrsi, macd, willr, mfi, ad, cmf, adx, eld, fisher
- `23:00:33`   rsi        drew 248543 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `23:00:33`   macd       drew 265555 px | hidden: nothing left | shown again: {'px': 39886, 'box': [464, 875, 1036, 946]} | closed: chart as before
- `23:00:33`   effspread  drew 256319 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `23:00:38` Buyback (a section under the chart with its own Close): on {'on': True, 'shown': True} -> its Close -> {'on': False, 'shown': False}
- `23:00:38` ✗ 10 of 119 studies do not close, hide or show cleanly: stoch, stochrsi, macd, willr, mfi, ad, cmf, adx, eld, fisher
