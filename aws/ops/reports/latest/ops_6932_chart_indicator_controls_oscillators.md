# ops 6932 - chart.html: every oscillator window closes, hides and shows; real browser on the live site (read-only), 2026-11-23T20:19+00:00

**Status:** failure  
**Duration:** 1726.1s  
**Finished:** 2026-11-23T20:48:36+00:00  

## Error

```
SystemExit: 1
```

## Data

| failed_checks | oscillators | work_at_every_step |
|---|---|---|
| 1 | 119 | 114 |

## Log
## 0. The live site serves the new build

- `20:19:51` engine and Effort & Spread file are the new build: True
- `20:19:51` installing playwright
- `20:19:53` installing pillow
## 1. The sweep: every oscillator in the Indicators list - on, hidden, shown, closed

- `20:20:11` NVDA daily, 6990 bars, auto refresh off; 215 studies in the list (95 on the price chart, 119 oscillators, Volume); the chart shows bars [6830, 6995]; two pictures of the untouched chart are identical: True
- `20:48:31` 119 studies taken through the four steps in 1700 s: 114 work at every step; 119 of them drew something on this chart (the rest had nothing to draw on a daily stock chart and still switched on, hid, showed and closed by their row)
- `20:48:31` the eye was on screen without hovering on 118 of 118; hidden then shown gave back the same picture on 115 of 119 that drew; closed gave back the chart before on 115 of 119
- `20:48:31`   macd         MACD 12,26,9               shown again it is not the picture it was (41026 px)
- `20:48:31`   cci          CCI 20                     closed, something stays on the chart (25147 px in [520, 875, 980, 946])
- `20:48:31`   willr        Williams %R                shown again it is not the picture it was (32145 px)
- `20:48:31`   mfi          MFI 14                     closed, something stays on the chart (25194 px in [520, 875, 980, 946])
- `20:48:31`   chaikin      Chaikin Osc                shown again it is not the picture it was (31512 px) | closed, something stays on the chart (24664 px in [524, 875, 976, 946])
- `20:48:31` FAIL: 5 of 119 studies do not close, hide or show cleanly: macd, cci, willr, mfi, chaikin
- `20:48:31`   rsi        drew 249431 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `20:48:31`   macd       drew 269494 px | hidden: nothing left | shown again: {'px': 41026, 'box': [456, 875, 1044, 946]} | closed: chart as before
- `20:48:31`   effspread  drew 259596 px | hidden: nothing left | shown again: same picture | closed: chart as before
- `20:48:36` Buyback (a section under the chart with its own Close): on {'on': True, 'shown': True} -> its Close -> {'on': False, 'shown': False}
- `20:48:36` ✗ 5 of 119 studies do not close, hide or show cleanly: macd, cci, willr, mfi, chaikin
