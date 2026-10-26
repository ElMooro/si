# ops 6681 - factor book v1.0.6 first light (FINRA short interest) + deployed versions after the fleet lane

**Status:** success  
**Duration:** 350.8s  
**Finished:** 2026-10-26T20:46:12+00:00  

## Log
## 0. deployed configuration after lane FLEET2

- `20:40:22` justhodl-analytics-snapshot: 1024 MB / 600 s, modified 2026-10-26T15:54:18
- `20:40:22` justhodl-earnings-whisper: 1536 MB / 180 s, modified 2026-10-26T15:54:46
- `20:40:22` justhodl-event-flow-monitor: 1024 MB / 300 s, modified 2026-10-26T15:55:11
- `20:40:22` justhodl-signal-genealogy: 2048 MB / 600 s, modified 2026-10-26T15:59:37
- `20:40:22` justhodl-eps-revision-velocity: 1024 MB / 300 s, modified 2026-10-26T17:23:07
- `20:40:22` justhodl-momentum-leaders: 1024 MB / 300 s, modified 2026-10-26T17:55:06
- `20:40:23` justhodl-earnings-pead: 1024 MB / 600 s, modified 2026-10-26T17:20:06
- `20:40:23` justhodl-estimate-revisions: 512 MB / 120 s, modified 2026-10-26T17:24:17
- `20:40:23` justhodl-pump-positioning: 768 MB / 300 s, modified 2026-10-26T18:10:29
- `20:40:23` justhodl-bank-factors: 3008 MB / 900 s, modified 2026-10-26T16:54:03
- `20:40:23` justhodl-equity-research: 1024 MB / 300 s, modified 2026-10-26T17:23:50
- `20:40:23` justhodl-magic-formula: 1536 MB / 900 s, modified 2026-10-26T17:52:21
- `20:40:24` book manifest before: version 1.0.3 generated None coverage.finra_short None
## 1. one async join

- `20:40:24` accepted 202 (request 69a1c1cf-5aae-4c93-9e8d-07f12217d8d8)
- `20:46:12` REPORT: Duration: 331783.06 ms Max Memory Used: 1153 MB Init Duration: 544.68 ms
- `20:46:12`    20:40:25 | [bank-factors] v1.0.6 start; budget 840s
- `20:46:12`    20:42:10 | [bank-factors] float 88206, short 3608, short-volume 1872, finra short 0, calendars 10091/4776 (106s)
## 2. the published book

- `20:46:12` manifest: version 1.0.6 generated None n None | coverage.finra_short 0 (short 3608) | sources.finra_short {"tickers": 0} | as_of.finra-short-interest None
- `20:46:12` matrix fields: 113; finra columns present: ['finra_short_interest', 'finra_days_to_cover', 'finra_si_change_pct', 'finra_vs_polygon_si_pct']
- `20:46:12` AAPL: short_source polygon short_pct_float 0.8289 si_settlement 2026-09-30 | polygon SI 120763539.0 | finra SI None dtc None change% None vs-polygon% None settlement None
- `20:46:12` GME: short_source None short_pct_float None si_settlement None | polygon SI None | finra SI None dtc None change% None vs-polygon% None settlement None
- `20:46:12` SOFI: short_source None short_pct_float None si_settlement None | polygon SI None | finra SI None dtc None change% None vs-polygon% None settlement None
- `20:46:12` PLTR: short_source None short_pct_float None si_settlement None | polygon SI None | finra SI None dtc None change% None vs-polygon% None settlement None
- `20:46:12` TSLA: short_source None short_pct_float None si_settlement None | polygon SI None | finra SI None dtc None change% None vs-polygon% None settlement None
- `20:46:12` ✅ ops 6681 complete
