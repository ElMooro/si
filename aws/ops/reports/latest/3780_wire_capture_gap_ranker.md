# ops 3780 — capture_gap overlay into master-ranker

**Status:** success  
**Duration:** 28.6s  
**Finished:** 2026-07-23T20:07:24+00:00  

## Data

| capture_joined | capture_names | current_top_tickers | forecast_overlap | invoke_seconds | invoke_status | structurally_undervalued | top_tickers | with_catchup |
|---|---|---|---|---|---|---|---|---|
|  | 1771 | 25 | 25 |  |  |  |  |  |
|  |  |  |  | 8.8 | 200 |  |  |  |
| 24 |  |  |  |  |  | 0 | 25 | 23 |

## Log
## G0 — live producer rows

- `20:06:56` ✅ G0.all_rows :: all_rows n=1771
- `20:06:56` ✅ G0.row_ticker :: present
- `20:06:56` ✅ G0.row_capture_gap :: present
- `20:06:56` ✅ G0.row_global_capture_gap :: present
- `20:06:56` ✅ G0.row_tier :: present
- `20:06:56` ✅ G0.row_mcap_share_pct :: present
- `20:06:56` ✅ G0.row_undervaluation_score :: present
## G0 — consumer anchors (grepped, never typed from memory)

- `20:06:56` ✅ G0.reader :: _ck already loaded — reusing, no second fetch
- `20:06:56` ✅ G0.loopvar :: top_tickers is the row list
- `20:06:56` ✅ G0.outkey :: output key confirmed
- `20:06:56` ✅ G0.anchor :: splice anchor unique (after _ck block)
## Forecast join from LIVE artifacts

- `20:06:56` ✅ FORECAST.nonzero :: 25 will join (sample ['ALL', 'AMD', 'AMP', 'BABA', 'BE', 'CHRW', 'FDX', 'FIX'])
## Splice (additive)

- `20:06:56` ✅ spliced + compile clean
- `20:06:56` ✅ SPLICE.marker :: marker in source
- `20:06:56` ✅ SPLICE.structural_kept :: pre-existing structural overlay untouched
- `20:06:56` ✅ SPLICE.no_double_fetch :: still exactly one chokepoint fetch
## Deploy

- `20:06:56`   zip: 102069 bytes
## 1. Lambda

- `20:06:56`   Lambda exists — updating
- `20:06:59` ✅   ✓ updated justhodl-master-ranker
- `20:07:15` ✅ settled attempt 1
- `20:07:15` ✅ DEPLOY.settled :: artifact live
## Invoke + prove non-zero join on LIVE output

- `20:07:24` ✅ LIVE.join_nonzero :: 24 of 25 ranked names carry capture_gap
- `20:07:24` ✅ LIVE.fields_carried :: fields survived into the live artifact
## Ranked names with capture context

- `20:07:24`   FIX    gap=  +4.4pp global= -12.3pp catchup=    -72% tier=WATCH
- `20:07:24`   LRCX   gap=  +1.2pp global=  +0.3pp catchup=    -57% tier=WATCH
- `20:07:24`   TSM    gap= -10.6pp global=  -1.7pp catchup=    300% tier=WATCH
- `20:07:24`   SPG    gap= -11.2pp global=  -2.5pp catchup=    -15% tier=WATCH
- `20:07:24`   UBS    gap= -16.4pp global= -69.3pp catchup=    -37% tier=NONE
- `20:07:24`   PWR    gap= -16.5pp global= -33.0pp catchup=    -46% tier=WATCH
- `20:07:24`   STX    gap= -18.0pp global=  -4.5pp catchup=    -55% tier=WATCH
- `20:07:24`   IPGP   gap= -24.0pp global= +36.1pp catchup=    -45% tier=WATCH
- `20:07:24`   AMD    gap= -30.4pp global= -14.3pp catchup=    -64% tier=WATCH
- `20:07:24`   AMP    gap= -35.4pp global= -25.2pp catchup=     31% tier=WATCH
- `20:07:24`   MPC    gap= -37.9pp global= -77.1pp catchup=    -13% tier=WATCH
- `20:07:24`   ALL    gap= -39.1pp global= -41.1pp catchup=     64% tier=WATCH
## Additive contract

- `20:07:24` ✅ ADDITIVE.top_tickers :: present
- `20:07:24` ✅ ADDITIVE.top_macro :: present
- `20:07:24` ✅ ADDITIVE.alerts :: present
- `20:07:24` ✅ ADDITIVE.feed_health :: present
- `20:07:24` ✅ ADDITIVE.structural_alive :: structural counter still reported
## VERDICT

- `20:07:24` ✅ PASS_ALL — capture_gap now reaches both the setups desk and the ranker
