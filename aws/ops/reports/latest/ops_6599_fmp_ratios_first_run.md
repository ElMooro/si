# ops 6599 - justhodl-fmp-ratios: declare, first run, verify; tv-bars refresh diagnosis

**Status:** success  
**Duration:** 435.8s  
**Finished:** 2026-10-21T15:15:05+00:00  

## Data

| after | before | note | step |
|---|---|---|---|
| 2026-10-21T15:07:53+00:00 / 506 tickers (506 fresh) | 2026-09-15T04:24:38.468021+00:00 / 40 tickers | schema fmp-ratios.v1 | fmp-ratios first run |

## Log
## 1. the function and its schedule exist (deploy-lambdas)

- `15:07:50` justhodl-fmp-ratios: runtime python3.12, memory 1024, timeout 900, modified 2026-10-21T15:03:59, state Active
- `15:07:50` env keys: ['FMP_KEY', 'FMP_RATIOS_MAX', 'S3_BUCKET'] (values never printed)
- `15:07:50` schedule justhodl-fmp-ratios-daily: cron(38 22 * * ? *) ENABLED tz UTC target justhodl-fmp-ratios
## 2. deployed manifest := committed (additions-only, live-verified)

- `15:07:51` S3 1024 entries, repo 1025; added [('scheduler', 'justhodl-fmp-ratios-daily')] removed 0 changed 0
- `15:07:52` ✅ manifest uploaded (+1), backup backups/schedule-manifest/20261021T150751Z/
## 3. first run now

- `15:07:53` book before: generated_at 2026-09-15T04:24:38.468021+00:00, 40 tickers
- `15:07:53` async invoke status 202 at 15:07:53
- `15:15:01` book after: generated_at 2026-10-21T15:07:53+00:00, engine justhodl-fmp-ratios, 2038400 bytes, 506 tickers (506 fresh, 0 kept from 2026-09-15T04:24:38.468021+00:00); universe {"core": 40, "tv_universe": 2, "sp500": 503, "selected": 506, "candidates": 506, "attempted": 506, "ok": 506, "errors": 0, "skipped_for_time": 0, "kept_previous": 0, "total_rows": 506}; errors 0; key_status {"status": "ok"}
- `15:15:01` AAPL: as_of 2026-10-21T15:07:53+00:00 price 342.92 pe 39.15639269406393 ev_ebitda 35.2 next_earnings 2026-11-02 financials years ['2023', '2024', '2025']
- `15:15:01` error sample: {}
- `15:15:02` fmp_book.row('AAPL') via the shared reader: ticker AAPL as_of 2026-10-21T15:07:53+00:00 pe 39.15639269406393; as_of() 2026-10-21T15:07:53+00:00; key_status() ok
## 4. why the TradingView bar bank stopped at 2026-10-19 (read-only)

- `15:15:05` tv-bars log (14 h, Complete): [["02:30:05", "REPORT RequestId: e6c5837d-3837-4110-8446-8d48c36e2617\tDuration: 1106.18 ms\tBilled Duration: 1534 ms\tMemory Size: 1024 MB\tMax Memory Used: 111 MB\tInit Duration: 426.88 ms\t XRAY TraceId: 1-6ad823aa-644"], ["02:30:04", "REPORT RequestId: 9663ef17-0dc6-4b19-93ec-8484bfc22d93\tDuration: 776.80 ms\tBilled Duration: 1313 ms\tMemory Size: 1024 MB\tMax Memory Used: 116 MB\tInit Duration: 535.82 ms\t XRAY TraceId: 1-6ad823aa-6448"], ["02:30:04", "REPORT RequestId: 2a08dd4b-f88e-4e58-82d7-46b1de45ec95\tDuration: 594.27 ms\tBilled Duration: 1182 ms\tMemory Size: 1024 MB\tMax Memory Used: 110 MB\tInit Duration: 587.57 ms\t XRAY TraceId: 1-6ad823aa-6448"], ["02:30:04", "REPORT RequestId: 83d9f3b3-140b-4abc-8b64-f1216eeb22bd\tDuration: 614.30 ms\tBilled Duration: 615 ms\tMemory Size: 1024 MB\tMax Memory Used: 113 MB\t XRAY TraceId: 1-6ad823aa-64486bce23b7adc413f67bc0\tSegme"], ["02:30:03", "REPORT RequestId: ba6ad823-a8f0-42dd-a454-3004d1e511ea\tDuration: 806.98 ms\tBilled Duration: 1356 ms\tMemory Size: 1024 MB\tMax Memory Used: 107 MB\tInit Duration: 548.87 ms\t XRAY TraceId: 1-6ad823aa-6448"]]
- `15:15:05` index: updated_at 2026-10-21T02:30:04+00:00 n_symbols 309/309; failures 4303; last-bar dates [["2026-10-19", 288], ["2026-10-02", 11], ["2026-10-04", 3], ["2026-09-30", 1], ["2025-01-08", 1], ["2024-03-28", 1]]; as_of dates [["2026-10-19", 288], ["2026-10-04", 16], ["2026-09-03", 2], ["2026-10-06", 1], ["2026-09-12", 1], ["2026-10-12", 1]]
- `15:15:05` most recent failures: [["2026-10-16T14:50:51+00:00", "C:EURSGD", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T15:04:52+00:00", "US:F", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T17:01:40+00:00", "US:ABNB", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T17:05:51+00:00", "US:SPCX", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T18:18:58+00:00", "US:GFFFX", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T18:19:46+00:00", "US:BMOP", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T19:22:35+00:00", "US:MMS", "TradingView upgrade refused (HTTP 400); no fallback attempted"], ["2026-10-16T20:06:53+00:00", "US:VFFSX", "TradingView upgrade refused (HTTP 400); no fallback attempted"]]
- `15:15:05` SSM /justhodl/tradingview/sessionid: version 1 last modified 2026-07-07 04:01:20 (value never read)
- `15:15:05` ⚠ ssm metadata: ParameterNotFound
- `15:15:05` ✅ fmp-ratios declared + refreshed (506 fresh rows); tv-bars diagnosis read
