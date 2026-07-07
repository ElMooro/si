## A1. NAAIM stale doc state

**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-07-07T21:51:11+00:00  

## Data

| body | cboe_chart_json | cboe_hist_json | cboe_indices_csv | fn_error | invoke_s | naaim_column_mode | naaim_generated_at | naaim_history_n | naaim_latest | naaim_latest_source | naaim_page | naaim_schedules | sellside |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | "header:NAAIM Number" | "2026-07-03T17:30:15+00:00" | 1043 | {"date": "2026-07-01", "value": 84.69} | "file" |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | {"rule:justhodl-naaim-weekly": "ENABLED cron(30 17 ? * THU,FRI *)"} |  |
| {"ok": true, "value": 84.69, "n": 1043, "state": "NEUTRAL", "provisional": false} |  |  |  | None | 2.1 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | {"status": 200, "bytes": 4000, "head": "<!DOCTYPE html>\n<html dir=\"ltr\" lang=\"en-US\" prefix=\"og: https://ogp.me/ns#\" class=\"no-js no-svg\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"view |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | {"type": "dict", "top_keys": ["schema_version", "method", "generated_at", "target_year", "n_firms", "spx_consensus", "all_firm_targets", "recent_revisions_30d", "macro_consensus", "interpretation"], "rows_n": 0} |
|  |  | {"status": 200, "bytes": 4000, "head": "{\"timestamp\": \"2026-07-07 21:01:11\", \"data\": [{\"date\": \"2006-01-03\", \"volume\": \"0.0\", \"open\": \"31.340000\", \"high\": \"31. |  |  |  |  |  |  |  |  |  |  |  |
|  | {"error": "HTTP Error 403: Forbidden"} |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | {"status": 200, "bytes": 4000, "head": "DATE,OPEN,HIGH,LOW,CLOSE\n01/03/2006,31.340000,31.340000,31.340000,31.340000\n01/04/2006,31.120000,31.120000,31.120000,31.120000\n01/05/2006 |  |  |  |  |  |  |  |  |  |  |

## Log
## A2. NAAIM schedule state

## A3. NAAIM synchronous invoke (capture real error)

- `21:51:10` ---- naaim log tail ----
- `21:51:10` START RequestId: b988b82e-d802-4638-9c4d-b234f3aa50c8 Version: $LATEST
- `21:51:10` page match: 'number is*:</h4' -> 4.0
- `21:51:10` page latest (gated): 2026-07-02 = 4.0
- `21:51:10` history file parsed: 1043 rows via header:NAAIM Number from https://naaim.org/wp-content/uploads/2026/07/USE_Data-since-Inception_2026-07-01
- `21:51:10` file canonical (1043 rows, header:NAAIM Number) — prior accumulation discarded
- `21:51:10` page print REJECTED as junk: 4.0 vs file tail 2026-07-01=84.69
- `21:51:10` wrote data/naaim.json n=1043 cur=84.7 z=0.58 pct=65.8 state=NEUTRAL
- `21:51:10` END RequestId: b988b82e-d802-4638-9c4d-b234f3aa50c8
- `21:51:10` REPORT RequestId: b988b82e-d802-4638-9c4d-b234f3aa50c8	Duration: 1040.27 ms	Billed Duration: 1645 ms	Memory Size: 256 MB	Max Memory Used: 102 MB	Init Duration: 604.20 ms	
- `21:51:10` XRAY TraceId: 1-6a4d74cc-7ded0e6b33a014b7337309fa	SegmentId: 0133324294777dc4	Sampled: true	
## A4. naaim.org probes from runner

## B. sellside-views shape dump

## C. CBOE implied-corr endpoint probes

- `21:51:11` ✅ diagnostics gathered 3/3 sections
- `21:51:11` FAILS=0 WARNS=0
