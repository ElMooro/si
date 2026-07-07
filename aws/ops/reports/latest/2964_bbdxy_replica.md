## 0. Preserve live env

**Status:** success  
**Duration:** 143.2s  
**Finished:** 2026-07-07T05:39:28+00:00  

## Data

| age_min | as_of | bbdxy_available | bilateral_ccys | bilaterals | body_status | breadth_1m_pp | breadth_verdict | cf_schema | cf_worker_serves_v3 | constituents | contrib_rows_1m | contrib_sum_pp | dollar_html_panel_live | env_keys | fails | fn_error | invoke_seconds | level | log_index_1m_pp | memory | missing | present | regime | schema | status | timeout | vs_fed_broad_1m_pp | warns | weights_effective | weights_sum |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | ['FRED_KEY', 'TELEGRAM_CHAT_ID', 'TELEGRAM_TOKEN'] |  |  |  |  |  | 256 |  |  |  |  |  | 180 |  |  |  |  |
|  |  |  |  |  | 200 |  |  |  |  |  |  |  |  |  |  | None | 14.1 |  |  |  |  |  |  |  | 200 |  |  |  |  |  |
| 0.2 |  |  |  | 13 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | NEUTRAL | 3.0 |  |  |  |  |  |  |
|  |  |  | ['AUD', 'BRL', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'INR', 'JPY', 'KRW', 'MXN', 'SGD', 'TWD'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 2026-07-02 | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 99.553 |  |  | [] |  |  |  |  |  |  |  | 2025-07-01 |  |
|  |  |  |  |  |  |  |  |  |  | 12 |  |  |  |  |  |  |  |  |  |  |  | 12 |  |  |  |  |  |  |  | 100.0 |
|  |  |  |  |  |  | -0.2 | IN LINE -- broad basket and DXY are moving together |  |  |  | 12 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.05 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 1.434 |  |  |  |  |  |  | 1.43 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 3.0 | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [] |  |  |  |  |  |  |  |  |  |  |  |  | [] |  |  |

## Log
## 1. Deploy v3 from repo source

- `05:37:05`   zip: 18843 bytes
## 1. Lambda

- `05:37:05`   Lambda exists — updating
- `05:37:08` ✅   ✓ updated justhodl-dollar-radar
## 2. Synchronous full run

## 3. Verify published JSON

## 4. Live-path checks (warn-level)

## verdict

- `05:39:28` report written: /home/runner/work/si/si/aws/ops/reports/2964.json
- `05:39:28` PASS -- BBDXY replica live with full attribution
