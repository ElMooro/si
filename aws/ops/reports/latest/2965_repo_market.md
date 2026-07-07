## 0. Probe NY Fed markets API from the runner

**Status:** success  
**Duration:** 55.3s  
**Finished:** 2026-07-07T06:20:29+00:00  

## Data

| age_min | cf_worker_regime | cf_worker_score | components_live | cp_expected_signal | cp_interp | cp_signal | cp_sofr_iorb_bps | dr_canaries | dr_repo_canary | env_keys | fails | first_row_keys | plumbing_body_status | plumbing_fn_error | plumbing_seconds | plumbing_status | probe_rows | radar_body_status | radar_fn_error | radar_seconds | radar_status | regime | regime_body_status | regime_fn_error | regime_seconds | regime_status | repo_html_panel_live | repo_market_body_status | repo_market_fn_error | repo_market_seconds | repo_market_status | repo_tail | role | rr_funding | rr_keys | rrp_bn | schedule | schema | score | sep17_row | si_series_n | sofr_iorb_bps | source | srf_bn | tail_bps | tail_pctile | tail_series_n | top_episodes | warns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | ['effectiveDate', 'percentPercentile1', 'percentPercentile25', 'percentPercentile75', 'percentPercentile99', 'percentRate', 'revisionIndicator', 'type', 'volumeInBillions'] |  |  |  |  | 27 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"effectiveDate": "2019-09-17", "type": "SOFR", "percentRate": 5.25, "percentPercentile1": 2.25, "percentPercentile25": 5.0, "percentPercentile75": 5.85, "percentPercentile99": 9.0, "volumeInBillions": 1177, "revisionIndicator": ""}] |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET', 'TELEGRAM_CHAT_ID', 'TELEGRAM_TOKEN'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | arn:aws:iam::857687956942:role/justhodl-scheduler-role |  |  |  | updated |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 200 | None | 11.3 | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 0.2 |  |  | 9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | CALM |  |  |  |  |  |  |  |  |  |  |  |  |  | 2.7 |  | 1.0 | 17.9 |  | 260 | -1.0 | NY Fed markets API | 0.0 | 8.0 | 14.3 | 260 | [{"date": "2019-09-17", "tail_bps": 375.0, "sofr_iorb_bps": 315.0}, {"date": "2018-12-31", "tail_bps": 325.0, "sofr_iorb_bps": 60.0}, {"date": "2019-09-18", "tail_bps": 245.0, "sofr_iorb_bps": 45.0}, {"date": "2019-09-16", "tail_bps": 217.0, "sofr_iorb_bps": 33.0}, {"date": "2020-03-16", "tail_bps": 174.0, "sofr_iorb_bps": 16.0}] |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 200 | None | 17.0 | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 200 | None | 2.7 | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 200 | None | 3.5 | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 15 | [{"label": "Onshore repo stress (SOFR plumbing)", "reading": "18/100, tail 8bps", "signal": "DUMP", "lean": -1, "weight": 1.5, "detail": "The dedicated repo-market engine: SOFR p99 tail, SOFR-IORB, RRP buffer and SRF take-up. A funding squeeze bids the dollar (PUMP); glassy repo lets it drift (DUMP) |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"score": 0.38, "repo_stress_score": 17.9, "regime": "CALM", "tail_bps": 8.0, "sofr_iorb_bps": -1.0, "source": "repo-market engine"} | ['blocks_used', 'capital_inflows', 'components', 'cross_border', 'dollar_context', 'elapsed_s', 'engine', 'generated_at', 'liquidity', 'methodology', 'participation', 'posture'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | NORMAL | Repo plumbing functioning normally | NORMAL | -1.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"name": "SOFR p99 Tail (repo-market)", "available": true, "latest_date": "2026-07-02", "tail_bps": 8.0, "z_score_1y": -0.74, "pctile_since_2018": 14.3, "repo_stress_score": 17.9, "repo_regime": "CALM", "signal": "WATCH", "interpretation": "The 99th-percentile SOFR borrower is paying 8bps over the m |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | CALM | 17.9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | [] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ['SRF latest probe failed: HTTP Error 400: Bad Request'] |

## Log
## 1. Env bundle + create justhodl-repo-market

- `06:19:34`   zip: 9355 bytes
## 1. Lambda

- `06:19:34`   Lambda exists — updating
- `06:19:39` ✅   ✓ updated justhodl-repo-market
## 2. EventBridge Scheduler schedule

## 3. Synchronous first run + verify repo-market.json

## 4. Deploy + verify the three fused consumers

- `06:19:51`   zip: 19120 bytes
## 1. Lambda

- `06:19:52`   Lambda exists — updating
- `06:19:57` ✅   ✓ updated justhodl-dollar-radar
- `06:20:14`   zip: 10126 bytes
## 1. Lambda

- `06:20:14`   Lambda exists — updating
- `06:20:17` ✅   ✓ updated justhodl-risk-regime
- `06:20:20`   zip: 13724 bytes
## 1. Lambda

- `06:20:20`   Lambda exists — updating
- `06:20:25` ✅   ✓ updated justhodl-crisis-plumbing
## 5. Live-path checks (warn-level, CDN/pages lag)

## verdict

- `06:20:29` report written: /home/runner/work/si/si/aws/ops/reports/2965.json
- `06:20:29` PASS -- repo market engine live and fused into dollar-radar, risk-regime and crisis-plumbing
