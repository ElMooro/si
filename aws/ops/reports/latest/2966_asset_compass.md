## 0. Env bundle + live-feed probes from the runner

**Status:** success  
**Duration:** 39.0s  
**Finished:** 2026-07-07T17:57:55+00:00  

## Data

| age_min | assets_n | coingecko_btc_usd | compass_body | compass_fn_error | compass_seconds | compass_status | context | env_keys | er_modeled | fred_DGS1 | fred_DGS2 | fred_EXPINF1YR | gld_bo | gold_beta | gold_beta_obs | growth_proxy | gsr | gsr_z | infl_1y | infl_src | polygon_gld_bars | real_1y_fwd | rf_1y_fwd | rf_dir | rf_now | role | schedule | schema | slv_bo | top_asym | warns_engine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 3.96 | 4.14 | 3.01917236 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 8 |  |  |  |  |  |  |  |  |  |  |
|  |  | 64101 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | arn:aws:iam::857687956942:role/justhodl-scheduler-role | updated |  |  |  |  |
|  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"assets\": 18, \"er_modeled\": 13, \"warns\": 0}"} | None | 34.1 | 200 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 0.0 | 18 |  |  |  |  |  |  |  | 13 |  |  |  | NONE | -5.79 | 743 | 1.59 | 6.89 | -1.42 | 3.01917236 | EXPINF1YR (Cleveland Fed 1y) |  | 1.3 | 4.32 | HIGHER | 3.96 |  |  | 1.0 | NONE | [{"ticker": "ETH", "label": "Ethereum", "score": 84.0, "ratio": 25.0, "status": "ACTIONABLE"}, {"ticker": "GLD", "label": "Gold", "score": 69.8, "ratio": 25.0, "status": "WATCH"}, {"ticker": "SLV", "label": "Silver", "score": 69.8, "ratio": 25.0, "status": "WATCH"}] | 0 |
|  |  |  |  |  |  |  | {"risk_regime": null, "cross_asset_roro": null, "rv_dislocations": 0} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
## 1. Deploy justhodl-asset-compass

- `17:57:18`   zip: 10459 bytes
## 1. Lambda

- `17:57:18`   Lambda exists — updating
- `17:57:21` ✅   ✓ updated justhodl-asset-compass
## 2. EventBridge Scheduler schedule

## 3. Synchronous first run + hard verify

## 4. Sibling context (warn-only)

## verdict

- `17:57:55` FAILS=0 WARNS=1
- `17:57:55` report written: /home/runner/work/si/si/aws/ops/reports/2966.json
