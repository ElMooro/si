# Candidate Lambda Review — full evidence

Generated: 2026-05-31T13:39:50.984770+00:00

Total reviewed: **25**

## Recommendation summary

- **INVESTIGATE**: 1
- **KEEP**: 24

> **How to use this report**: For each candidate, the evidence
> below should be enough to make a KEEP / DELETE / INVESTIGATE decision.
> The auto-recommendation is conservative — when in doubt, INVESTIGATE.
> If you see ⚠️ flags (function URL, ESM, page refs), do NOT delete without
> verifying the downstream consumers can handle the loss.



## INVESTIGATE (1)

### `justhodl-ab-test`

- **Recommendation**: `INVESTIGATE`
- **Reasons**: ACTIVE_29_INVOKES_30D, NAME_HAS_TEST
- **Description**: A/B test of competing prompt strategies
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 29
- **Last log event**: 2026-05-30T18:27:03.946000+00:00
- **Code stats**: 351 LOC, boto3=True fred=False polygon=False dynamodb=True


## KEEP (24)

### `justhodl-backtest-harness`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_31_INVOKES_30D
- **Description**: Daily snapshot of all signal types + forward-return tracker. DDB table justhodl-backtest.
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 31
- **Last log event**: 2026-05-30T22:00:34.405000+00:00
- **Code stats**: 449 LOC, boto3=True fred=False polygon=False dynamodb=True

### `justhodl-breadth-thrust`

- **Recommendation**: `KEEP`
- **Reasons**: SOME_INVOKES_2_30D
- **Description**: Zweig Breadth Thrust + Whaley January Barometer + Coppock Curve. 11-of-11 historical 12m positive returns when fired. State machine NULL/ARMED/FIRED/COOLDOWN. Polygon grouped aggs for NYSE A/D ratio, 
- **Last invoke (30d)**: 2026-05-20T13:35:00+00:00
- **Invoke count (30d)**: 2
- **Last log event**: 2026-05-20T15:31:12.341000+00:00
- **Code stats**: 704 LOC, boto3=True fred=False polygon=True dynamodb=False

### `justhodl-calls-backtest`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_29_INVOKES_30D
- **Description**: Replays decisive-call ledger as SPY-exposure backtest. Daily.
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 29
- **Last log event**: 2026-05-31T11:00:13.789000+00:00
- **Code stats**: 305 LOC, boto3=True fred=False polygon=True dynamodb=False

### `justhodl-capital-return`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_18_INVOKES_30D, WRITES_S3
- **Description**: Capital-return cannibal screen - companies shrinking their share count via FCF-funded net buybacks at a reasonable valuation with a healthy business. Shareholder-yield factor with funding/valuation/qu
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 18
- **Last log event**: 2026-05-31T11:00:14.318000+00:00
- **S3 outputs written**: `data/capital-return.json`
- **Code stats**: 238 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-carry-surface` ⚠️ PAGES

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_56_INVOKES_30D, REFERENCED_BY_1_PAGES
- **Description**: UNIVERSAL CARRY SURFACE — institutional cross-asset carry engine. Answers: 'which asset is the market paying me most to hold, right now?' across equity / FX / FI / commodity / crypto. Z-scored within 
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 56
- **Last log event**: 2026-05-31T12:01:42.363000+00:00
- **Pages referencing**: `carry.html`
- **Code stats**: 826 LOC, boto3=True fred=True polygon=False dynamodb=False

### `justhodl-coffee-can`

- **Recommendation**: `KEEP`
- **Reasons**: EVENTBRIDGE_TARGET, ACTIVE_14_INVOKES_30D
- **Description**: Coffee-Can Tracker — multibagger holding-discipline + thesis-break detection
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 14
- **Last log event**: 2026-05-31T11:00:13.775000+00:00
- **EventBridge rules**: coffee-can-daily
- **Code stats**: 447 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-firm-stress`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_14_INVOKES_30D, WRITES_S3
- **Description**: Firm Stress Desk - re-prices the firm book through 15 scenarios (historical replays + macro shocks) via cached six-factor loadings, attributes each loss by desk / sector / name and runs a reverse stre
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 14
- **Last log event**: 2026-05-31T03:00:40.794000+00:00
- **S3 outputs written**: `data/firm-stress.json`
- **Code stats**: 382 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-forward-returns`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_7_INVOKES_30D, WRITES_S3
- **Description**: Capital Compass — forward 10y expected returns per asset class (Damodaran/GMO/AQR methodology). Earnings yield + growth for stocks, YTM for bonds, Erb-Harvey gold, real yield TIPS, AFFO REITs. Vs 30y 
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 7
- **Last log event**: 2026-05-31T12:00:59.552000+00:00
- **S3 outputs written**: `data/forward-returns.json`
- **Code stats**: 597 LOC, boto3=True fred=True polygon=False dynamodb=False

### `justhodl-gdelt-sentiment` ⚠️ URL

- **Recommendation**: `KEEP`
- **Reasons**: HAS_FUNCTION_URL, ACTIVE_1442_INVOKES_30D
- **Description**: GDELT 2.0 global news + geopolitical sentiment. Pulls latest 15-min GKG batch, filters financial themes, computes per-asset sentiment.
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 1442
- **Last log event**: 2026-05-31T13:30:12.445000+00:00
- **Function URL**: `https://sfots654xx2sprhteqdtdpdmqy0kapla.lambda-url.us-east-1.on.aws/` ← HTTP callable, do not delete
- **Code stats**: 309 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-global-macro`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_28_INVOKES_30D
- **Description**: Per-country economic regime aggregator. 15 countries × 5 dimensions (unemp, PMI, IP YoY, equity ETF, currency). Composite Health Score 0-100 + HOT/MIXED/COLD regime classification.
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 28
- **Last log event**: 2026-05-30T22:01:03.230000+00:00
- **Code stats**: 326 LOC, boto3=True fred=True polygon=False dynamodb=False

### `justhodl-gold-equity-rotation`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_11_INVOKES_30D, WRITES_S3
- **Description**: Gold-equity rotation. 5-factor: SPY/GLD ratio z-score, 20d momentum + MA50/MA200, 5d persistence, GDX+SLV strength, DXY overlay. GOLD_BREAKOUT_RICH or EQUITY_DOMINANT_RICH. Erb-Harvey 2013, Baur-Lucey
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 11
- **Last log event**: 2026-05-30T22:45:04.537000+00:00
- **S3 outputs written**: `data/gold-equity-rotation.json`
- **Code stats**: 368 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-history-api` ⚠️ URL ⚠️ PAGES

- **Recommendation**: `KEEP`
- **Reasons**: HAS_FUNCTION_URL, SOME_INVOKES_3_30D, REFERENCED_BY_1_PAGES
- **Description**: Read-only API for justhodl-history DDB. Function URL exposes /index, /snapshot, /latest, /timestamps. Reserved concurrency=5.
- **Last invoke (30d)**: 2026-05-06T13:40:00+00:00
- **Invoke count (30d)**: 3
- **Last log event**: 2026-05-06T17:59:46.543000+00:00
- **Function URL**: `https://67eohg2wno7aaceipjmcro4o3y0xgsiw.lambda-url.us-east-1.on.aws/` ← HTTP callable, do not delete
- **Pages referencing**: `audit.html`
- **Code stats**: 250 LOC, boto3=True fred=False polygon=False dynamodb=True

### `justhodl-magic-formula`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_11_INVOKES_30D, WRITES_S3
- **Description**: Greenblatt Magic Formula Screener: rank S&P 500 (ex-fin/utils/REITs) by Earnings Yield + ROIC. GuruFocus signature. Greenblatt 2010 30.8% CAGR backtest. Daily 22:45 UTC.
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 11
- **Last log event**: 2026-05-30T22:45:19.781000+00:00
- **S3 outputs written**: `data/magic-formula.json`
- **Code stats**: 349 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-metals-miners`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_13_INVOKES_30D, WRITES_S3
- **Description**: Metals & Miners screen - scores gold/silver/copper/uranium/lithium miners as leveraged calls on their metal vs proxy-ETF trend regimes, gated on balance-sheet survivability, with metal-anchored price 
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 13
- **Last log event**: 2026-05-30T14:10:16.971000+00:00
- **S3 outputs written**: `screener/metals-miners.json`
- **Code stats**: 493 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-opex-calendar`

- **Recommendation**: `KEEP`
- **Reasons**: SOME_INVOKES_2_30D, WRITES_S3
- **Description**: OPEX/0DTE gamma pinning calendar (Edge #8). Classifies trading day into OPEX regime (PRE/OPEX/POST/QUAD), computes Polygon max-pain, backtests 5y SPY post-OPEX returns, provides state-aware trade tick
- **Last invoke (30d)**: 2026-05-20T13:40:00+00:00
- **Invoke count (30d)**: 2
- **Last log event**: 2026-05-20T14:06:52.745000+00:00
- **S3 outputs written**: `data/opex-calendar.json`
- **Code stats**: 732 LOC, boto3=True fred=False polygon=True dynamodb=False

### `justhodl-portfolio-snapshot`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_517_INVOKES_30D
- **Description**: Portfolio enrichment (#9). Joins positions+watchlist with alpha/confluence/regime/sentiment + latest prices. Auto-syncs watchlist with TIER S/A from alpha-score.
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 517
- **Last log event**: 2026-05-31T13:05:23.618000+00:00
- **Code stats**: 431 LOC, boto3=True fred=False polygon=True dynamodb=True

### `justhodl-post-earnings-mean-rev`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_13_INVOKES_30D, WRITES_S3
- **Description**: Post-earnings drift exhaustion / mean-rev scanner. 4-factor: earnings 5-15td ago + RSI<=25 or >=75 + price ext >=1.5 std vs 50dMA + IV crushed. Counter-trend 5-10d hold. Bernard-Thomas 1989, Bali 2008
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 13
- **Last log event**: 2026-05-31T00:00:44.494000+00:00
- **S3 outputs written**: `data/post-earnings-mean-rev.json`
- **Code stats**: 466 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-public-api-demo` ⚠️ URL

- **Recommendation**: `KEEP`
- **Reasons**: HAS_FUNCTION_URL, ACTIVE_106_INVOKES_30D
- **Description**: Reference public API endpoint demonstrating api_auth.py.
- **Last invoke (30d)**: 2026-05-06T13:40:00+00:00
- **Invoke count (30d)**: 106
- **Last log event**: 2026-05-06T21:24:43.862000+00:00
- **Function URL**: `https://odoy2bydzufzjbp765n3ix6w5u0rvqmj.lambda-url.us-east-1.on.aws/` ← HTTP callable, do not delete
- **Code stats**: 114 LOC, boto3=False fred=False polygon=False dynamodb=False

### `justhodl-smart-money-holdings` ⚠️ PAGES

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_28_INVOKES_30D, REFERENCED_BY_1_PAGES, WRITES_S3
- **Description**: Builds inverse mapping {symbol: top funds holding it} for screener integration
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 28
- **Last log event**: 2026-05-31T11:00:50.364000+00:00
- **S3 outputs written**: `screener/smart-money-holdings.json`
- **Pages referencing**: `screener/index.html`
- **Code stats**: 396 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-smart-money-tracker`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_24_INVOKES_30D, WRITES_S3
- **Description**: Top hedge funds / institutional holders activity (13F-based)
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 24
- **Last log event**: 2026-05-31T11:00:38.522000+00:00
- **S3 outputs written**: `screener/smart-money.json`
- **Code stats**: 265 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-tax-plan` ⚠️ URL

- **Recommendation**: `KEEP`
- **Reasons**: HAS_FUNCTION_URL, ACTIVE_10_INVOKES_30D, WRITES_S3
- **Description**: Tax-Aware Portfolio Engine. Per-position LT/ST classification + tax-if-sold-today. After-tax forward returns (taxable/IRA/Roth) per Capital Compass asset. Tax-loss harvest candidates with substitute E
- **Last invoke (30d)**: 2026-05-30T13:35:00+00:00
- **Invoke count (30d)**: 10
- **Last log event**: 2026-05-31T11:00:17.386000+00:00
- **Function URL**: `https://a2orhvvfva3kh5r6jx5soijcbm0uecuw.lambda-url.us-east-1.on.aws/` ← HTTP callable, do not delete
- **S3 outputs written**: `data/tax-plan-snapshot.json`
- **Code stats**: 658 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-tic-flows`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_6_INVOKES_30D, WRITES_S3
- **Description**: TIC Flows — foreign Treasury holdings + de-dollarization tracker
- **Last invoke (30d)**: 2026-05-30T13:40:00+00:00
- **Invoke count (30d)**: 6
- **Last log event**: 2026-05-30T22:01:01.855000+00:00
- **S3 outputs written**: `data/tic-flows.json`
- **Code stats**: 281 LOC, boto3=True fred=True polygon=False dynamodb=False

### `justhodl-vix-backwardation-trigger`

- **Recommendation**: `KEEP`
- **Reasons**: SOME_INVOKES_2_30D
- **Description**: Institutional once-per-cycle capitulation buy signal. State machine NULL->WARM->ARMED->FIRED->COOLDOWN based on VIX9D/VIX/VIX3M term structure + VVIX panic gauge. Emits retail trade ticket + forward 1
- **Last invoke (30d)**: 2026-05-20T13:35:00+00:00
- **Invoke count (30d)**: 2
- **Last log event**: 2026-05-20T15:30:59.941000+00:00
- **Code stats**: 575 LOC, boto3=True fred=False polygon=False dynamodb=False

### `justhodl-vol-target-unwind`

- **Recommendation**: `KEEP`
- **Reasons**: ACTIVE_8_INVOKES_30D, WRITES_S3
- **Description**: Vol-target unwind trigger (Edge #4). RV21 vs 16/20/25 thresholds; estimates AUM mechanically rebalancing; outputs trade ticket and forward expectations.
- **Last invoke (30d)**: 2026-05-20T13:40:00+00:00
- **Invoke count (30d)**: 8
- **Last log event**: 2026-05-20T15:38:46.045000+00:00
- **S3 outputs written**: `data/vol-target-unwind.json`
- **Code stats**: 700 LOC, boto3=True fred=False polygon=False dynamodb=False
