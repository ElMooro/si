# Data Source Audit — Is everything pulling fresh values?

**Status:** success  
**Duration:** 3.8s  
**Finished:** 2026-04-23T22:11:04+00:00  

## Data

| cftc_keys | cftc_present | crypto_count | fred_coverage_pct | fred_nulls | fred_total | news_present | stocks_count | stocks_coverage_pct | stocks_null |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 0.0 | 233 | 233 |  |  |  |  |
|  |  |  |  |  |  |  | 187 | 100.0 | 0 |
|  |  | 25 |  |  |  |  |  |  |  |
| 9 | True |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | True |  |  |  |

## Log
## 0. Load data/report.json — anchor for all checks

- `22:11:01`   report.json: 1,720,189 bytes, 0.5 min old
- `22:11:01`   Top-level keys: ['ai_analysis', 'ath_breakouts', 'cftc_positioning', 'crypto', 'crypto_global', 'ecb_ciss', 'fetch_time_seconds', 'fred', 'generated_at', 'khalid_index', 'market_flow', 'net_liquidity', 'news', 'risk_dashboard', 'sectors', 'signals', 'stats', 'stocks', 'ticker_names', 'version']
## 1. FRED — 233 series

- `22:11:01`   Total FRED series in report: 233
- `22:11:01`   Series with null/zero value: 233
- `22:11:01`   Category breakdown:
- `22:11:01`     ⚠ commodities: 15 series, 15 null/zero
- `22:11:01`     ⚠ credit: 21 series, 21 null/zero
- `22:11:01`     ⚠ dxy: 15 series, 15 null/zero
- `22:11:01`     ⚠ ecb: 15 series, 15 null/zero
- `22:11:01`     ⚠ global_cycle: 16 series, 16 null/zero
- `22:11:01`     ⚠ global_liquidity: 8 series, 8 null/zero
- `22:11:01`     ⚠ ice_bofa: 25 series, 25 null/zero
- `22:11:01`     ⚠ inflation: 10 series, 10 null/zero
- `22:11:01`     ⚠ liquidity: 24 series, 24 null/zero
- `22:11:01`     ⚠ macro: 33 series, 33 null/zero
- `22:11:01`     ⚠ pmi_world: 15 series, 15 null/zero
- `22:11:01`     ⚠ risk: 14 series, 14 null/zero
- `22:11:01`     ⚠ systemic_risk: 1 series, 1 null/zero
- `22:11:01`     ⚠ treasury: 21 series, 21 null/zero
- `22:11:01` 
- `22:11:01`   Critical indicators:
- `22:11:01`     DGS10 (10-year yield): value=None, date=2026-04-22, chg_1d=None
- `22:11:01`     WALCL (Fed balance sheet): value=None, date=2026-04-22, chg_1d=None
- `22:11:01`     UNRATE (Unemployment): value=None, date=2026-03-01, chg_1d=None
- `22:11:01`     CPIAUCSL (CPI): value=None, date=2026-03-01, chg_1d=None
- `22:11:01`     VIXCLS (VIX): value=None, date=2026-04-22, chg_1d=None
- `22:11:01`     DTWEXBGS (Dollar index): value=None, date=2026-04-17, chg_1d=None
## 2. Stocks — expected 187 tickers

- `22:11:01`   Stocks in report: 187
- `22:11:01`     ✓ SPY: price=708.45, change_1d=None
- `22:11:01`     ✓ QQQ: price=651.42, change_1d=None
- `22:11:01`     ✓ NVDA: price=199.64, change_1d=None
- `22:11:01`     ✓ TSLA: price=373.72, change_1d=None
- `22:11:01` ⚠     ✗ BRK.B: MISSING
- `22:11:01`     ✓ GLD: price=431.04, change_1d=None
- `22:11:01`     ✓ TLT: price=86.55, change_1d=None
- `22:11:01`   Stocks with null/zero price: 0
## 3. Crypto — expected 25 coins

- `22:11:01`   Coins in report: 25
- `22:11:01`     ✓ BTC: price=78093, 7d=4.200000110983346%, from_ATH=-38.06102%
- `22:11:01`     ✓ ETH: price=2329.54, 7d=-0.49502747946337305%, from_ATH=-52.90094%
- `22:11:01`     ✓ SOL: price=85.99, 7d=-3.3143625669338235%, from_ATH=-70.68418%
- `22:11:01`     ✓ XRP: price=1.44, 7d=-0.7103628118549284%, from_ATH=-60.61781%
## 4. CFTC — expected 29 contracts, 7 categories

- `22:11:01`   CFTC keys: ['crisis_level', 'crisis_score', 'extreme_count', 'positioning_score', 'reversal_count', 'risk_appetite', 'sector_positioning', 'smart_money', 'summary']
- `22:11:01`   Contract entries detected: 0
- `22:11:01`     ✓ crisis_score: 20
## 5. ECB CISS — systemic risk indicator

- `22:11:01`   ECB CISS entries: 6
- `22:11:01`     CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX: {'current': 0.011752774843560528, 'date': '2026-04-21', 'prev': 0.014624817641850568, 'change': -0.0029, 'pct_change': -
- `22:11:01`     CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX: {'current': 0.007342313592550885, 'date': '2026-04-21', 'prev': 0.006687448359983052, 'change': 0.0007, 'pct_change': 9.
- `22:11:01`     CISS.D.GB.Z0Z.4F.EC.SS_CIN.IDX: {'current': 0.05285662885427027, 'date': '2026-04-21', 'prev': 0.05391567306559519, 'change': -0.0011, 'pct_change': -1.
## 6. Options Flow — flow-data.json

- `22:11:01`   flow-data.json: 31,347 bytes, 3.1 min old
- `22:11:01`     put_call_ratio: None
- `22:11:01`     pc_signal: None
- `22:11:01`     gamma_regime: None
- `22:11:01`     net_premium: None
- `22:11:01`     spy_price: None
- `22:11:01`     sentiment_composite: None
- `22:11:01`     trading_signals: None
## 7. Crypto Intel — crypto-intel.json

- `22:11:01`   crypto-intel.json: 56,393 bytes, 1.3 min old
- `22:11:01`     btc_dominance: None
- `22:11:01`     eth_dominance: None
- `22:11:01`     total_mcap_fmt: None
- `22:11:01`     fear_greed_value: None
- `22:11:01`     risk_score: {'score': 50, 'regime': 'ELEVATED', 'action': 'CAUTION', 'signals': []}
- `22:11:01`     funding_summary: None
- `22:11:01`     mvrv_approx: None
- `22:11:01`     stablecoin_net_signal: None
- `22:11:01`     whale_count_24h: None
## 8. Stock Screener — screener/data.json

- `22:11:01`   screener/data.json: 326,495 bytes, 160.4 min old
- `22:11:01`     generated_at: 2026-04-23T19:30:37.395329+00:00
- `22:11:01`     generated_at_unix: 1776972637
- `22:11:01`     elapsed_seconds: 73.6
- `22:11:01`     count: 503
- `22:11:01`     stocks: list[503]
## 9. News — NewsAPI + RSS

- `22:11:01`   News list: 40 headlines
- `22:11:01`   Sample: Top Fed official sees potential rate hike amid higher gas prices, inflation conc
## 10. AI Briefings — Anthropic-generated

- `22:11:01`     generated_at: 2026-04-23T22:10:20.429043Z
- `22:11:01`     sections: {'macro': {'title': 'Macro Economy', 'outlook': 'SLOWDOWN', 'signals': ['Normalizing labor market at 4.3% unemployment.'
- `22:11:01`     portfolio: {'gold': {'action': 'UNDERWEIGHT', 'reasons': ['Strong USD headwind for gold.'], 'vehicles': ['GLD', 'IAU', 'SGOL', 'Phy
- `22:11:01`     best_plays: {'generated_at': '2026-04-23T22:10:20.429297Z', 'top_stocks': [{'ticker': 'EOG', 'name': 'EOG Resources', 'price': 133.8
## 11. DynamoDB — justhodl-signals table

- `22:11:01` ⚠   An error occurred (AccessDeniedException) when calling the DescribeTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DescribeTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/justhodl-signals because no identity-based policy allows the dynamodb:DescribeTable action
- `22:11:01` ⚠   justhodl-outcomes: An error occurred (AccessDeniedException) when calling the DescribeTable operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:DescribeTable on resource: arn:aws:dynamodb:us-east-1:857687956942:table/justhodl-outcomes because no identity-based policy allows the dynamodb:DescribeTable action
## 12. SSM — Calibration params

- `22:11:01`   ✓ /justhodl/calibration/weights
- `22:11:01`     Last modified: 2026-04-19 09:00:47.859000+00:00
- `22:11:01`     Value: {"crypto_risk_score": 0.3098, "crypto_fear_greed": 0.3098, "khalid_index": 1.0, "cftc_gold": 0.8, "cftc_spx": 0.8, "cftc_bitcoin": 0.75, "cftc_crude":
- `22:11:01`   ✓ /justhodl/calibration/accuracy
- `22:11:01`     Last modified: 2026-04-19 09:00:47.911000+00:00
- `22:11:01`     Value: {"crypto_risk_score": {"accuracy": 0.0, "n": 369, "avg_return": null}, "crypto_fear_greed": {"accuracy": 0.0, "n": 369, "avg_return": null}}
## 13. DEX Scanner — dex.html pushes to GitHub

- `22:11:02`   dex-scanner last 6h: 24 invocations, 0 errors
## 14. Anthropic API — are our AI Lambdas actually invoking Claude?

- `22:11:03`   justhodl-ai-chat: 0 Claude model refs, 0 API errors in last 6h
- `22:11:03`   justhodl-investor-agents: 0 Claude model refs, 0 API errors in last 6h
- `22:11:03`   justhodl-morning-intelligence: 0 Claude model refs, 0 API errors in last 6h
- `22:11:04`   justhodl-daily-report-v3: 0 Claude model refs, 0 API errors in last 6h
- `22:11:04` Done
