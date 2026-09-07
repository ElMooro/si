# ops 5214 -- fusion R1 fix-forward (GBC, etf-flows, timestamps, TTLs) + fan-out audit

**Status:** success  
**Duration:** 44.1s  
**Finished:** 2026-09-07T17:03:42+00:00  

## Error

```
SystemExit: 0
```

## Data

| age_h | asof | basis | best_horizon | capital | confidence | contradiction | conviction | coverage | diag | engine | entity | family | fusion | independent | last_log | member | missing | n | raw | skip_reasons | skipped | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 2026-09-07T17:00:41Z | engine |  |  |  |  |  |  |  | regime_composite |  | MACRO |  |  |  |  |  | 1 |  | {} | 0 | OK |
|  | 2026-09-06T18:22:33Z | engine |  |  |  |  |  |  |  | liquidity_credit_engine |  | MACRO |  |  |  |  |  | 1 |  | {} | 0 | OK |
|  | 2026-09-07T12:01:05Z | engine |  |  |  |  |  |  |  | global_business_cycle |  | MACRO |  |  |  |  |  | 1 |  | {} | 0 | OK |
|  | 2026-09-07T16:41:43Z | engine |  |  |  |  |  |  |  | risk_gate |  | RISK |  |  |  |  |  | 1 |  | {} | 0 | OK |
|  | 2026-09-07T16:15:50Z | engine |  |  |  |  |  |  |  | crisis_composite |  | RISK |  |  |  |  |  | 1 |  | {} | 0 | OK |
|  | 2026-09-06T21:35:40Z | engine |  |  |  |  |  |  |  | tail_risk |  | RISK |  |  |  |  |  | 3 |  | {} | 0 | OK |
|  | 2026-09-07T14:40:29Z | engine |  |  |  |  |  |  |  | insider_radar |  | FLOW |  |  |  |  |  | 3 |  | {} | 0 | OK |
|  | 2026-09-07T15:07:01Z | engine |  |  |  |  |  |  |  | institutional_13f_flows |  | FLOW |  |  |  |  |  | 236 |  | {"wn is None or wn == 0": 7290} | 7290 | OK |
|  | 2026-09-07T11:35:42Z | engine |  |  |  |  |  |  |  | etf_flows |  | FLOW |  |  |  |  |  | 60 |  | {} | 0 | OK |
|  | 2026-09-04T21:41:08Z | engine |  |  |  |  |  |  |  | dark_pool |  | FLOW |  |  |  |  |  | 58 |  | {"not sym or sc is None or st not in ('ACCUMULATION', 'DISTRIBUTION')": 2} | 2 | OK |
|  | 2026-09-07T13:40:37Z | engine |  |  |  |  |  |  |  | estimate_revisions |  | FUNDAMENTAL |  |  |  |  |  | 84 |  | {"not sym or sym in seen": 6} | 6 | OK |
|  | 2026-09-07T16:25:19Z | engine |  |  |  |  |  |  |  | momentum_leaders |  | MARKET |  |  |  |  |  | 60 |  | {} | 0 | OK |
|  | 2026-09-05T03:32:02Z | engine |  |  |  |  |  |  |  | fortress |  | MARKET |  |  |  |  |  | 800 |  | {"not sym or comp is None or tier not in self.TIER": 150} | 150 | OK |
|  | 2026-09-07T16:18:26Z | engine |  |  |  |  |  |  |  | katlin |  | MARKET |  |  |  |  |  | 239 |  | {} | 0 | OK |
|  | 2026-09-07T02:29:32Z | engine |  |  |  |  |  |  |  | catalyst |  | CATALYST |  |  |  |  |  | 201 |  | {} | 0 | OK |
|  | 2026-09-07T16:07:22Z | engine |  |  |  |  |  |  |  | dealer_gex |  | CATALYST |  |  |  |  |  | 10 |  | {} | 0 | OK |
|  | 2026-09-07T12:20:57Z | engine |  |  |  |  |  |  |  | short_interest |  | FLOW |  |  |  |  |  | 64 |  | {"s is None": 5721} | 5721 | OK |
|  |  |  | INTERMEDIATE | OPEN | 0.59 | 0 | 22 | 0.3077 |  |  | market:US_EQUITY |  | 0.2244 | 1 |  |  | CATALYST,FLOW,MARKET,RISK |  | 2 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.6124 | 10 | 56 | 0.4211 |  |  | etf:SPY |  | 0.5614 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.4609 | 52 | 26 | 0.4211 |  |  | etf:QQQ |  | -0.2611 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.4473 | 56 | 22 | 0.4211 |  |  | etf:IWM |  | -0.2193 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.4477 | 66 | 17 | 0.5217 |  |  | equity:NVDA |  | -0.1654 | 3 |  |  | CATALYST,MARKET,RISK |  | 4 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.6621 | 0 | 56 | 0.3478 |  |  | equity:TSM |  | 0.5614 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.6621 | 0 | 56 | 0.3478 |  |  | equity:ASML |  | 0.5614 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.4486 | 52 | 26 | 0.3478 |  |  | equity:AAPL |  | -0.2611 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | SWING | OPEN | 0.7824 | 0 | 52 | 0.6522 |  |  | equity:MSFT |  | 0.5205 | 3 |  |  | FUNDAMENTAL,MARKET |  | 4 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.6621 | 0 | 56 | 0.3478 |  |  | equity:AMZN |  | 0.5614 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET,RISK |  | 3 |  |  |  |
|  |  |  | SWING | OPEN | 0.4503 | 70 | 14 | 0.6522 |  |  | equity:GOOGL |  | 0.1418 | 3 |  |  | FUNDAMENTAL,MARKET |  | 4 |  |  |  |
|  |  |  | SWING | OPEN | 0.7075 | 0 | 74 | 0.5217 |  |  | equity:META |  | 0.7405 | 2 |  |  | CATALYST,FUNDAMENTAL,MARKET |  | 3 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.5686 | 0 | 22 | 0.2222 |  |  | crypto:BTC |  | 0.2244 | 1 |  |  | CATALYST,FLOW,MARKET,RISK |  | 2 |  |  |  |
|  |  |  | INTERMEDIATE | OPEN | 0.5686 | 0 | 22 | 0.2222 |  |  | crypto:ETH |  | 0.2244 | 1 |  |  | CATALYST,FLOW,MARKET,RISK |  | 2 |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:40 | justhodl-bloomberg-v8 |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:41 | justhodl-data-collector |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:41 | justhodl-health-monitor |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:42 | justhodl-market-interpreter |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:41 | justhodl-nobrainer-tracker |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:44 | justhodl-regime-composite |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:41 | justhodl-reports-builder |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:42 | justhodl-synthetic-monitor |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:41 | justhodl-vol-regime |  |  |  |  |  |  |
| 0.0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-09-07T17:00:47 | justhodl-whats-changed |  |  |  |  |  |  |

## Log
## functions

- `17:02:58`   Lambda exists — updating
- `17:03:01` ✅   ✓ updated justhodl-jhsignal-bridge
- `17:03:07`    justhodl-jhsignal-bridge Active / Successful
- `17:03:07`   Lambda exists — updating
- `17:03:13` ✅   ✓ updated justhodl-jh-fusion
- `17:03:18`    justhodl-jh-fusion Active / Successful
## bridge re-run

- `17:03:29` ✅    run 20260907T170319Z-2cdba968 (bridge v1.0.1): 1823 signals / 1483 entities in 7.84s; freshness {'FRESH': 1823, 'STALE': 0, 'EXPIRED': 0, 'INVALID': 0}; changes {'new': 61, 'revised': 0, 'expired': 0}; bus {'sent': 62, 'failed': 0, 'suppressed': 0, 'per_signal_events': 61}; state {'enabled': True, 'table': 'ACTIVE', 'written': 1823, 'expired_swept': 0}
- `17:03:29`    market:US_EQUITY engines: ['crisis_composite', 'global_business_cycle', 'liquidity_credit_engine', 'regime_composite', 'risk_gate']
- `17:03:29`    snapshot families: {'MACRO': 3, 'RISK': 5, 'CATALYST': 211, 'FLOW': 421, 'MARKET': 1099, 'FUNDAMENTAL': 84}; duplicates dropped: 0
- `17:03:29`    etf:SPY: ['dealer_gex', 'etf_flows', 'institutional_13f_flows', 'tail_risk']
- `17:03:29`    etf:QQQ: ['dealer_gex', 'etf_flows', 'institutional_13f_flows', 'tail_risk']
- `17:03:29`    equity:NVDA: ['catalyst', 'dealer_gex', 'estimate_revisions', 'institutional_13f_flows']
- `17:03:29`    equity:AAPL: ['dealer_gex', 'institutional_13f_flows', 'short_interest']
## fusion via coordinator route

- `17:03:40` ✅    fusion 20260907T170327Z-61cd5e46 via jhsignal.batch_published: entities 14, coverage mean 0.39, confidence mean 0.56, hard 0 / soft 3, regime MILDLY_SUPPORTIVE (0.27, 5 legs), route_verified=True
## fan-out '15min' tick audit (justhodl-regime-composite had not run for 23h)

- `17:03:40`    ticks: {'daily-morn': 28, 'daily-eve': 8, 'daily-06utc': 2, 'daily-16utc': 2, 'every_6h': 2, '4hourly': 6, '15min': 10, 'daily-17utc': 10, 'hourly': 10, 'daily-15utc': 1, 'daily-08utc': 4, '30min': 1, 'daily-07utc': 1}
- `17:03:40`    router justhodl-scheduler last log event: 2026-09-07T17:00:40
- `17:03:41`    router schedules: []
- `17:03:42`    15min members silent > 6h: 0 of 10 -> []
- `17:03:42` ✅ GREEN -- fix-forward verified on live data
