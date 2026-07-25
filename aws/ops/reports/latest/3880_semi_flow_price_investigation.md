# ops 3880 — INVESTIGATE: semi flow/price divergence + rebalance-timing claim

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-07-25T19:14:41+00:00  

## Data

| core_failures | semi_stocks_found | semi_stocks_missing_from_universe |
|---|---|---|
|  | ['NVDA', 'AMD', 'AVGO', 'TSM', 'MU', 'QCOM', 'TXN', 'INTC', 'LRCX', 'KLAC', 'AMAT', 'ARM', 'ASML', 'MRVL', 'ON', 'SMCI'] | [] |
| [] |  |  |

## Log
## 1. rebalance-radar — the engine already built for this exact pattern

- `19:14:40` ✅   data/rebalance-radar.json: 21.2h old, generated 2026-07-24T22:05:10+00:00
- `19:14:40`   top-level keys: ['calendar', 'engine', 'event_study', 'generated_at', 'method', 'qtd_proxies', 'rotation_risk', 'version', 'window_forensics']
- `19:14:40`   window/context: {}
- `19:14:40`   forensics/classification: null
- `19:14:40`   rotation_risk: {"flag": false, "severity": "NONE", "evidence": ["Crypto leg 5d: $+0.75B (accelerating)"], "read": "No leadership-rotation signature in the current window.", "historical_context": "Event study (45 quarters): SPY mean T+1..T+3 after quarter-end = +0.34% cum, i.e. month-end pressure typically REVERSES early in the new quarter."}
- `19:14:40`   FULL RAW DOC (first 3000 chars): {"engine": "justhodl-rebalance-radar", "version": "1.0.0", "generated_at": "2026-07-24T22:05:10+00:00", "calendar": {"today": "2026-07-24", "quarter_end": "2026-09-30", "prev_quarter_end": "2026-06-30", "bdays_to_qtr_end": 48, "bdays_since_prev_qtr_end": 18, "in_rebalance_window": false, "window_anchor": "2026-09-30", "window_def": "T-5..T+3 business days around quarter-end"}, "event_study": {"computed_at": "2026-07-02T02:59:24+00:00", "assets": {"SPY": {"label": "S&P 500", "table": [{"offset": -5, "mean_pct": 0.08, "median_pct": 0.02, "hit_up": 51.0, "n": 45}, {"offset": -4, "mean_pct": -0.1, "median_pct": -0.026, "hit_up": 49.0, "n": 45}, {"offset": -3, "mean_pct": 0.32, "median_pct": 0.056, "hit_up": 53.0, "n": 45}, {"offset": -2, "mean_pct": -0.154, "median_pct": -0.019, "hit_up": 49.0, "n": 45}, {"offset": -1, "mean_pct": 0.057, "median_pct": 0.059, "hit_up": 56.0, "n": 45}, {"offset": 0, "mean_pct": 0.194, "median_pct": 0.209, "hit_up": 60.0, "n": 45}, {"offset": 1, "mean_pct": 0.115, "median_pct": 0.261, "hit_up": 67.0, "n": 45}, {"offset": 2, "mean_pct": 0.156, "median_pct": 0.169, "hit_up": 61.0, "n": 44}, {"offset": 3, "mean_pct": 0.074, "median_pct": 0.157, "hit_up": 57.0, "n": 44}, {"offset": 4, "mean_pct": 0.126, "median_pct": 0.265, "hit_up": 59.0, "n": 44}, {"offset": 5, "mean_pct": 0.068, "median_pct": 0.103, "hit_up": 52.0, "n": 44}]}, "QQQ": {"label": "Nasdaq", "table": [{"offset": -5, "mean_pct": -0.056, "median_pct": 0.014, "hit_up": 53.0, "n": 45}, {"offset": -4, "mean_pct": -0.255, "median_pct": -0.083, "hit_up": 47.0, "n": 45}, {"offset": -3, "mean_pct": 0.326, "median_pct": 0.093, "hit_up": 53.0, "n": 45}, {"offset": -2, "mean_pct": -0.177, "median_pct": 0.071, "hit_up": 53.0, "n": 45}, {"offset": -1, "mean_pct": -0.056, "median_pct": -0.049, "hit_up": 47.0, "n": 45}, {"offset": 0, "mean_pct": 0.269, "median_pct": 0.129, "hit_up": 56.0, "n": 45}, {"offset": 1, "mean_pct": 0.11, "median_pct": 0.402, "hit_up": 62.0, "n": 45}, {"offset": 2, "mean_pct": 0.167, "median_pct": 0.379, "hit_up": 61.0, "n": 44}, {"offset": 3, "mean_pct": 0.123, "median_pct": 0.355, "hit_up": 59.0, "n": 44}, {"offset": 4, "mean_pct": 0.186, "median_pct": 0.211, "hit_up": 59.0, "n": 44}, {"offset": 5, "mean_pct": 0.131, "median_pct": 0.238, "hit_up": 61.0, "n": 44}]}, "SMH": {"label": "Semiconductors/AI", "table": [{"offset": -5, "mean_pct": 0.021, "median_pct": -0.146, "hit_up": 49.0, "n": 45}, {"offset": -4, "mean_pct": -0.244, "median_pct": -0.041, "hit_up": 47.0, "n": 45}, {"offset": -3, "mean_pct": 0.412, "median_pct": 0.244, "hit_up": 56.0, "n": 45}, {"offset": -2, "mean_pct": -0.31, "median_pct": -0.329, "hit_up": 44.0, "n": 45}, {"offset": -1, "mean_pct": -0.092, "median_pct": 0.028, "hit_up": 51.0, "n": 45}, {"offset": 0, "mean_pct": 0.412, "median_pct": 0.142, "hit_up": 58.0, "n": 45}, {"offset": 1, "mean_pct": 0.053, "median_pct": 0.168, "hit_up": 58.0, "n": 45}, {"offset": 2, "mean_pct": 0.108, "median_pct": 0.341, "hit_up": 57.0, "n": 44}
## 2. event-study cache — measured T-5..T+5 pattern for SMH around quarter-end

- `19:14:40` ✅   rebalance-eventstudy.json: 568.3h old
- `19:14:40`   SMH event-study entry: null
- `19:14:40`   top-level keys: ['assets', 'computed_at', 'n_quarters']
## 3. catalyst-calendar — real scheduled events, this week specifically

- `19:14:40` ✅   catalyst-calendar.json: 1.4h old
- `19:14:40`   top-level keys: ['as_of', 'by_source', 'by_type', 'duration_s', 'events', 'high_impact_next_30d', 'high_impact_next_7d', 'method', 'n_events', 'schema_version', 'window_days']
- `19:14:40`   FULL RAW DOC (first 2500 chars): {"schema_version": "1.0", "method": "catalyst_calendar_v1", "as_of": "2026-07-25T17:51:42+00:00", "window_days": 60, "n_events": 579, "events": [{"date": "2026-07-27", "time": null, "type": "AUCTION", "title": "2-Year Note auction", "subtitle": "Size TBD", "impact": "MEDIUM", "source": "TreasuryDirect", "url": "https://www.treasurydirect.gov/auctions/upcoming/", "size_billions": 0.0, "cusip": "91282CRB9", "days_to": 2}, {"date": "2026-07-27", "time": null, "type": "AUCTION", "title": "5-Year Note auction", "subtitle": "Size TBD", "impact": "MEDIUM", "source": "TreasuryDirect", "url": "https://www.treasurydirect.gov/auctions/upcoming/", "size_billions": 0.0, "cusip": "91282CRA1", "days_to": 2}, {"date": "2026-07-27", "time": null, "type": "AUCTION", "title": "13-Week Bill auction", "subtitle": "Size TBD", "impact": "LOW", "source": "TreasuryDirect", "url": "https://www.treasurydirect.gov/auctions/upcoming/", "size_billions": 0.0, "cusip": "912797SK4", "days_to": 2}, {"date": "2026-07-27", "time": null, "type": "AUCTION", "title": "26-Week Bill auction", "subtitle": "Size TBD", "impact": "LOW", "source": "TreasuryDirect", "url": "https://www.treasurydirect.gov/auctions/upcoming/", "size_billions": 0.0, "cusip": "912797VU8", "days_to": 2}, {"date": "2026-07-28", "time": null, "type": "AUCTION", "title": "7-Year Note auction", "subtitle": "Size TBD", "impact": "MEDIUM", "source": "TreasuryDirect", "url": "https://www.treasurydirect.gov/auctions/upcoming/", "size_billions": 0.0, "cusip": "91282CRC7", "days_to": 3}, {"date": "2026-07-28", "time": null, "type": "AUCTION", "title": "6-Week Bill auction", "subtitle": "Size TBD", "impact": "LOW", "source": "TreasuryDirect", "url": "https://www.treasurydirect.gov/auctions/upcoming/", "size_billions": 0.0, "cusip": "912797UF2", "days_to": 3}, {"date": "2026-07-28", "time": "AMC", "type": "EARNINGS", "title": "V earnings", "subtitle": "Visa Inc.", "impact": "LOW", "source": "FMP", "url": null, "size_billions": null, "ticker": "V", "consensus": 3.23, "n_estimates": "11", "market_cap": 0.0, "days_to": 3}, {"date": "2026-07-28", "time": "BMO", "type": "EARNINGS", "title": "KO earnings", "subtitle": "Coca-Cola Company (The)", "impact": "LOW", "source": "FMP", "url": null, "size_billions": null, "ticker": "KO", "consensus": 0.92, "n_estimates": "7", "market_cap": 0.0, "days_to": 3}, {"date": "2026-07-28", "time": "BMO", "type": "EARNINGS", "title": "BA earnings", "subtitle": "Boeing Company (The)", "impact": "LOW", "source"
## 4. LIVE semi-complex ETF data — daily.json (built this arc, fully trusted)

- `19:14:40` ✅   daily.json 21.2h old, 300 ETFs
- `19:14:40`   SMH    daily=$  -528.2M 5d=$  -1071.5M 21d=$  +2286.9M z90d=-0.46 ret5d=2.05% ret21d=-6.61% quadrant=NEUTRAL persistence_days=3
- `19:14:40`   SOXX   daily=$  -638.8M 5d=$  -2618.8M 21d=$  +5715.0M z90d=-0.81 ret5d=3.94% ret21d=-8.67% quadrant=NEUTRAL persistence_days=4
- `19:14:40`   XLK    daily=$  +126.2M 5d=$   +278.1M 21d=$   +550.6M z90d=0.06 ret5d=0.56% ret21d=-3.12% quadrant=NEUTRAL persistence_days=2
- `19:14:40`   SOXL   daily=$    +0.0M 5d=$  +1311.7M 21d=$  +3687.4M z90d=0.17 ret5d=10.82% ret21d=-44.08% quadrant=NEUTRAL persistence_days=0
- `19:14:40`   SOXS   daily=$    -4.5M 5d=$    +70.8M 21d=$  -1902.2M z90d=-0.14 ret5d=-12.56% ret21d=1177.72% quadrant=NEUTRAL persistence_days=1
## 5. LIVE single-name semi data — constituent-pressure.json (built this arc)

- `19:14:40` ✅   constituent-pressure.json 1.4h old, 2247 stocks
- `19:14:40`   NVDA   sector=Technology daily=$   +40.1M 5d=$  -1310.5M 21d=$  -1784.3M perf_w=1.99% perf_m=3.94% perf_ytd=10.91% z_xsec=-0.26 quadrant=NEUTRAL
- `19:14:40`   AMD    sector=Technology daily=$    +0.5M 5d=$   -567.0M 21d=$    +19.5M perf_w=5.28% perf_m=0.43% perf_ytd=143.72% z_xsec=0.13 quadrant=NEUTRAL
- `19:14:40`   AVGO   sector=Technology daily=$    +0.9M 5d=$   -553.0M 21d=$   -155.8M perf_w=2.99% perf_m=-0.04% perf_ytd=10.35% z_xsec=0.02 quadrant=NEUTRAL
- `19:14:40`   TSM    sector=Technology daily=$   -76.3M 5d=$   -213.7M 21d=$   +457.8M perf_w=1.27% perf_m=-8.49% perf_ytd=32.75% z_xsec=0.33 quadrant=NEUTRAL
- `19:14:40`   MU     sector=Technology daily=$   +35.3M 5d=$   -674.5M 21d=$  +4730.6M perf_w=8.48% perf_m=-12.17% perf_ytd=222.68% z_xsec=4.78 quadrant=STEALTH_ACCUMULATION
- `19:14:40`   QCOM   sector=Technology daily=$   -18.9M 5d=$   -168.6M 21d=$   +131.7M perf_w=-2.8% perf_m=-15.42% perf_ytd=-2.39% z_xsec=0.88 quadrant=NEUTRAL
- `19:14:40`   TXN    sector=Technology daily=$   -19.6M 5d=$   -236.4M 21d=$   +144.5M perf_w=-1.56% perf_m=-7.76% perf_ytd=61.15% z_xsec=0.69 quadrant=NEUTRAL
- `19:14:40`   INTC   sector=Technology daily=$    -0.4M 5d=$   -330.4M 21d=$    +31.6M perf_w=-2.86% perf_m=-29.87% perf_ytd=150.19% z_xsec=0.18 quadrant=NEUTRAL
- `19:14:40`   LRCX   sector=Technology daily=$    -3.3M 5d=$   -275.7M 21d=$    +42.6M perf_w=-2.58% perf_m=-18.57% perf_ytd=78.3% z_xsec=0.22 quadrant=NEUTRAL
- `19:14:40`   KLAC   sector=Technology daily=$   -20.7M 5d=$   -256.7M 21d=$   +162.7M perf_w=-1.05% perf_m=-12.46% perf_ytd=73.26% z_xsec=0.71 quadrant=NEUTRAL
- `19:14:40`   AMAT   sector=Technology daily=$    -8.3M 5d=$   -325.8M 21d=$    +88.3M perf_w=1.24% perf_m=-8.95% perf_ytd=108.67% z_xsec=0.32 quadrant=NEUTRAL
- `19:14:40`   ARM    sector=Technology daily=$    +0.9M 5d=$    -64.4M 21d=$    +16.5M perf_w=-2.69% perf_m=-27.59% perf_ytd=137.86% z_xsec=0.17 quadrant=NEUTRAL
- `19:14:40`   ASML   sector=Technology daily=$   -27.4M 5d=$   -166.7M 21d=$   +187.0M perf_w=0.54% perf_m=-0.32% perf_ytd=64.24% z_xsec=0.39 quadrant=NEUTRAL
- `19:14:40`   MRVL   sector=Technology daily=$   -25.4M 5d=$   -190.4M 21d=$   +149.7M perf_w=2.94% perf_m=-29.8% perf_ytd=128.56% z_xsec=1.01 quadrant=STEALTH_ACCUMULATION
- `19:14:40`   ON     sector=Technology daily=$   -14.7M 5d=$    -54.5M 21d=$   +118.9M perf_w=-0.64% perf_m=-25.0% perf_ytd=60.31% z_xsec=3.72 quadrant=STEALTH_ACCUMULATION
- `19:14:40`   SMCI   sector=Technology daily=$    -0.3M 5d=$     -0.4M 21d=$     +1.6M perf_w=24.48% perf_m=-7.24% perf_ytd=2.84% z_xsec=0.19 quadrant=NEUTRAL
## 6. sector-flow-state / sector rotation read on Technology this week

- `19:14:40` ✅   sector-flow-state.json 0.9h old
- `19:14:40`   Technology sector entry: {"symbol": "XLK", "name": "Technology", "conviction": 38.9, "posture": "UNDERWEIGHT", "quadrant": "Weakening", "confluence": 2, "drivers": ["RS accelerating", "cycle-favored"], "rotation_score": 44.9, "rs_rank_1y": 81.7, "rs_slope": -0.2563, "flow_confirm": "NEUTRAL", "in_cycle": true, "dollar_flow_usd": 15384389912, "dollar_confirms": null}
## 7. rotation-dashboard — does the cross-asset rotation view show a leadership handoff

- `19:14:40` ✅   rotation-dashboard.json 15.2h old
- `19:14:40`   top-level keys: ['assets', 'avoid', 'build_seconds', 'caveats', 'degraded', 'engine', 'excluded', 'generated_at', 'layer1_regime', 'layer2_ratios', 'layer3_layer4', 'methodology', 'overweight', 'quadrant_counts', 'thesis', 'version']
- `19:14:40`   L1 nowcast: null
## 8. can THIS environment (GitHub Actions runner) reach general internet for an independent price check — Claude's own sandbox cannot

- `19:14:41` ✅   reached stooq.com — 3 lines of SMH daily history, last 5 rows:
<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="robots" content="noindex,nofollow"></head><body><noscript>This site requires JavaScript to verify your browser. Please enable JavaScript and reload.</noscript><script nonce="8ovj5BWvwsYh6aKkzpaowg">
(async()=>{const c="AAAAAGplC1JwhHPE7q435iV19Kn7vVFTFFGel-sYcCJs7LB-YfqfZcj7iX4",d=4,t="0".repeat(d),e=new TextEncoder;let n=0;while(1){const h=await crypto.subtle.digest("SHA-256",e.encode(c+n)),x=Array.from(new Uint8Array(h)).map(b=>b.toString(16).padStart(2,"0")).join("");if(x.startsWith(t))break;n++}const r=await fetch("/__verify",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"c="+encodeURIComponent(c)+"&n="+n,credentials:"same-origin"});if(r.ok)location.reload()})();
</script></body></html>
## 9. verdict prep — deterministic rebalance-calendar facts (no data needed)

- `19:14:41`   Russell 2026 reconstitution: 2026-06-26 (29 days before 2026-07-25)
- `19:14:41`   S&P/Nasdaq quarterly rebalance: 2026-06-19 (36 days before 2026-07-25)
- `19:14:41`   Q2 2026 quarter-end: 2026-06-30 (25 days before 2026-07-25)
- `19:14:41`   Khalid's stated timing 'a week or two ago' = 2026-07-11 to 2026-07-18 (7-14 days before 2026-07-25) — does NOT match any standard mechanical date
## 10. verdict

- `19:14:41` ✅ PROBE COMPLETE — core data readable, findings above, no conclusions asserted beyond what's printed
