# Phase 9 end-to-end health audit

**Status:** success  
**Duration:** 6.9s  
**Finished:** 2026-04-26T20:37:20+00:00  

## Log
## 1. Producer Lambdas alive

- `20:37:13`   ✅ justhodl-crisis-plumbing  runtime=python3.12  mem=512MB  timeout=120s  modified=2026-04-26T15:03:07.586+0000
- `20:37:13`   ✅ justhodl-regime-anomaly  runtime=python3.12  mem=1024MB  timeout=240s  modified=2026-04-26T15:22:15.000+0000
## 2. EventBridge schedules

- `20:37:14`   ✅ justhodl-crisis-plumbing-refresh  rate(6 hours)  state=ENABLED  targets=['justhodl-crisis-plumbing']
- `20:37:14`   ✅ justhodl-regime-anomaly-refresh  rate(1 day)  state=ENABLED  targets=['justhodl-regime-anomaly']
## 3. S3 outputs fresh

- `20:37:14`   ✅ data/crisis-plumbing.json  age=5.6h (max 6.5h)  size=4.3KB
- `20:37:14`   ✅ data/regime-anomaly.json  age=5.2h (max 25h)  size=0.6KB
## 4. JSON content sanity

- `20:37:14`   crisis-plumbing.json: 4/5 crisis indices populated, 0/6 plumbing series populated
- `20:37:14`     composite: score=37.0 signal=NORMAL flagged=?
- `20:37:15`   regime-anomaly.json: ka_index_n_obs=0  warming_up=True  state=None
- `20:37:15`     n_anomalies=?  composite_score=?
## 5. HTML pages serve from justhodl.ai

- `20:37:19`   ✅ https://justhodl.ai/crisis.html  HTTP 200  bytes=21115
- `20:37:19`   ✅ https://justhodl.ai/regime.html  HTTP 200  bytes=20074
## 6. DOM markers

- `20:37:19`   ✅ crisis.html contains 'Crisis & Plumbing'
- `20:37:19`   ✅ crisis.html contains 'crisis-plumbing.json'
- `20:37:19`   ✅ crisis.html contains 'composite'
- `20:37:19`   ✅ crisis.html contains 'crisis_indices'
- `20:37:19`   ✅ crisis.html contains 'yield_curve'
- `20:37:19`   ✅ regime.html contains 'Regime'
- `20:37:19`   ✅ regime.html contains 'regime-anomaly.json'
- `20:37:19`   ✅ regime.html contains 'transition'
- `20:37:19`   ✅ regime.html contains 'anomal'
## 7. Nav wiring (sidebar + index launcher)

- `20:37:19`   ✅ index.html links to /crisis.html
- `20:37:19`   ✅ index.html links to /regime.html
- `20:37:19` ⚠   ✗ https://justhodl.ai/_partials/sidebar.html: HTTP 404
## FINAL VERDICT

- `20:37:20`   ✅  Lambdas alive
- `20:37:20`   ✅  Schedules wired
- `20:37:20`   ✅  S3 fresh
- `20:37:20`   ✅  Content sane
- `20:37:20`   ✅  Pages serve
- `20:37:20`   ✅  DOM markers present
- `20:37:20`   ✗  Nav wired
- `20:37:20` 
- `20:37:20`   🟡 SOME GAPS — see log above
- `20:37:20` Done
