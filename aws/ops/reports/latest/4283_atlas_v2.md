# ops 4283 -- Atlas v2: nested stores + dormant triage + switch-on wave 1

**Status:** success  
**Duration:** 350.1s  
**Finished:** 2026-08-02T18:56:06+00:00  

## Data

| age_h | artifact | fam | top |
|---|---|---|---|
| 5.5 | _alerts/uptime-status.json | Alerts & R | {"name": "\ud83c\udfaf Equity Front-Run Sniffer", "status": "FRESH"} |
| 624.7 | _alerts/frontrun-sniffer-alert-state.jso | Alerts & R | {"row": "-", "signal": "GOOG CPR surging 1329%"} |
| 0.3 | _state/velocity-acceleration-pending.jso | Nested Sig | {"name": "DELL", "status": "confirmed"} |
| 3.8 | _research/state.json | Nested Sig | {"name": "Canada Goose Holdings Inc.", "conviction": "3"} |
| 5.5 | _skill/calibration-config.json | Nested Sig | {"row": "-", "status": "AUTO_APPLIED"} |
| 36.8 | ecb-hist/_manifest.json | Nested Sig | {"name": "Eurosystem Total Assets (\u20ac", "percentile": 28.3} |
| 49.0 | interpretations/_summary.json | Nested Sig | {"name": "yield-curve", "status": "err"} |
| 812.5 | _preview/master-ranker.json | Nested Sig | {"name": "STX", "score": 131.8} |

## Log
## 1. nested stores

- `18:50:17` subdirs: 61 total, 46 signal-candidates: ['_alerts/', '_altseason/', '_askdesk/', '_audit/', '_backtest/', '_canaries/', '_cycle/', '_estrev/', '_harvest/', '_internals/', '_intraday/', '_ledger/', '_ma200/', '_map/', '_preview/', '_research/', '_skill/', '_state/', '_streaming/', '_upside/']
- `18:51:10` ✅ nested scan: 420 docs -> 8 opportunity artifacts ({'Alerts & Routing': 2, 'Nested Signals': 6})
## 2. dormant triage (writer-code classification)

- `18:51:10` ✅ triage of 60 dormant targets: {'MAIN_PATH': 56, 'LAZY_EVENT': 4}
## 3. switch-on wave 1 (MAIN_PATH writers, cap 10)

- `18:51:12` invoked justhodl-convergence-radar
- `18:51:15` invoked justhodl-alert-sentinel
- `18:51:17` invoked justhodl-prepump-alerts-router
- `18:51:18` invoked justhodl-trade-ticket-monitor
- `18:51:56` invoked justhodl-altseason
- `18:53:47` invoke justhodl-backtest-harness: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/
- `18:53:48` invoked justhodl-theme-classifier
- `18:54:08` invoked justhodl-crisis-canaries
- `18:54:11` invoked justhodl-accumulation-radar
- `18:56:01` invoked justhodl-signal-harvester
- `18:56:05` ✅ LIT: data/_alerts/convergence-radar-alerted.json (by justhodl-convergence-radar)
- `18:56:05` ✅ LIT: data/_alerts/last.json (by justhodl-alert-sentinel)
- `18:56:05` ✅ LIT: data/_alerts/prepump-router-state.json (by justhodl-prepump-alerts-router)
- `18:56:05` ✅ LIT: data/_alerts/trade-monitor-state.json (by justhodl-trade-ticket-monitor)
- `18:56:05` ✅ LIT: data/_altseason/global-history.json (by justhodl-altseason)
- `18:56:05` ✅ LIT: data/_backtest/state.json (by justhodl-backtest-harness)
- `18:56:05` ✅ LIT: data/_cache/ticker-profiles.json (by justhodl-theme-classifier)
- `18:56:06` ✅ LIT: data/_canaries/history.json (by justhodl-crisis-canaries)
- `18:56:06` ✅ LIT: data/_cycle/pv.json (by justhodl-accumulation-radar)
- `18:56:06` ✅ LIT: data/_harvest/last-run.json (by justhodl-signal-harvester)
- `18:56:06` wave 1: 10 invoked, 10 targets materialized
- `18:56:06` ✅ ATLAS v2 published: 194 engines / 14 families; dormant split lazy=4 true=56; wave1 lit=10
## RESULT

- `18:56:06` ✅ OPS 4283 PASS
