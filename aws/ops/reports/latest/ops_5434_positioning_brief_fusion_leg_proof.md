# ops 5434 -- positioning_brief fusion leg: receipts + one bridge run

**Status:** success  
**Duration:** 18.2s  
**Finished:** 2026-09-12T03:59:08+00:00  

## Data

| brief_asof | brief_n | brief_status | coverage | fusion | leg_present | n_signals | run_id | shadow | step |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-12T03:54:05Z | 1 | OK |  |  |  | 1994 | 20260912T035851Z-42db4bd0 |  | bridge |
|  |  |  | 0.7692 | 0.2058 | True |  | 20260912T035857Z-419f8bac | True | fusion |

## Log
- `03:58:50` ✅ justhodl-jhsignal-bridge receipt commit=868331f run=34671666214 sha_match=True live=bP86WtWEyJbb
- `03:58:50` ✅ justhodl-jhsignal-bridge zip: registry engines=20 positioning_brief=True jh_brief_adapters.py=True
- `03:58:51` ✅ justhodl-jh-fusion receipt commit=868331f run=34671666214 sha_match=True live=2JcomMpPdCaW
- `03:58:51` ✅ justhodl-jh-fusion zip: registry engines=20 positioning_brief=True jh_brief_adapters.py=True
- `03:58:57` ✅ bridge run 20260912T035851Z-42db4bd0: n_signals=1994 n_entities=1597 positioning_brief={"engine_id": "positioning_brief", "family": "FLOW", "criticality": "NONCRITICAL", "n_signals": 1, "n_rejected": 0, "n_skipped": 0, "skip_reasons": {}, "source_status": "OK", "data_asof": "2026-09-12T03:54:05Z", "asof_basis": "engine", "diagnostics": []}
- `03:58:57` ✅ state store: positioning_brief market rows=1 {"signal_type": "positioning_flow", "score": 0.27186, "confidence": 1.0, "horizon": "INTERMEDIATE", "freshness": "FRESH", "data_asof": "2026-09-12T03:54:05Z"}
- `03:59:08` ✅ fusion run 20260912T035857Z-419f8bac at 2026-09-12T03:58:57Z: market leg present=True shadow=True best=INTERMEDIATE fusion=0.2058 coverage=0.7692 confidence=0.8134
- `03:59:08` ✅ GREEN -- positioning_brief is a live FLOW leg in shadow: receipts match 868331f, bridge OK/1 signal, state FRESH, fusion market entity carries it
