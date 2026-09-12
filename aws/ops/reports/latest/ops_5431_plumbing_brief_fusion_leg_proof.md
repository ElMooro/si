# ops 5431 -- plumbing_brief fusion leg: receipts + one bridge run

**Status:** success  
**Duration:** 27.2s  
**Finished:** 2026-09-12T03:37:13+00:00  

## Data

| brief_asof | brief_n | brief_status | coverage | fusion | leg_present | n_signals | run_id | shadow | step |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-12T02:31:59Z | 1 | OK |  |  |  | 1992 | 20260912T033647Z-61ad31ae |  | bridge |
|  |  |  | 0.6154 | 0.1834 | True |  | 20260912T033703Z-0c56cdac | True | fusion |

## Log
- `03:36:46` ✅ justhodl-jhsignal-bridge receipt commit=9494d42 run=34670592916 sha_match=True live=mo/k+0qB3BiA
- `03:36:46` ✅ justhodl-jhsignal-bridge zip: registry engines=19 plumbing_brief=True jh_brief_adapters.py=True
- `03:36:46` ✅ justhodl-jh-fusion receipt commit=9494d42 run=34670592916 sha_match=True live=j0onZ7GvQa8F
- `03:36:46` ✅ justhodl-jh-fusion zip: registry engines=19 plumbing_brief=True jh_brief_adapters.py=True
- `03:37:02` ✅ bridge run 20260912T033647Z-61ad31ae: n_signals=1992 n_entities=1596 plumbing_brief={"engine_id": "plumbing_brief", "family": "RISK", "criticality": "IMPORTANT", "n_signals": 1, "n_rejected": 0, "n_skipped": 0, "skip_reasons": {}, "source_status": "OK", "data_asof": "2026-09-12T02:31:59Z", "asof_basis": "engine", "diagnostics": []}
- `03:37:03` ✅ state store: plumbing_brief market rows=1 {"signal_type": "plumbing_stress", "score": 0.118, "confidence": 1.0, "horizon": "INTERMEDIATE", "freshness": "FRESH", "data_asof": "2026-09-12T02:31:59Z"}
- `03:37:13` ✅ fusion run 20260912T033703Z-0c56cdac at 2026-09-12T03:37:03Z: market leg present=True shadow=True best=INTERMEDIATE fusion=0.1834 coverage=0.6154 confidence=0.731
- `03:37:13` ✅ GREEN -- plumbing_brief is a live RISK leg in shadow: receipts match 9494d42, bridge OK/1 signal, state FRESH, fusion market entity carries it
