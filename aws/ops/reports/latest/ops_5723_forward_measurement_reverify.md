- `19:31:22` justhodl-signal-harvester exact commit and runtime hash bd566aba32b22422f5c421c5679738e0f9868c8f
- `19:31:22` justhodl-prospective-evaluator exact commit and runtime hash bd566aba32b22422f5c421c5679738e0f9868c8f
**Status:** success  
**Duration:** 87.5s  
**Finished:** 2026-09-18T19:32:48+00:00  

## Data

| batch | capture_coverage | checked_in_batch | commit | generated_at | legacy_ledger_writes | price_probe_as_of | price_probe_observations | price_probe_sha256 | registered_in_capture | schedule | sizing_authority | statuses |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| data/research-forecasts/evaluation-runs/5d9039cc77a3fec2df911788a8b5f0f727bfe137fd4313ef7d4c7d0b1c63265f.json | {'candidate_scan_complete': False, 'candidate_sources': 797, 'source_read_failures': [{'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/13f-positions.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/data-census.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/etf-holdings-complete.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/etf-holdings-index.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/euro-fragmentation.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/feed-catalog.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/fortress.json'}, {'reason': 'SOURCE_READ_UNAVAILABLE', 'source_key': 'data/industry-case.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/khalid-candidates.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/khalid.json'}, {'reason': 'SOURCE_EXCEEDS_CAPTURE_BOUND', 'source_key': 'data/quiver-lobbying-cache.json'}], 'sources_scanned': 797} | 100 | bd566aba32b22422f5c421c5679738e0f9868c8f | 2026-09-18T19:32:47.735224+00:00 | 0 | 2026-09-17 | 9 | 53c324fcfe9a31b43e07ecad1ccfd1993fd859ffba0e04bae0a1799f03591de9 | 114 | rate(1 hour) | False | {'PENDING_FORWARD_WINDOW': 200} |

## Log
- `19:32:48` ✅ Exact releases, all candidate sources traversed, original market bytes replayed, prospective pending/measurement states verified and hourly evaluator installed
