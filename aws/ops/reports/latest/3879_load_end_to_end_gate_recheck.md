# ops 3879 — GATE: real load() end-to-end vs LIVE data (fetch-shimmed)

**Status:** success  
**Duration:** 5.4s  
**Finished:** 2026-07-25T19:00:23+00:00  

## Log
## 1. pull the served page and extract the FULL inline script (load(); included)

- `19:00:18` ✅   44,384 chars of inline script extracted, load(); call intact
## 2. pull every live feed load() actually requests

- `19:00:18` ✅   etf-flows/daily.json: 170,648 bytes
- `19:00:18` ✅   etf-flows/composite.json: 7,310 bytes
- `19:00:19` ✅   etf-flows/rotation.json: 49,103 bytes
- `19:00:19` ✅   etf-flows/ai-analysis.json: 16,771 bytes
- `19:00:19` ✅   etf-flows/constituent-pressure.json: 3,612,137 bytes
- `19:00:20` ✅   macro/regime.json: 11,899 bytes
## 3. run the REAL load() through a fetch shim, wait for async completion

- `19:00:23` THREW null
BYTES_ai-body 25540
BYTES_macro-regime-body 2989
BYTES_constituent-pressure-body 49870
BYTES_sector-heatmap 4251
BYTES_top-inflows 3348
BYTES_top-outflows 3355
BYTES_full-table 224699
BYTES_buying-board 7673
BYTES_selling-board 7665
BYTES_unified-heatmap 15736
BYTES_master-table 320452
BYTES_meta-bar 153
DONE

## 4. gate — EVERY section, old and new, must have populated

- `19:00:23` ✅   no exception thrown by the real load()
- `19:00:23` ✅   PRE-EXISTING section 'ai-body' populated (25540 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'macro-regime-body' populated (2989 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'constituent-pressure-body' populated (49870 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'sector-heatmap' populated (4251 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'top-inflows' populated (3348 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'top-outflows' populated (3355 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'full-table' populated (224699 bytes)
- `19:00:23` ✅   PRE-EXISTING section 'meta-bar' populated (153 bytes)
- `19:00:23` ✅   NEW section 'buying-board' populated (7673 bytes)
- `19:00:23` ✅   NEW section 'selling-board' populated (7665 bytes)
- `19:00:23` ✅   NEW section 'unified-heatmap' populated (15736 bytes)
- `19:00:23` ✅   NEW section 'master-table' populated (320452 bytes)
- `19:00:23` ✅ PASS_ALL — the real load() executes every section, pre-existing and new, cleanly against live data
