# ops 3523 — census #6 wave-2 + composer probe

**Status:** success  
**Duration:** 32.5s  
**Finished:** 2026-07-19T18:04:33+00:00  

## Log
- `18:04:01`   zip: 85772 bytes
## 1. Lambda

- `18:04:01`   Lambda exists — updating
- `18:04:05` ✅   ✓ updated justhodl-proven-portfolio
- `18:04:29` PASS  X1_composer — {'n_raw_positions': 40, 'n_positions': 0, 'guard': {'dropped_nonfinite': 0, 'deduped': 0, 'clamped': 0}, 'n_stale': 0, 'n_missing': 1, 'missing': ['data/stress-index.json']}
- `18:04:31` WAVE2_TABLE [{"page": "political.html", "feed": "data/lobbying-intel.json", "mode": "bars", "path": "all_tickers", "label": "ticker", "val": "score", "n": 200}, {"page": "primary-dealers.html", "feed": "data/nyfed-primary-dealer.json", "mode": "bars-dict", "path": "by_tenor_usd_b.TREASURY_COUPONS", "n": 7}, {"page": "us-data-desk.html", "feed": "data/census-economic.json", "mode": "bars-dict", "path": "summary", "n": 12}, {"page": "bls.html", "feed": "data/bls-employment.json", "mode": "bars", "path": "crisis.components", "label": "key", "val": "value", "n": 8}, {"page": "heatmap.html", "feed": "data/stock-valuations.json", "mode": "bars", "path": "sp_table", "label": "t", "val": "pe", "n": 499}, {"page": "valuations.html", "feed": "data/stock-valuations.json", "mode": "bars", "path": "sp_table", "label": "t", "val": "pe", "n": 499}, {"page": "ofr.html", "feed": "data/settlement-fails.json", "mode": "bars", "path": "classes", "label": "key", "val": "ftd_latest", "n": 6}, {"page": "dollar.html", "feed": "data/dollar-radar.json", "mode": "bars", "path": "canaries", "label": "label", "val": "lean", "n": 15}, {"page": "eurodollar.html", "feed": "data/eurodollar-plumbing.json", "mode": "bars", "path": "layers.us_core.metrics", "label": "id", "val": "value", "n": 9}, {"page": "tv-notes.html", "feed": "data/tradingview-notes.json", "mode": "bars", "path": "notes", "label": "symbol", "val": "created", "n": 3322}]
- `18:04:31` PASS  X2_wave2 — {'derived': 10, 'misses': 2}
- `18:04:33` FAIL  X3_enhanced_pages — [('proven-alpha.html', False, False), ('liquidity.html', False, False), ('industry-rotation.html', False, False), ('capital-flow.html', False, False), ('share-flows.html', False, False), ('opportunities.html', False, False), ('panels.html', False, False)]
