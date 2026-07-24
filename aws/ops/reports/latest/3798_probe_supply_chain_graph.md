# ops 3798 — what is actually inside supply-chain-graph.json

**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-07-24T03:20:42+00:00  

## Data

| age_hours | bytes | capture_ledger | dependency_populated_today | distinct_symbols_in_graph | edges_present | last_modified | nodes_present | nodes_with_centrality | overlap |
|---|---|---|---|---|---|---|---|---|---|
| 5.4 | 73048 |  |  |  |  | 2026-07-23 21:55:42 |  |  |  |
|  |  |  |  |  | 303 |  | 183 |  |  |
|  |  |  |  |  |  |  |  | 0 |  |
|  |  |  |  | 185 |  |  |  |  |  |
|  |  | 3012 |  |  |  |  |  |  | 180 |
|  |  |  | 153 |  |  |  |  |  |  |

## Log
## 1. Does the feed exist and is it fresh?

- `03:20:41` ✅ FEED.exists :: 73048 bytes, 5.4h old
- `03:20:41` ✅ FEED.fresh :: 5.4 hours old
## 2. ACTUAL top-level keys

- `03:20:41`   engine                     str      27
- `03:20:41`   version                    str      5
- `03:20:41`   ok                         bool     True
- `03:20:41`   generated_at               str      32
- `03:20:41`   thesis                     str      222
- `03:20:41`   n_nodes                    int      183
- `03:20:41`   n_edges                    int      303
- `03:20:41`   n_themes                   int      19
- `03:20:41`   themes                     list     19
- `03:20:41`   graph_stats                dict     6
- `03:20:41`   booming_hubs               list     4
- `03:20:41`   nodes                      list     183
- `03:20:41`   edges                      list     303
- `03:20:41`   supply_chain_laggards      list     1
- `03:20:41`   top_picks                  list     1
- `03:20:41`   data_source                str      82
- `03:20:41`   caveats                    list     3
- `03:20:41`   elapsed_s                  float    7.3
## 3. Real field names on an edge / node

- `03:20:41`   edge[0] keys: ['confirm', 'customer', 'relationship', 'source', 'supplier']
- `03:20:41`   edge[0]     : {"supplier": "ASML", "customer": "TSM", "relationship": "EUV litho", "source": "curated", "confirm": "none"}
- `03:20:41` ✅ EDGE.reader_matches :: 303 of 303 edges expose supplier|source (what chokepoint reads)
- `03:20:41`   node[0] keys: ['degree', 'is_boom', 'n_customers', 'n_suppliers', 'origin', 'perf_30d', 'perf_5d', 'price', 'theme', 'ticker']
- `03:20:41`   node[0]     : {"ticker": "A", "theme": "Healthcare/Biopharma", "origin": "curated", "perf_30d": 5.51, "perf_5d": -0.93, "price": 133.46, "is_boom": false, "degree": 3, "n_suppliers": 2, "n_customers": 1}
## 4. Symbol universe of the graph vs the capture ledger

- `03:20:41`   sample: ['A', 'AAPL', 'ACLS', 'ADI', 'AEHR', 'AEIS', 'ALB', 'ALSN', 'ALV', 'AMAT', 'AMD', 'AME', 'AMKR', 'AMZN', 'ANET', 'AOS', 'APH', 'APLD', 'APTV', 'ARM', 'ASML', 'ATKR', 'AVGO', 'AXL']
- `03:20:42` ✅ OVERLAP.nonzero :: 180 shared symbols
- `03:20:42`   overlap sample: ['A', 'AAPL', 'ACLS', 'ADI', 'AEHR', 'AEIS', 'ALB', 'ALSN', 'ALV', 'AMAT', 'AMD', 'AME', 'AMKR', 'AMZN', 'ANET', 'AOS', 'APH', 'APLD', 'APTV', 'ARM', 'ASML', 'ATKR', 'AVGO', 'AXL']
## VERDICT

- `03:20:42` ⚠ The graph itself only names 185 symbols. dependency_pct cannot exceed that. If Khalid expects broad coverage, the GRAPH is the thing to expand — the join is fine.
- `03:20:42` ✅ PASS_ALL — schema probed
