# ops 3799 — read the node fields that actually exist

**Status:** success  
**Duration:** 29.8s  
**Finished:** 2026-07-24T03:25:34+00:00  

## Data

| dependency_ledger | dependency_members | graph_edges | graph_nodes | invoke_seconds | invoke_status | nodes | was_before | with_centrality | with_degree | with_n_suppliers | with_ticker |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 183 |  | 0 | 183 | 183 | 183 |
|  |  |  |  | 10.4 | 200 |  |  |  |  |  |  |
| 181 | 181 | 303 | 183 |  |  |  | 153 |  |  |  |  |

## Log
## G0 — confirm the dead read

- `03:25:04` ✅ G0.anchor :: node loop anchor unique
- `03:25:04` ✅ G0.v423 :: engine at v4.2.3
- `03:25:04` ✅ G0.centrality_absent :: 0 of 183 nodes carry 'centrality' — the read is dead
- `03:25:04` ✅ G0.degree_present :: 183 nodes carry 'degree'
- `03:25:04` ✅ G0.ticker_key :: nodes key the symbol as 'ticker' (reader looks for symbol|id)
## [1] Read ticker/degree/n_suppliers instead of the absent keys

## [2] Ship the graph ceiling so the metric is not overstated

- `03:25:04` ✅ dependency_note + graph size shipped in the feed
- `03:25:04` ✅ v4.3 spliced + compile clean
## Deploy

- `03:25:04`   zip: 103328 bytes
## 1. Lambda

- `03:25:05`   Lambda exists — updating
- `03:25:08` ✅   ✓ updated justhodl-chokepoint
- `03:25:23` ✅ settled attempt 1
- `03:25:23` ✅ DEPLOY.settled :: v4.3 live
## Invoke + measure the change

- `03:25:34` ✅ LIVE.v43 :: version=4.3
- `03:25:34` ✅ FIX.improved :: 181 names carry dependency (was 153)
- `03:25:34` ✅ FEED.note :: ceiling stated in the feed
## Sample — names the graph actually maps

- `03:25:34`   NEE    Regulated Electric             dep=100.0%  crit=44.0
- `03:25:34`   AMZN   Specialty Retail               dep=100.0%  crit=63.9
- `03:25:34`   GSAT   Telecommunications Services    dep=100.0%  crit=44.8
- `03:25:34`   AAPL   Consumer Electronics           dep=100.0%  crit=74.1
- `03:25:34`   JCI    Construction Materials         dep=100.0%  crit=40.9
- `03:25:34`   TKR    Manufacturing - Tools & Access dep=100.0%  crit=39.1
- `03:25:34`   PWR    Engineering & Construction     dep=100.0%  crit=41.0
- `03:25:34`   WMT    Discount Stores                dep=100.0%  crit=41.8
- `03:25:34`   NTR    Agricultural Inputs            dep=100.0%  crit=30.3
- `03:25:34`   UAL    Airlines, Airports & Air Servi dep=100.0%  crit=16.4
- `03:25:34`   MP     Industrial Materials           dep=100.0%  crit=16.8
- `03:25:34`   CIFR   Financial - Capital Markets    dep= 90.0%  crit=34.6
- `03:25:34`   XOM    Oil & Gas Integrated           dep= 80.0%  crit=34.6
- `03:25:34`   LLY    Drug Manufacturers - General   dep= 66.7%  crit=91.9
## Additive

- `03:25:34` ✅ ADDITIVE.capture_gap :: preserved
- `03:25:34` ✅ ADDITIVE.revenue_share_pct :: preserved
- `03:25:34` ✅ ADDITIVE.catchup_pct :: preserved
- `03:25:34` ✅ ADDITIVE.criticality_pctile :: preserved
## VERDICT

- `03:25:34` ✅ PASS_ALL — node read fixed; graph ceiling stated rather than implied
