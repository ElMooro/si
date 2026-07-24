# ops 3800 — dependency must not be a coverage artifact

**Status:** success  
**Duration:** 14.6s  
**Finished:** 2026-07-24T03:33:35+00:00  

## Data

| dep_max | dep_median | dep_min | dependency_on_member_rows | dependency_published | exactly_100 | graph_edges | graph_nodes | invoke_seconds | invoke_status | was_153_then_181 |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 12.5 | 200 |  |
|  |  |  |  | 156 | 0 | 303 | 183 |  |  | True |
| 66.7 | 6.7 | 0.9 |  |  |  |  |  |  |  |  |
|  |  |  | 156 |  |  |  |  |  |  |  |

## Log
## Settle

- `03:33:21` ✅ v4.3.1 live (attempt 1)
- `03:33:21` ✅ DEPLOY.settled :: peer-floor patch in deployed zip
- `03:33:35` ✅ LIVE.v431 :: version=4.3.1
- `03:33:35` ✅ SANITY.no_lone_100 :: 0 rows still print >=99.9% (was 11)
- `03:33:35` ✅ FEED.peer_count :: mapped-peer count shipped
- `03:33:35` ✅ FEED.suppress_reason :: suppression reason shipped
## Distribution now

- `03:33:35`   LLY    Drug Manufacturers - General   dep= 66.7%  mapped_peers=3   crit=91.9
- `03:33:35`   WULF   Software - Application         dep= 63.6%  mapped_peers=3   crit=36.2
- `03:33:35`   META   Internet Content & Information dep= 58.3%  mapped_peers=4   crit=86.5
- `03:33:35`   APLD   Information Technology Service dep= 52.9%  mapped_peers=3   crit=15.0
- `03:33:35`   CAT    Agricultural - Machinery       dep= 50.0%  mapped_peers=3   crit=46.6
- `03:33:35`   CCJ    Uranium                        dep= 50.0%  mapped_peers=3   crit=23.8
- `03:33:35`   ALB    Chemicals - Specialty          dep= 50.0%  mapped_peers=3   crit=20.2
- `03:33:35`   TSLA   Auto - Manufacturers           dep= 45.5%  mapped_peers=3   crit=47.4
- `03:33:35`   LITE   Communication Equipment        dep= 44.4%  mapped_peers=4   crit=42.3
- `03:33:35`   BKR    Oil & Gas Equipment & Services dep= 40.0%  mapped_peers=3   crit=40.4
- `03:33:35`   SLB    Oil & Gas Equipment & Services dep= 40.0%  mapped_peers=3   crit=36.1
- `03:33:35`   MSFT   Software - Infrastructure      dep= 39.6%  mapped_peers=8   crit=85.6
## Additive

- `03:33:35` ✅ ADDITIVE.capture_gap :: preserved
- `03:33:35` ✅ ADDITIVE.revenue_share_pct :: preserved
- `03:33:35` ✅ ADDITIVE.catchup_pct :: preserved
- `03:33:35` ✅ ADDITIVE.criticality_pctile :: preserved
## VERDICT

- `03:33:35` ✅ PASS_ALL — dependency shares now require real peer coverage
