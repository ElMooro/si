# ops 5425 -- verdict header edge gate (read-only)

**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-09-12T02:53:48+00:00  

## Data

| age_h | as_of | bias | binder_loaded | first_child_is_header | header_loaded | marker | nav_links | painted | score | status | step | text | title | writer |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.96 | 2026-09-12T01:56:00Z | risk-on |  |  |  |  |  |  | 0.2161 | 200 | verdict |  |  | jh-fusion-projection |
|  |  |  | True | True | True | JH_VERDICT_HEADER_V1 | 973 | True |  |  | render | CALL RISK-ON · score 0.216 · INTERMEDIATE slightly_bullish · regime MILDLY_SUPPORTIVE · coverage 0.31 · SHADOW missing C | Command Desk · JustHodl.AI |  |

## Log
- `02:53:43` ✅ private-artifacts.js 200 -- exact home matcher live, empty-path branch gone
- `02:53:43` ✅ jh-verdict-header.js status=200 marker=True bucket_url=False same_origin=True type=application/javascript; charset=utf-8
- `02:53:43` ✅ /data/verdict.json status=200 writer=jh-fusion-projection bias=risk-on score=0.2161 horizon=INTERMEDIATE shadow=True missing=['CATALYST', 'FLOW', 'MARKET', 'RISK'] as_of=2026-09-12T01:56:00Z age_h=0.96 cache=-
- `02:53:48` ✅ home render painted=True marker=JH_VERDICT_HEADER_V1 prepended=True text='CALL RISK-ON · score 0.216 · INTERMEDIATE slightly_bullish · regime MILDLY_SUPPORTIVE · coverage 0.31 · SHADOW missing C' nav_links=973 binder=True header=True pageerrors=[] verdict_req_failed=[]
- `02:53:48` ✅ GREEN -- verdict header live at the edge: binder exact-path, header same-origin, verdict via zone route, banner painted on / with nav intact
