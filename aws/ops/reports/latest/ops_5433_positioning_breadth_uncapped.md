# ops 5433 -- positioning breadth: uncapped counts + brief rebuild

**Status:** success  
**Duration:** 1.8s  
**Finished:** 2026-09-12T03:54:07+00:00  

## Data

| breadth | flat | n_with_inst_trans | negative | positive | step |
|---|---|---|---|---|---|
| 0.2718600953895072 | 32 | 5064 | 1832 | 3200 | breadth |

## Log
- `03:54:06` ✅ universe n=11633 generated=2026-09-11T22:00:37.858984+00:00 | with inst_trans=5064 positive=3200 negative=1832 flat=32 breadth=0.2719
- `03:54:07` ✅ data/finviz-inst-flow.json counts (100, 100) -> (3200, 1832); lists kept at {'accumulating': 100, 'distributing': 100}
- `03:54:07` ✅ data/positioning-brief.json status=LIVE 13F quarter 2026-06-30 funds=18 stale=PERSHING,GREENLIGHT,SCION | inst breadth (uncapped) buy/sell/flat 3200/1832/32 of 5064 -> 0.2719
