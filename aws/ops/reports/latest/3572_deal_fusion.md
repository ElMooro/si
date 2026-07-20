# ops 3572 — deal-win fusion across the fleet

**Status:** success  
**Duration:** 16.3s  
**Finished:** 2026-07-20T16:42:33+00:00  

## Error

```
SystemExit: 0
```

## Log
- `16:42:17` PASS  G1_best_setups_ctx — zip markers deal_context join
- `16:42:18` PASS  G2_master_ranker_overlay — zip markers deal_win overlay + fusion print
- `16:42:18` PASS  G3_mi_facts — zip markers feed + fresh_deal_wins fact
- `16:42:33` PASS  G4_ranker_behavior — feed regenerated post-patch · rows carrying deal_win overlay = 0 (count depends on a ranked name having a fresh <=72h deal — overlay attach is data-gated, presence of ANY is bonus proof)
- `16:42:33` PASS  G5_alpha_families_card — served markers: c-deals card + feed wire
- `16:42:33` PASS  G6_why_deal_radar — served markers: Deal Radar section + filler
- `16:42:33` VERDICT: PASS_ALL
