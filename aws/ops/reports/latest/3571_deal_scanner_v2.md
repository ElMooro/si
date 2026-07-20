# ops 3571 — deal-scanner v2.0.0 full-market + graded family

**Status:** success  
**Duration:** 158.2s  
**Finished:** 2026-07-20T16:42:17+00:00  

## Error

```
SystemExit: 0
```

## Log
- `16:40:16` PASS  G1_settled_v2 — zip markers VERSION 2.0.0 + deal-win + by_sector
- `16:40:16` FAIL  G2_schedule_3x — rule deal-scanner-daily = cron(5 */3 * * ? *)
- `16:40:31` PASS  G3_feed_v2_live — prs=3600 deals=16 sectors_boards=11 caps_boards=6 sectors_hit=6/11 caps_hit=6/6 tape_tickers=1299 sources={'fmp_pr': 1400, 'fmp_news': 1400, 'polygon': 800}
- `16:40:31` PASS  G4_graded_family — signals_logged=4 list=['IREN', 'EPR', 'MRAI', 'KMDA'] · DDB deal-win#IREN#2026-07-20: FOUND conf=0.66 base=40.237 regime=True
- `16:42:17` PASS  G5_page_boards — served markers: Market Coverage / All Cap Tiers / Graded Signals
- `16:42:17` VERDICT: GAPS: G2_schedule_3x
