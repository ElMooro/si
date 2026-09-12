# ops 5438 brief compiler schedule

**Status:** success  
**Duration:** 21.1s  
**Finished:** 2026-09-12T04:32:06+00:00  

## Log
- `04:31:45` ✅ plumbing status=LIVE plumbing-stress composite_label=NORMAL score=44.1
- `04:31:45` ✅ official_stats status=LIVE GDPNow 4.4164 on 2026-07-01; T10Y3M 0.89 on 2026-09-11
- `04:31:46` ✅ market_tape status=LIVE session=2026-09-10 n_tickers=12572 etfs=60 heavy in/out/other 3/5/52 basis=uncapped
- `04:31:47` ✅ positioning status=LIVE 13F quarter 2026-06-30 funds=18 stale=PERSHING,GREENLIGHT,SCION | inst breadth (uncapped) buy/sell/flat 0/0/0 of 0
- `04:31:47` ✅ event status=LIVE finviz-signals n_screens=None confluence_n=385
- `04:31:47` ✅ verdict coverage=0.9231 missing=['CATALYST']
- `04:31:47` ✅ manifest rules=455 added=['brief-compiler-plumbing-6h', 'brief-compiler-market-tape-6h', 'brief-compiler-official-stats-daily', 'brief-compiler-positioning-daily', 'brief-compiler-event-6h', 'brief-compiler-verdict-3h']
- `04:32:05` ✅ reconciler drift=309
- `04:32:06` ✅ compiler verdict invoke: {"ok": true, "results": {"verdict": {"coverage": 0.9231, "score": 0.1884, "missing": ["CATALYST"], "as_of": "2026-09-12T04:25:56Z"}}}
