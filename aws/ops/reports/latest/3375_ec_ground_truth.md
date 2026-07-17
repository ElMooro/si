# ops 3375 — etf-constituents ground truth

**Status:** success  
**Duration:** 17.1s  
**Finished:** 2026-07-17T04:05:34+00:00  

## Error

```
SystemExit: 0
```

## Log
- `04:05:33` PASS  G1_invoke_completed — err=None timed_out=False tail=['[constituents] per-stock exposure: 2258 stocks with ETF holdings', '[constituents] DONE — 284/289 ETFs, 2258 stocks, top mover:   ($+135456M)', 'REPORT RequestId: 4268c721-be4b-4b97-917a-1417dd666509\tDuration: 15359.65 ms\tBilled Duration: 15841 m
- `04:05:34` PASS  G2_fresh_writes — objects=284 fresh_this_invoke=284 newest=2026-07-17 04:05:34+00:00
- `04:05:34` PASS  G3_object_quality — {"key": "etf-flows/constituents/SPY.json", "top3": [["NVDA", 7.88606626], ["AAPL", 7.37011103], ["MSFT", 4.50311066]], "sum_w": 63.3}
- `04:05:34` universe: {"n": 300, "has_SPY": true, "has_QQQ": true, "head": ["SPY", "VOO", "IVV", "QQQ", "IWM", "VTI", "DIA", "XLK"]}
- `04:05:34` VERDICT: PASS_ALL
